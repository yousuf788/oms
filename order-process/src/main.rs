// order-process (S2 cluster replica) — Aeron transport
// Subscribes to order channel via Aeron (replaces raw UDP recv).
// Publishes committed results to S3 via Aeron (replaces raw UDP send).
// All Raft consensus, WAL, and leader election logic is unchanged.

use order_process::config::{find_node, init_config, node_name, resolve_node_id};
use order_process::health_probe::start_health_responder;
use order_process::leader_election::LeaderElection;
use order_process::wal::ReplicatedCommand;
use rand::Rng;
use rusteron_client::*;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::ffi::CString;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

const ORDER_STREAM_ID: i32 = 1001;
const RESULT_STREAM_ID: i32 = 2001;

#[derive(Deserialize, Debug)]
struct Order {
    order_id: u64,
    symbol: String,
    side: String,
    qty: u32,
}

fn aeron_dir() -> String {
    std::env::var("AERON_DIR")
        .unwrap_or_else(|_| {
            #[cfg(target_os = "linux")]
            unsafe {
                extern "C" { fn getuid() -> u32; }
                format!("/dev/shm/aeron-{}", getuid())
            }
            #[cfg(not(target_os = "linux"))]
            "/dev/shm/aeron-0".to_string()
        })
}

fn main() {
    let cfg = init_config();
    let node_id = resolve_node_id();
    if !(1..=3).contains(&node_id) {
        panic!("NODE_ID must be 1, 2, or 3 (got {node_id})");
    }

    let self_node = find_node(node_id).expect("unknown NODE_ID");

    // Trivial liveness responder for the witness service — independent of Aeron
    // and the Raft control channel, so it comes up even before either does.
    start_health_responder(node_id, &cfg.bind_host, self_node.health_port);

    println!(
        "[role] {} (S2-{}) starting — peers: {}",
        node_name(node_id),
        node_id,
        cfg.nodes
            .iter()
            .map(|n| format!("{}={}", n.name, n.host))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // ── Connect to Aeron Media Driver ──────────────────────────────────────────
    let aeron_dir_path = aeron_dir();
    println!("[order-process] connecting to Aeron Media Driver at {aeron_dir_path}");

    let ctx = AeronContext::new().expect("Aeron context");
    let aeron_dir_cstr = CString::new(aeron_dir_path).unwrap();
    ctx.set_dir(&aeron_dir_cstr).expect("set aeron dir");
    ctx.set_error_handler(Some(|code: i32, msg: &str| {
        eprintln!("[aeron] error {code}: {msg}");
    })).expect("set error handler");

    let aeron = Aeron::new(&ctx).expect("Aeron client");
    aeron.start().expect("start aeron client");

    // ── Subscribe to order channel ─────────────────────────────────────────────
    // Each S2 node subscribes on its own host:order_port.
    // S1 (order-sending) publishes to each of these unicast endpoints.
    let order_channel = format!(
        "aeron:udp?endpoint={}:{}",
        self_node.host, self_node.order_port
    );
    println!(
        "[order-process] subscribing to orders on {order_channel} stream {ORDER_STREAM_ID}"
    );
    let order_channel_cstr = CString::new(order_channel).unwrap();
    let order_subscription = aeron
        .async_add_subscription(
            &order_channel_cstr,
            ORDER_STREAM_ID,
            Handlers::NONE,
            Handlers::NONE,
        )
        .expect("async_add_subscription (orders)")
        .poll_blocking(Duration::from_secs(10))
        .expect("order subscription ready");

    // ── Publication to order-receiver (S3) ────────────────────────────────────
    // All 3 nodes create this publication. Only the active leader calls offer().
    let result_channel = format!("aeron:udp?endpoint={}:{}", cfg.s3_host, cfg.s3_port);
    println!(
        "[order-process] adding result publication → {result_channel} stream {RESULT_STREAM_ID}"
    );
    let result_channel_cstr = CString::new(result_channel).unwrap();
    let result_pub = aeron
        .async_add_publication(&result_channel_cstr, RESULT_STREAM_ID)
        .expect("async_add_publication (results)")
        .poll_blocking(Duration::from_secs(10))
        .expect("result publication ready");

    let result_pub = Arc::new(result_pub);

    // ── Start Raft election ────────────────────────────────────────────────────
    let election = LeaderElection::start(node_id);

    // ── Main processing loop ───────────────────────────────────────────────────
    let mut last_role_line = String::new();

    // Shared dedup set wrapped in Mutex so the fragment handler closure can use it
    let seen_ids: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let pending_orders: Arc<Mutex<Vec<Order>>> = Arc::new(Mutex::new(Vec::with_capacity(500)));

    let mut idle = BackoffIdleStrategy::new();

    loop {
        // Print role changes
        let line = election.role_summary();
        if line != last_role_line {
            println!("[role] {line}");
            last_role_line = line;
        }

        // Drain up to 500 orders from the Aeron subscription into pending_orders
        {
            let seen_ids = Arc::clone(&seen_ids);
            let pending_orders = Arc::clone(&pending_orders);
            let fragments = order_subscription
                .poll_fn(move |buf: &[u8], _hdr: AeronHeader| {
                    if let Ok(order) = serde_json::from_slice::<Order>(buf) {
                        let mut pending = pending_orders.lock().unwrap();
                        if pending.len() >= 500 {
                            return; // batch full
                        }
                        let mut seen = seen_ids.lock().unwrap();
                        if seen.insert(order.order_id) {
                            pending.push(order);
                        }
                    }
                }, 500)
                .unwrap_or(0);
            idle.idle(fragments);
        }

        // Check if we have orders to process
        let orders = {
            let mut pending = pending_orders.lock().unwrap();
            if pending.is_empty() {
                continue;
            }
            // Clear the seen set periodically (when batch drains)
            seen_ids.lock().unwrap().clear();
            std::mem::take(&mut *pending)
        };

        if election.is_leader() {
            process_orders_batch_as_leader(
                node_id,
                &orders,
                &election,
                &result_pub,
            );
        }
    }
}

fn process_orders_batch_as_leader(
    node_id: u8,
    orders: &[Order],
    election: &LeaderElection,
    result_pub: &AeronPublication,
) {
    let leader = node_name(node_id);
    let verbose = order_process::config::verbose_raft();
    let current_term = election.current_term();
    let outcomes = ["FILLED", "PARTIALLY_FILLED", "REJECTED"];
    let mut rng = rand::thread_rng();

    let commands: Vec<ReplicatedCommand> = orders
        .iter()
        .map(|order| {
            let status = outcomes[rng.gen_range(0..outcomes.len())];
            let filled_qty: u32 = if status == "REJECTED" {
                0
            } else {
                rng.gen_range(1..=order.qty)
            };
            ReplicatedCommand {
                order_id: order.order_id,
                symbol: order.symbol.clone(),
                side: order.side.clone(),
                qty: order.qty,
                status: status.to_string(),
                filled_qty,
                processed_by: format!("{} (S2-{})", leader, node_id),
                term: current_term,
            }
        })
        .collect();

    let committed_batch = election.propose_batch(commands);
    if committed_batch.is_empty() {
        return;
    }

    let mut idle = BusySpinIdleStrategy::default();
    for committed in committed_batch {
        let result = json!({
            "order_id": committed.order_id,
            "symbol": committed.symbol,
            "side": committed.side,
            "qty": committed.qty,
            "status": committed.status,
            "filled_qty": committed.filled_qty,
            "processed_by": committed.processed_by,
            "term": committed.term,
        });

        let line = result.to_string();
        let payload = line.as_bytes();

        // Offer to S3 with backpressure retry
        loop {
            match result_pub.offer(payload) {
                Ok(_) => break,
                Err(e) if e.is_retryable() => {
                    idle.idle(0);
                    continue;
                }
                Err(e) => {
                    eprintln!("[order-process] result publish error: {e}");
                    break;
                }
            }
        }

        if verbose {
            println!(
                "[order] {} LEADER committed order_id={} status={} filled={}/{}",
                leader, committed.order_id, committed.status,
                committed.filled_qty, committed.qty,
            );
        }
    }
}
