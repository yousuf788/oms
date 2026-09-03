// order-process (S2 cluster replica) — Aeron transport
// Subscribes to order channel via Aeron (replaces raw UDP recv).
// Publishes committed results to S3 via Aeron (replaces raw UDP send).
// All Raft consensus, WAL, and leader election logic is unchanged.
//
// Security: every inbound Aeron order message must carry a valid HMAC-SHA256
// tag (written by order-sending). Raft control messages use the same key.
// monitoring corroboration messages use a separate monitoring_HMAC_KEY.
// Set CLUSTER_HMAC_KEY and monitoring_HMAC_KEY in .env (openssl rand -hex 32).

use order_process::config::{find_node, init_config, node_name, resolve_node_id};
use order_process::auth;
use order_process::health_probe::start_health_responder;
use order_process::leader_election::LeaderElection;
use order_process::replay_client::start_replay_client;
use order_process::replay_server::start_replay_server;
use order_process::sequence_tracker::SequenceTracker;
use order_process::wal::ReplicatedCommand;
use rand::Rng;
use rusteron_client::*;
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

const ORDER_STREAM_ID: i32 = 1001;
const RESULT_STREAM_ID: i32 = 2001;

/// Wire format for an inbound order from order-sending's Aeron order
/// channel (inside the HMAC-signed frame that `auth::verify` unwraps). KEEP
/// IN SYNC WITH order-sending/src/main.rs::OrderWire — bincode encodes
/// struct fields positionally (by declaration order and type, not by field
/// name), so both sides must declare identical field order and types.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct OrderWire {
    order_id: u64,
    symbol: u8,
    side: bool,
    qty: u32,
    #[allow(dead_code)] // received for wire compatibility; not used on this side
    ts_ms: u64,
}

const SYMBOLS: [&str; 3] = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];

impl OrderWire {
    fn symbol_str(&self) -> &'static str {
        SYMBOLS.get(self.symbol as usize).copied().unwrap_or("UNKNOWN")
    }
    fn side_str(&self) -> &'static str {
        if self.side { "BUY" } else { "SELL" }
    }
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
    println!("[order-process] === STEP 1: Loading Configuration & Resolving Node ID ===");
    let cfg = init_config();
    let node_id = resolve_node_id();
    if !(1..=3).contains(&node_id) {
        panic!("NODE_ID must be 1, 2, or 3 (got {node_id})");
    }

    let self_node = find_node(node_id).expect("unknown NODE_ID");
    println!(
        "[order-process] Config initialized for Node ID {} ({}) — Host: {}, Raft Port: {}, Order Port: {}, Health Port: {}",
        node_id, self_node.name, self_node.host, self_node.raft_port, self_node.order_port, self_node.health_port
    );

    println!("[order-process] === STEP 2: Starting monitoring Health Probe Responder ===");
    // Trivial liveness responder for the monitoring service — independent of Aeron
    // and the Raft control channel, so it comes up even before either does.
    start_health_responder(node_id, &cfg.bind_host, self_node.health_port);
    println!(
        "[health-probe] UDP liveness responder listening on {}:{}",
        cfg.bind_host, self_node.health_port
    );

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

    println!("[order-process] === STEP 3: Connecting to Aeron Media Driver ===");
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
    println!("[order-process] Aeron client successfully connected to media driver");

    println!("[order-process] === STEP 4: Initializing Aeron Order Subscription & Result Publication ===");
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
    println!("[order-process] order channel subscription ACTIVE on stream {ORDER_STREAM_ID}");

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
    println!("[order-process] result channel publication ACTIVE on stream {RESULT_STREAM_ID}");

    println!("[order-process] === STEP 5: Starting Raft Consensus Engine ===");
    // Bounded hand-off from replay_server (S3's REPLAY_REQUEST) to the
    // result publisher thread, which is the sole owner of `result_pub`.
    let (replay_tx, replay_rx) = mpsc::sync_channel::<(u64, u64)>(64);
    let election = LeaderElection::start(node_id, result_pub, replay_rx);

    println!("[order-process] === STEP 5b: Starting S1<->S2 and S2<->S3 Replay Channels ===");
    // Ingest-side sequence tracker: dedups every inbound order by order_id
    // across the whole process lifetime (fixing the old per-batch-only
    // `seen_ids` dedup) and detects gaps for the replay-request ticker.
    // Marked in the polling thread only, so no lock contention on the
    // decode/dedup hot path beyond this one uncontended mutex (mirrors the
    // existing `wal: Mutex<Wal>` / `state: Mutex<RaftState>` pattern).
    let tracker = Arc::new(Mutex::new(SequenceTracker::new()));
    start_replay_client(node_id, cfg.s1_host.clone(), cfg.s1_replay_port, Arc::clone(&tracker));
    start_replay_server(cfg.bind_host.clone(), self_node.replay_port, Arc::clone(&election), replay_tx);

    println!("[order-process] === STEP 6: Spawning Order Ingress Polling Thread & Hot Loop ===");
    // Bounded channel to apply backpressure if processing is slower than Aeron delivery
    let (order_tx, order_rx) = crossbeam_channel::bounded::<OrderWire>(500_000);

    let poll_tx = order_tx.clone();
    let poll_tracker = Arc::clone(&tracker);
    thread::spawn(move || {
        let mut idle = BackoffIdleStrategy::new();
        loop {
            let verbose = order_process::config::verbose_raft();
            let fragments = order_subscription
                .poll_fn(|buf: &[u8], _hdr: AeronHeader| {
                    match auth::verify(buf) {
                        Some(payload) => match bincode::deserialize::<OrderWire>(payload) {
                            Ok(order) => {
                                // Dedup across the process lifetime (not just
                                // this poll batch) — replayed and Aeron-redelivered
                                // orders are only forwarded for processing once.
                                //
                                // Blocking, not try_send: this channel exists
                                // specifically so processing slower than
                                // Aeron delivery backpressures here (see its
                                // doc comment below) rather than silently
                                // dropping an order the tracker has already
                                // marked "seen" — a drop *after* marking
                                // would be permanently invisible to gap
                                // detection/replay, defeating the entire
                                // point of that mechanism. Blocking the
                                // Aeron polling thread here is safe and
                                // correct: it naturally throttles fragment
                                // consumption, which Aeron's own flow
                                // control then backpressures upstream to
                                // order-sending.
                                if poll_tracker.lock().unwrap().mark(order.order_id) {
                                    let _ = poll_tx.send(order);
                                }
                            }
                            Err(err) if verbose => eprintln!(
                                "[order-process] dropped order ({} bytes payload): decode error: {err}",
                                payload.len()
                            ),
                            Err(_) => {}
                        },
                        None if verbose => eprintln!(
                            "[order-process] dropped order packet ({} bytes): HMAC failure — check CLUSTER_HMAC_KEY in .env",
                            buf.len()
                        ),
                        None => {}
                    }
                }, 5000)
                .unwrap_or(0);
            idle.idle(fragments);
        }
    });

    let mut last_role_line = String::new();
    let mut batch = Vec::with_capacity(20_000);

    println!("[order-process] READY — entering main order batching & consensus loop...");
    loop {
        let line = election.role_summary();
        if line != last_role_line {
            println!("[role] {line}");
            last_role_line = line;
        }

        // Gather up to 20,000 orders in a single batch. Dedup already
        // happened at ingest (poll_tracker, above) — every order reaching
        // this channel is known-new.
        batch.clear();

        while batch.len() < 20_000 {
            match order_rx.try_recv() {
                Ok(order) => batch.push(order),
                Err(_) => break, // channel empty
            }
        }

        if batch.is_empty() {
            thread::sleep(Duration::from_millis(1));
            continue;
        }

        if election.is_leader() {
            process_orders_batch_as_leader(node_id, &batch, &election);
        } else {
            // As a follower, we just drain the channel so it doesn't back up
        }
    }
}

fn process_orders_batch_as_leader(
    node_id: u8,
    orders: &[OrderWire],
    election: &LeaderElection,
) {
    let leader = node_name(node_id);
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
                symbol: order.symbol_str().to_string(),
                side: order.side_str().to_string(),
                qty: order.qty,
                status: status.to_string(),
                filled_qty,
                processed_by: format!("{} (S2-{})", leader, node_id),
                term: current_term,
            }
        })
        .collect();

    // Result delivery to S3 is handled asynchronously by LeaderElection's
    // background publisher thread once each entry commits (see
    // leader_election.rs::result_publisher_loop) — this call's return value
    // is used only as a flow-control signal here, never to gate delivery.
    election.propose_batch(commands);
}
