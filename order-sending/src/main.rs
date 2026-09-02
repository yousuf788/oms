// order-sending (S1) — Aeron high-throughput publisher
// Architecture:
//   - N generator threads produce OrderWire values → bounded channel
//   - 1 fan-out thread bincode-encodes + HMAC-signs once, then distributes to
//     3 dedicated per-node publisher threads (one AeronPublication each), so a
//     slow or backpressured node no longer stalls delivery to the other two
//   - Backpressure: if a node's publisher thread is slow, its channel fills
//     and the fan-out thread blocks on that node's send() — no silent drops
// Rate-paced to TARGET_TPS orders/sec (default 5000).

mod auth;
mod config;

use config::init_config;
use rand::Rng;
use rusteron_client::*;
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

const ORDER_STREAM_ID: i32 = 1001;

/// Wire format sent to order-process's Aeron order channel (inside the
/// HMAC-signed frame `auth::sign` produces). KEEP IN SYNC WITH
/// order-process/src/main.rs::OrderWire — bincode encodes struct fields
/// positionally (by declaration order and type, not by field name), so both
/// sides must declare identical field order and types.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct OrderWire {
    order_id: u64,
    symbol: u8,
    side: bool,
    qty: u32,
    ts_ms: u64,
}

const SYMBOLS: [&str; 3] = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];

fn sent_log_path() -> PathBuf {
    PathBuf::from("logs").join("orders-sent.log")
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn aeron_dir() -> String {
    std::env::var("AERON_DIR").unwrap_or_else(|_| {
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
    println!("[order-sending] === STEP 1: Loading Configuration & Pacing Parameters ===");
    let cfg = init_config();

    let target_tps: u64 = std::env::var("TARGET_TPS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(5000);
    let num_gen_threads: usize = std::env::var("SENDER_THREADS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    let channel_capacity = target_tps.max(10000) as usize;

    println!(
        "[order-sending] Target TPS: {}, Generator Threads: {}, Channel Capacity: {}",
        target_tps, num_gen_threads, channel_capacity
    );

    println!("[order-sending] === STEP 2: Verifying CLUSTER_HMAC_KEY Security Configuration ===");
    // Eagerly load the key at startup so we panic early if CLUSTER_HMAC_KEY is missing.
    let _ = auth::cluster_key();
    println!("[order-sending] CLUSTER_HMAC_KEY successfully validated");

    println!("[order-sending] === STEP 3: Connecting to Aeron Media Driver ===");
    let aeron_dir_path = aeron_dir();
    println!("[order-sending] connecting to Aeron Media Driver at {aeron_dir_path}");

    let ctx = AeronContext::new().expect("Aeron context");
    let aeron_dir_cstr = CString::new(aeron_dir_path).unwrap();
    ctx.set_dir(&aeron_dir_cstr).expect("set aeron dir");
    ctx.set_error_handler(Some(|code: i32, msg: &str| {
        eprintln!("[aeron] error {code}: {msg}");
    })).expect("set error handler");

    let aeron = Aeron::new(&ctx).expect("Aeron client");
    aeron.start().expect("start aeron client");
    println!("[order-sending] Aeron client successfully connected to media driver");

    // ── Shared counters ────────────────────────────────────────────────────────
    let order_counter = Arc::new(AtomicU64::new(1));
    let sent_total = Arc::new(AtomicU64::new(0));

    println!("[order-sending] === STEP 4: Starting Background Log Writer & Throughput Stats Thread ===");
    let (log_tx, log_rx) = mpsc::sync_channel::<u64>(1_000_000);
    {
        let path = sent_log_path();
        thread::spawn(move || {
            if let Some(parent) = path.parent() { let _ = create_dir_all(parent); }
            let mut file = OpenOptions::new().create(true).append(true).open(&path)
                .expect("cannot open orders-sent.log");
            let mut buf = String::with_capacity(128 * 1024);
            loop {
                match log_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(order_id) => {
                        buf.push_str(&order_id.to_string()); buf.push('\n');
                        if buf.len() >= 65536 {
                            let _ = file.write_all(buf.as_bytes());
                            let _ = file.flush();
                            buf.clear();
                        }
                    }
                    Err(_) => {
                        if !buf.is_empty() {
                            let _ = file.write_all(buf.as_bytes());
                            let _ = file.flush();
                            buf.clear();
                        }
                    }
                }
            }
        });
    }

    {
        let sent_total = Arc::clone(&sent_total);
        thread::spawn(move || {
            let mut last = 0u64;
            loop {
                thread::sleep(Duration::from_secs(1));
                let now = sent_total.load(Ordering::Relaxed);
                println!("[order-sending] throughput: {:>8} orders/sec  total: {}", now - last, now);
                last = now;
            }
        });
    }

    println!("[order-sending] === STEP 5: Initializing Per-Node Unicast Aeron Publications ===");

    // ── Create Aeron publications — one per S2 node (unicast). Non-exclusive
    // (AeronPublication, not AeronExclusivePublication) because only the
    // non-exclusive type is Send — each is moved into its own dedicated
    // publisher thread below. ──────────────────────────────────────────────
    let mut node_channels: Vec<mpsc::SyncSender<Arc<Vec<u8>>>> = Vec::new();
    for (i, node) in cfg.nodes.iter().enumerate() {
        let ch = format!("aeron:udp?endpoint={}:{}", node.host, node.order_port);
        println!("[order-sending] publication[{i}] → {ch} stream {ORDER_STREAM_ID}");
        let ch_cstr = CString::new(ch).unwrap();
        let pub_ = aeron
            .async_add_publication(&ch_cstr, ORDER_STREAM_ID)
            .unwrap_or_else(|e| panic!("add_publication node {}: {e}", i + 1))
            .poll_blocking(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("poll_publication node {}: {e}", i + 1));

        let (node_tx, node_rx) = mpsc::sync_channel::<Arc<Vec<u8>>>(channel_capacity);
        node_channels.push(node_tx);
        thread::spawn(move || {
            // A node with no live subscriber reports back pressure as
            // `NotConnected`, which `is_retryable()` correctly treats as
            // retryable (a subscriber may join later) — but retrying it
            // unboundedly would let one dead/slow node freeze this thread
            // forever, and with it its own bounded channel and (via the
            // fan-out thread blocking on that channel) every other node and
            // every generator thread behind it. Cap retries as a safety net
            // so a node that never connects (or drops mid-stream) just loses
            // this one message instead of wedging the whole pipeline.
            const MAX_BACKPRESSURE_RETRIES: u32 = 100_000;
            let mut idle = BusySpinIdleStrategy::default();
            loop {
                match node_rx.recv() {
                    Ok(frame) => {
                        let mut retries = 0u32;
                        loop {
                            match pub_.offer(frame.as_slice()) {
                                Ok(_) => break,
                                Err(e) if e.is_retryable() && retries < MAX_BACKPRESSURE_RETRIES => {
                                    retries += 1;
                                    idle.idle(0);
                                    continue;
                                }
                                Err(e) => {
                                    eprintln!("[order-sending] publish error (node {}): {e}", i + 1);
                                    break;
                                }
                            }
                        }
                    }
                    Err(_) => break, // fan-out thread exited
                }
            }
        });
    }

    // ── Channel: generator threads → fan-out thread ──────────────────────────
    let (payload_tx, payload_rx) = mpsc::sync_channel::<OrderWire>(channel_capacity);

    // ── Fan-out thread: bincode-encode + HMAC-sign once, distribute to all 3
    // per-node publisher threads. Blocking send() on each node channel
    // preserves "no silent drops" — a full channel means that node's own
    // offer() retry loop is backpressured, and we wait rather than lose the
    // order. ──────────────────────────────────────────────────────────────
    let fanout_handle = {
        let log_tx = log_tx.clone();
        thread::spawn(move || {
            loop {
                match payload_rx.recv() {
                    Ok(order) => {
                        let Ok(encoded) = bincode::serialize(&order) else { continue };
                        let frame = Arc::new(auth::sign(&encoded));
                        for node_tx in &node_channels {
                            if node_tx.send(Arc::clone(&frame)).is_err() {
                                return; // that node's publisher thread exited
                            }
                        }
                        let _ = log_tx.try_send(order.order_id);
                    }
                    Err(_) => break, // all generator threads exited
                }
            }
        })
    };

    // ── Rate pacing ────────────────────────────────────────────────────────────
    let per_thread_tps = if target_tps > 0 {
        (target_tps as f64 / num_gen_threads as f64).max(0.1)
    } else { 0.0 };
    let nanos_per_order = if per_thread_tps > 0.0 {
        (1_000_000_000.0 / per_thread_tps) as u64
    } else { 0 };

    println!(
        "[order-sending] starting {num_gen_threads} generator threads → {target_tps} orders/sec"
    );

    // ── Generator threads: build OrderWire values ───────────────────────────────
    for _tid in 0..num_gen_threads {
        let order_counter = Arc::clone(&order_counter);
        let sent_total = Arc::clone(&sent_total);
        let payload_tx = payload_tx.clone();
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let thread_start = Instant::now();
            let mut order_index = 0u64;
            loop {
                let order_id = order_counter.fetch_add(1, Ordering::Relaxed);
                let symbol = rng.gen_range(0..SYMBOLS.len() as u8);
                let side = rng.gen_bool(0.5);
                let qty: u32 = rng.gen_range(1..=10);
                let ts_ms = now_ms() as u64;
                let order = OrderWire { order_id, symbol, side, qty, ts_ms };

                // Send to fan-out thread (blocks if it's busy → backpressure)
                if payload_tx.send(order).is_err() {
                    break; // fan-out thread exited
                }
                sent_total.fetch_add(1, Ordering::Relaxed);

                // Rate pacing
                order_index += 1;
                if nanos_per_order > 0 {
                    let expected = Duration::from_nanos(order_index * nanos_per_order);
                    let actual = thread_start.elapsed();
                    if expected > actual {
                        thread::sleep(expected - actual);
                    }
                }
            }
        });
    }

    // Generator threads and publisher threads run until killed; block here.
    let _ = fanout_handle.join();
}
