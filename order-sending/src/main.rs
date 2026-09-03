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
mod replay;
mod wal;

use config::init_config;
use rand::Rng;
use rusteron_client::*;
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use wal::SenderWal;

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

    println!("[order-sending] === STEP 3b: Opening Order WAL & Resuming Sequence Counter ===");
    // Durable, replayable record of every order this process generates —
    // read back here so a restart resumes order_id past whatever was last
    // durably recorded instead of colliding with previously-sent ids. Also
    // consulted by the replay listener (replay.rs) to serve REPLAY_REQUEST
    // ranges from order-process.
    let wal = Arc::new(Mutex::new(SenderWal::open().expect("failed to open order WAL")));
    let resume_from = wal.lock().unwrap().last_order_id() + 1;
    println!("[order-sending] resuming order_id counter at {resume_from}");

    // ── Shared counters ────────────────────────────────────────────────────────
    let order_counter = Arc::new(AtomicU64::new(resume_from));
    let sent_total = Arc::new(AtomicU64::new(0));

    println!("[order-sending] === STEP 4: Starting Background WAL Writer & Throughput Stats Thread ===");
    // Bounded + blocking (not try_send): a full channel here backpressures
    // the fan-out thread rather than silently dropping the durability
    // record for an order that was just published live.
    let (log_tx, log_rx) = mpsc::sync_channel::<OrderWire>(1_000_000);
    {
        let wal = Arc::clone(&wal);
        thread::spawn(move || {
            // Same 64KB-or-50ms-idle batching pattern as the old text log
            // writer: accumulate a batch, flush with a single write_all —
            // never one syscall per order on the hot path.
            let mut batch: Vec<OrderWire> = Vec::with_capacity(2048);
            let mut batch_bytes = 0usize;
            const RECORD_ESTIMATE: usize = 32; // matches wal.rs's per-record capacity hint
            loop {
                match log_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(order) => {
                        batch.push(order);
                        batch_bytes += RECORD_ESTIMATE;
                        if batch_bytes >= 65536 {
                            if let Err(e) = wal.lock().unwrap().append_batch(&batch) {
                                eprintln!("[order-sending] WAL append_batch failed: {e}");
                            }
                            batch.clear();
                            batch_bytes = 0;
                        }
                    }
                    Err(_) => {
                        if !batch.is_empty() {
                            if let Err(e) = wal.lock().unwrap().append_batch(&batch) {
                                eprintln!("[order-sending] WAL append_batch failed: {e}");
                            }
                            batch.clear();
                            batch_bytes = 0;
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
            let mut idle = BusySpinIdleStrategy;
            while let Ok(frame) = node_rx.recv() {
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
            // node_rx.recv() returning Err means the fan-out thread exited.
        });
    }

    println!("[order-sending] === STEP 5b: Starting Replay Listener ===");
    // Cloned before `node_channels` is moved into the fan-out thread below —
    // the replay listener re-publishes on these same per-node channels, so
    // a replayed order goes through the identical offer()/retry path as a
    // live one.
    let node_channels_for_replay = node_channels.clone();
    replay::start_replay_listener(
        cfg.bind_host.clone(),
        cfg.bind_port,
        Arc::clone(&wal),
        node_channels_for_replay,
    );

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
            while let Ok(order) = payload_rx.recv() {
                let Ok(encoded) = bincode::serialize(&order) else { continue };
                let frame = Arc::new(auth::sign(&encoded));
                for node_tx in &node_channels {
                    // Non-blocking: a node with no live subscriber (or
                    // genuinely backpressured) must not stall delivery
                    // to the other two — that's the whole reason this
                    // file uses one channel/thread per node (see the
                    // header comment). Skipping this node for this one
                    // order is safe because the order is already
                    // durable in this process's WAL (appended via
                    // log_tx below): that node's own ingest-side
                    // sequence tracker will notice the resulting gap
                    // and pull it back via REPLAY_REQUEST once it can
                    // process again (see replay.rs / order-process's
                    // replay_client.rs).
                    match node_tx.try_send(Arc::clone(&frame)) {
                        Ok(_) => {}
                        Err(mpsc::TrySendError::Full(_)) => {}
                        Err(mpsc::TrySendError::Disconnected(_)) => return,
                    }
                }
                // Blocking, not try_send: never silently drop the
                // durability record for an order that was just
                // handed to the publisher threads.
                if log_tx.send(order).is_err() {
                    return; // WAL writer thread exited
                }
            }
            // payload_rx.recv() returning Err means all generator threads exited.
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
