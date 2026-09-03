// order-receiver (S3) — Aeron subscriber
// Subscribes to the result channel that the S2 leader publishes to.
// Deduplicates by order_id (handles leader failover duplicates).

mod auth;
mod checkpoint;
mod config;
mod replay_client;
mod sequence_tracker;

use config::init_config;
use replay_client::start_replay_client;
use rusteron_client::*;
use sequence_tracker::SequenceTracker;
use serde::Deserialize;
use std::ffi::CString;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const RESULT_STREAM_ID: i32 = 2001;

/// Wire format for a committed result from order-process's Aeron result
/// channel. KEEP IN SYNC WITH order-process/src/wal.rs::ReplicatedCommand —
/// order-process serializes that struct directly as the wire payload, and
/// bincode decodes positionally (by declaration order and type, not by
/// field name), so the field order/types here must match it exactly.
#[derive(Deserialize, Debug, Clone)]
struct ResultWire {
    order_id: u64,
    symbol: String,
    side: String,
    qty: u32,
    status: String,
    filled_qty: u32,
    processed_by: String,
    term: u64,
}

fn received_log_path() -> PathBuf {
    PathBuf::from("logs").join("orders-received.log")
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
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
    println!("[order-receiver] === STEP 1: Loading Receiver Configuration ===");
    let cfg = init_config();
    println!(
        "[order-receiver] Config initialized — Bind Host: {}, S3 Port: {}",
        cfg.bind_host, cfg.bind_port
    );

    println!("[order-receiver] === STEP 2: Connecting to Aeron Media Driver ===");
    let aeron_dir_path = aeron_dir();
    println!("[order-receiver] connecting to Aeron Media Driver at {aeron_dir_path}");

    let ctx = AeronContext::new().expect("Aeron context");
    let aeron_dir_cstr = CString::new(aeron_dir_path).unwrap();
    ctx.set_dir(&aeron_dir_cstr).expect("set aeron dir");
    ctx.set_error_handler(Some(|code: i32, msg: &str| {
        eprintln!("[aeron] error {code}: {msg}");
    })).expect("set error handler");

    let aeron = Aeron::new(&ctx).expect("Aeron client");
    aeron.start().expect("start aeron client");
    println!("[order-receiver] Aeron client successfully connected to media driver");

    println!("[order-receiver] === STEP 3: Subscribing to Aeron Result Channel ===");
    let channel = format!("aeron:udp?endpoint={}:{}", cfg.bind_host, cfg.bind_port);
    println!(
        "[order-receiver] subscribing on {channel} stream {RESULT_STREAM_ID}, writing to {}",
        received_log_path().display()
    );
    let channel_cstr = CString::new(channel).unwrap();
    let subscription = aeron
        .async_add_subscription(
            &channel_cstr,
            RESULT_STREAM_ID,
            Handlers::NONE,
            Handlers::NONE,
        )
        .expect("async_add_subscription")
        .poll_blocking(Duration::from_secs(10))
        .expect("subscription ready");
    println!("[order-receiver] result channel subscription ACTIVE on stream {RESULT_STREAM_ID}");

    println!("[order-receiver] === STEP 4: Starting Log Writer & Throughput Monitor ===");
    let (log_tx, log_rx) = mpsc::sync_channel::<String>(1_000_000);
    {
        let path = received_log_path();
        thread::spawn(move || {
            if let Some(parent) = path.parent() { let _ = create_dir_all(parent); }
            let mut file = OpenOptions::new().create(true).append(true).open(&path)
                .expect("cannot open orders-received.log");
            let mut buf = String::with_capacity(128 * 1024);
            loop {
                match log_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(line) => {
                        buf.push_str(&line); buf.push('\n');
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

    let received_total = Arc::new(AtomicU64::new(0));
    {
        let received_total = Arc::clone(&received_total);
        thread::spawn(move || {
            let mut last = 0u64;
            loop {
                thread::sleep(Duration::from_secs(1));
                let now = received_total.load(Ordering::Relaxed);
                println!("[order-receiver] throughput: {:>8} results/sec  total: {}", now - last, now);
                last = now;
            }
        });
    }

    println!("[order-receiver] === STEP 4b: Loading Checkpoint & Starting Replay Client ===");
    // Restores the dedup/gap watermark from the last checkpoint (0 if none)
    // instead of assuming nothing has ever been seen, and immediately
    // issues a catch-up REPLAY_REQUEST to the S2 cluster for anything since
    // that checkpoint — see checkpoint.rs and replay_client.rs.
    let start_watermark = checkpoint::load();
    if start_watermark > 0 {
        println!("[order-receiver] resuming from checkpoint: last_contiguous={start_watermark}");
    }
    let tracker = Arc::new(Mutex::new(SequenceTracker::with_watermark(start_watermark)));
    checkpoint::start_checkpoint_writer(Arc::clone(&tracker));
    start_replay_client(cfg.s2_nodes.clone(), Arc::clone(&tracker), start_watermark);

    println!("[order-receiver] === STEP 5: Entering Poll Loop for Inbound Result Stream ===");
    let mut idle = BackoffIdleStrategy::new();

    println!("[order-receiver] ready, polling for results...");
    loop {
        let fragments = subscription
            .poll_fn(|buf: &[u8], _hdr: AeronHeader| {
                let Some(payload) = auth::verify(buf) else {
                    eprintln!(
                        "[order-receiver] dropped result packet ({} bytes): HMAC failure — check CLUSTER_HMAC_KEY in .env",
                        buf.len()
                    );
                    return;
                };
                if let Ok(result) = bincode::deserialize::<ResultWire>(payload) {
                    // Dedup + gap tracking across the whole process lifetime
                    // (not just an in-memory HashSet that forgets on
                    // restart) — replays and leader-failover redeliveries
                    // are recognized and skipped here.
                    if !tracker.lock().unwrap().mark(result.order_id) {
                        return;
                    }
                    let received_ts_ms = now_ms();
                    let line = format!(
                        "{} {} {} {} {} {} {} {} {}",
                        result.order_id, result.symbol, result.side, result.qty,
                        result.status, result.filled_qty, result.processed_by,
                        result.term, received_ts_ms,
                    );
                    let _ = log_tx.try_send(line);
                    received_total.fetch_add(1, Ordering::Relaxed);
                }
            }, 256)
            .unwrap_or(0);

        idle.idle(fragments);
    }
}
