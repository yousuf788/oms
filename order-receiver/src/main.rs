// order-receiver (S3) — Aeron subscriber
// Subscribes to the result channel that the S2 leader publishes to.
// Deduplicates by order_id (handles leader failover duplicates).

mod config;

use config::init_config;
use rusteron_client::*;
use serde::Deserialize;
use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
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
    let cfg = init_config();

    // ── Connect to Aeron Media Driver ──────────────────────────────────────────
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

    // ── Subscribe to result channel ────────────────────────────────────────────
    // S2 leader publishes to our host:port, so we subscribe on our own endpoint.
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

    // ── Background log writer (buffers text lines, flushed on 64KB or 50ms
    // idle) — the receiver previously opened, wrote, and flushed the log
    // file on every single message, which was the actual throughput ceiling
    // for this service. This mirrors order-sending's writer thread. ──────────
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

    // ── Stats thread (replaces the old per-message println!, which at high
    // throughput would itself become a bottleneck via stdout's internal lock) ─
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

    // ── Poll loop ──────────────────────────────────────────────────────────────
    let mut seen_order_ids: HashSet<u64> = HashSet::new();
    let mut idle = BackoffIdleStrategy::new();

    println!("[order-receiver] ready, polling for results...");
    loop {
        let fragments = subscription
            .poll_fn(|buf: &[u8], _hdr: AeronHeader| {
                if let Ok(result) = bincode::deserialize::<ResultWire>(buf) {
                    if !seen_order_ids.insert(result.order_id) {
                        return; // deduplicate
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
