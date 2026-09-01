// order-sending (S1) — Aeron high-throughput publisher
// Architecture:
//   - N generator threads produce order payloads → bounded channel
//   - 1 publisher thread drains the channel and offers to all 3 Aeron publications
//   - Backpressure: if publications are slow, generator threads block (no silent drops)
// Rate-paced to TARGET_TPS orders/sec (default 5000).

mod config;

use config::init_config;
use rand::Rng;
use rusteron_client::*;
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

const ORDER_STREAM_ID: i32 = 1001;

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
    let cfg = init_config();

    let target_tps: u64 = std::env::var("TARGET_TPS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(5000);
    let num_gen_threads: usize = std::env::var("SENDER_THREADS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(8);

    // ── Connect to Aeron Media Driver ──────────────────────────────────────────
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

    // ── Create Aeron publications — one per S2 node (unicast) ─────────────────
    let mut publications = Vec::new();
    for (i, node) in cfg.nodes.iter().enumerate() {
        let ch = format!("aeron:udp?endpoint={}:{}", node.host, node.order_port);
        println!("[order-sending] publication[{i}] → {ch} stream {ORDER_STREAM_ID}");
        let ch_cstr = CString::new(ch).unwrap();
        let pub_ = aeron
            .async_add_exclusive_publication(&ch_cstr, ORDER_STREAM_ID)
            .unwrap_or_else(|e| panic!("add_publication node {}: {e}", i + 1))
            .poll_blocking(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("poll_publication node {}: {e}", i + 1));
        publications.push(pub_);
    }

    // ── Shared counters ────────────────────────────────────────────────────────
    let order_counter = Arc::new(AtomicU64::new(1));
    let sent_total = Arc::new(AtomicU64::new(0));

    // ── Channel: generator threads → publisher thread ──────────────────────────
    // Bounded so generator threads apply backpressure when publisher is slow.
    let (payload_tx, payload_rx) = mpsc::sync_channel::<String>(target_tps.max(10000) as usize);

    // ── Background log writer ──────────────────────────────────────────────────
    let (log_tx, log_rx) = mpsc::sync_channel::<String>(1_000_000);
    {
        let path = sent_log_path();
        thread::spawn(move || {
            if let Some(parent) = path.parent() { let _ = create_dir_all(parent); }
            let mut file = OpenOptions::new().create(true).append(true).open(&path)
                .expect("cannot open orders-sent.log");
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

    // ── Stats thread ───────────────────────────────────────────────────────────
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

    // ── Rate pacing ────────────────────────────────────────────────────────────
    let per_thread_tps = if target_tps > 0 {
        (target_tps as f64 / num_gen_threads as f64).max(0.1)
    } else { 0.0 };
    let nanos_per_order = if per_thread_tps > 0.0 {
        (1_000_000_000.0 / per_thread_tps) as u64
    } else { 0 };

    let symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];

    println!(
        "[order-sending] starting {num_gen_threads} generator threads → {target_tps} orders/sec"
    );

    // ── Generator threads: build payloads ──────────────────────────────────────
    {
        let order_counter = Arc::clone(&order_counter);
        let sent_total = Arc::clone(&sent_total);
        let log_tx = log_tx.clone();
        for _tid in 0..num_gen_threads {
            let order_counter = Arc::clone(&order_counter);
            let sent_total = Arc::clone(&sent_total);
            let log_tx = log_tx.clone();
            let payload_tx = payload_tx.clone();
            thread::spawn(move || {
                let mut rng = rand::thread_rng();
                let thread_start = Instant::now();
                let mut order_index = 0u64;
                loop {
                    let order_id = order_counter.fetch_add(1, Ordering::Relaxed);
                    let symbol = symbols[rng.gen_range(0..symbols.len())];
                    let side = if rng.gen_bool(0.5) { "BUY" } else { "SELL" };
                    let qty: u32 = rng.gen_range(1..=10);
                    let ts_ms = now_ms();
                    let payload = format!(
                        "{{\"order_id\":{order_id},\"symbol\":\"{symbol}\",\"side\":\"{side}\",\"qty\":{qty},\"ts_ms\":{ts_ms}}}"
                    );

                    // Send to publisher thread (blocks if publisher is busy → backpressure)
                    if payload_tx.send(payload.clone()).is_err() {
                        break; // publisher thread exited
                    }
                    sent_total.fetch_add(1, Ordering::Relaxed);
                    let _ = log_tx.try_send(payload);

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
    }

    // ── Publisher thread: drains channel → offers to all Aeron publications ────
    // Single thread owns all AeronExclusivePublication (not Send/Sync).
    //
    // A node with no live subscriber reports back pressure as `NotConnected`,
    // which `is_retryable()` correctly treats as retryable (a subscriber may
    // join later) — but retrying it unboundedly here would let one dead node
    // freeze the publisher thread forever, and with it the bounded channel and
    // every generator thread behind it. Skip a not-yet-connected publication
    // for this message instead of blocking; it catches up once connected.
    // Genuine back pressure from an already-connected node still retries, capped
    // as a safety net in case a node drops mid-stream.
    const MAX_BACKPRESSURE_RETRIES: u32 = 100_000;
    let mut idle = BusySpinIdleStrategy::default();
    loop {
        match payload_rx.recv() {
            Ok(payload) => {
                let payload_bytes = payload.as_bytes();
                for pub_ in &publications {
                    if !pub_.is_connected() {
                        continue;
                    }
                    let mut retries = 0u32;
                    loop {
                        match pub_.offer(payload_bytes) {
                            Ok(_) => break,
                            Err(e) if e.is_retryable() && retries < MAX_BACKPRESSURE_RETRIES => {
                                retries += 1;
                                idle.idle(0);
                                continue;
                            }
                            Err(e) => {
                                eprintln!("[order-sending] publish error: {e}");
                                break;
                            }
                        }
                    }
                }
            }
            Err(_) => break, // all generator threads exited
        }
    }
}
