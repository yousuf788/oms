// order-sending (S1) — high-throughput mode
// Spawns SENDER_THREADS threads (default 8), each with its own UDP socket,
// blasting orders as fast as possible to all S2 nodes.
// Log I/O is off the hot path: a background thread drains a channel and writes.
// A stats thread prints actual throughput every second.

mod config;

use config::init_config;
use rand::Rng;

use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::net::UdpSocket;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn sent_log_path() -> PathBuf {
    PathBuf::from("logs").join("orders-sent.log")
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn main() {
    let cfg = init_config();

    // Number of parallel sender threads (tune via env var SENDER_THREADS)
    let num_threads: usize = std::env::var("SENDER_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);

    // Shared atomic order-id counter — each thread atomically grabs the next id.
    let order_counter = Arc::new(AtomicU64::new(1));

    // Shared sent-count for throughput stats
    let sent_total = Arc::new(AtomicU64::new(0));

    // Background log writer: receives pre-serialised lines and writes to file.
    let (log_tx, log_rx) = mpsc::sync_channel::<String>(1_000_000);
    {
        let path = sent_log_path();
        thread::spawn(move || {
            if let Some(parent) = path.parent() {
                let _ = create_dir_all(parent);
            }
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .expect("cannot open orders-sent.log");
            // Drain in batches of 1024 for efficient I/O
            let mut buf = String::with_capacity(128 * 1024);
            loop {
                match log_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(line) => {
                        buf.push_str(&line);
                        buf.push('\n');
                        // Batch-flush when buffer is large enough or channel is empty
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

    // Stats thread: prints orders/sec every 1 second
    {
        let sent_total = Arc::clone(&sent_total);
        thread::spawn(move || {
            let mut last = 0u64;
            loop {
                thread::sleep(Duration::from_secs(1));
                let now = sent_total.load(Ordering::Relaxed);
                let rate = now - last;
                last = now;
                println!(
                    "[order-sending] throughput: {:>8} orders/sec  total: {}",
                    rate, now
                );
            }
        });
    }

    let targets: Arc<Vec<(String, u16)>> = Arc::new(
        cfg.nodes
            .iter()
            .map(|n| (n.host.clone(), n.order_port))
            .collect(),
    );

    let target_tps: u64 = std::env::var("TARGET_TPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5000);

    println!(
        "[order-sending] starting {} sender threads → target throughput: {} orders/sec → targets: {}",
        num_threads,
        if target_tps > 0 { format!("{target_tps}") } else { "unlimited".to_string() },
        targets
            .iter()
            .map(|(h, p)| format!("{h}:{p}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];
    let bind_host = cfg.bind_host.clone();

    let per_thread_tps = if target_tps > 0 {
        (target_tps as f64 / num_threads as f64).max(0.1)
    } else {
        0.0
    };
    let nanos_per_order = if per_thread_tps > 0.0 {
        (1_000_000_000.0 / per_thread_tps) as u64
    } else {
        0
    };

    let mut handles = Vec::new();
    for tid in 0..num_threads {
        let order_counter = Arc::clone(&order_counter);
        let sent_total = Arc::clone(&sent_total);
        let log_tx = log_tx.clone();
        let targets = Arc::clone(&targets);
        let bind_host = bind_host.clone();

        let handle = thread::spawn(move || {
            // Each thread gets its own socket on an ephemeral port
            let socket = UdpSocket::bind(format!("{bind_host}:0"))
                .unwrap_or_else(|_| panic!("thread {tid}: failed to bind socket"));
            // Non-blocking: if send buffer is full, drop rather than block
            socket.set_nonblocking(true).ok();

            let mut rng = rand::thread_rng();
            let thread_start = std::time::Instant::now();
            let mut order_index = 0u64;

            loop {
                let order_id = order_counter.fetch_add(1, Ordering::Relaxed);
                let symbol = symbols[rng.gen_range(0..symbols.len())];
                let side = if rng.gen_bool(0.5) { "BUY" } else { "SELL" };
                let qty: u32 = rng.gen_range(1..=10);
                let ts_ms = now_ms();

                // Build a compact JSON payload without serde overhead on hot path
                let payload = format!(
                    "{{\"order_id\":{order_id},\"symbol\":\"{symbol}\",\"side\":\"{side}\",\"qty\":{qty},\"ts_ms\":{ts_ms}}}"
                );

                // Send to all S2 nodes
                for (host, port) in targets.iter() {
                    let _ = socket.send_to(payload.as_bytes(), (host.as_str(), *port));
                }

                sent_total.fetch_add(1, Ordering::Relaxed);

                // Non-blocking log push — drop silently if channel is full
                let _ = log_tx.try_send(payload);

                order_index += 1;
                if nanos_per_order > 0 {
                    let expected_elapsed = Duration::from_nanos(order_index * nanos_per_order);
                    let actual_elapsed = thread_start.elapsed();
                    if expected_elapsed > actual_elapsed {
                        thread::sleep(expected_elapsed - actual_elapsed);
                    }
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.join();
    }
}
