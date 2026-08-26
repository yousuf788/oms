// order-receiver (S3) — Aeron subscriber
// Subscribes to the result channel that the S2 leader publishes to.
// Deduplicates by order_id (handles leader failover duplicates).

mod config;

use config::init_config;
use rusteron_client::*;
use serde_json::Value;
use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const RESULT_STREAM_ID: i32 = 2001;

fn received_log_path() -> PathBuf {
    PathBuf::from("logs").join("orders-received.log")
}

fn append_received_log(line: &str) {
    let path = received_log_path();
    if let Some(parent) = path.parent() {
        let _ = create_dir_all(parent);
    }
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = writeln!(file, "{line}");
        let _ = file.flush();
    }
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

    // ── Poll loop ──────────────────────────────────────────────────────────────
    let mut seen_order_ids: HashSet<u64> = HashSet::new();
    let mut idle = BackoffIdleStrategy::new();

    println!("[order-receiver] ready, polling for results...");
    loop {
        let fragments = subscription
            .poll_fn(|buf: &[u8], _hdr: AeronHeader| {
                if let Ok(mut result) = serde_json::from_slice::<Value>(buf) {
                    let order_id = result.get("order_id").and_then(|v| v.as_u64());
                    if let Some(id) = order_id {
                        if !seen_order_ids.insert(id) {
                            return; // deduplicate
                        }
                    }
                    if let Some(obj) = result.as_object_mut() {
                        obj.insert(
                            "received_ts_ms".to_string(),
                            Value::Number(serde_json::Number::from(now_ms() as u64)),
                        );
                    }
                    let line = result.to_string();
                    append_received_log(&line);
                    println!("[order-receiver] received -> {line}");
                }
            }, 256)
            .unwrap_or(0);

        idle.idle(fragments);
    }
}
