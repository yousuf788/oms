// order-sending (S1)
// Loads NODE1/2/3_HOST and ports from `.env` (no hardcoded IPs).
// Appends each sent order to logs/orders-sent.log

mod config;

use config::init_config;
use rand::Rng;
use serde_json::json;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::net::UdpSocket;
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn sent_log_path() -> PathBuf {
    PathBuf::from("logs").join("orders-sent.log")
}

fn append_sent_log(line: &str) {
    let path = sent_log_path();
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

fn main() {
    let cfg = init_config();
    let socket = UdpSocket::bind((cfg.bind_host.as_str(), cfg.bind_port))
        .expect("failed to bind sender socket");
    let mut order_id: u64 = 1;
    let symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];

    println!(
        "[order-sending] bound on {}:{}, writing to {}",
        cfg.bind_host,
        cfg.bind_port,
        sent_log_path().display()
    );
    println!(
        "[order-sending] targets: {}",
        cfg.nodes
            .iter()
            .map(|n| format!("{}:{}", n.host, n.order_port))
            .collect::<Vec<_>>()
            .join(", ")
    );

    loop {
        thread::sleep(Duration::from_secs(1));
        let mut rng = rand::thread_rng();
        let order = json!({
            "order_id": order_id,
            "symbol": symbols[rng.gen_range(0..symbols.len())],
            "side": if rng.gen_bool(0.5) { "BUY" } else { "SELL" },
            "qty": rng.gen_range(1..=10),
            "ts_ms": now_ms(),
        });
        order_id += 1;

        if let Ok(buf) = serde_json::to_vec(&order) {
            for node in &cfg.nodes {
                let _ = socket.send_to(&buf, (node.host.as_str(), node.order_port));
            }
        }
        let line = order.to_string();
        append_sent_log(&line);
        println!("[order-sending] sent -> {}", line);
    }
}
