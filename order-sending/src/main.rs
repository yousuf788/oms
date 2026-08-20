// order-sending (S1)
// Deploy on Yousuf (or any machine that can reach all S2 order ports).
//
// Build: cargo build -p order-sending --release
// Run:   ./target/release/order-sending

mod config;

use config::{S2_NODES, SENDER_BIND_HOST, SENDER_BIND_PORT};
use rand::Rng;
use serde_json::json;
use std::net::UdpSocket;
use std::thread;
use std::time::Duration;

fn main() {
    let socket = UdpSocket::bind((SENDER_BIND_HOST, SENDER_BIND_PORT))
        .expect("failed to bind sender socket");
    let mut order_id: u64 = 1;
    let symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];

    println!(
        "[order-sending] bound on {}:{}, publishing to all S2 nodes every 1s...",
        SENDER_BIND_HOST, SENDER_BIND_PORT
    );

    loop {
        thread::sleep(Duration::from_secs(1));
        let mut rng = rand::thread_rng();
        let order = json!({
            "order_id": order_id,
            "symbol": symbols[rng.gen_range(0..symbols.len())],
            "side": if rng.gen_bool(0.5) { "BUY" } else { "SELL" },
            "qty": rng.gen_range(1..=10),
        });
        order_id += 1;

        if let Ok(buf) = serde_json::to_vec(&order) {
            for node in S2_NODES.iter() {
                let _ = socket.send_to(&buf, (node.host, node.order_port));
            }
        }
        println!("[order-sending] sent order {}", order);
    }
}
