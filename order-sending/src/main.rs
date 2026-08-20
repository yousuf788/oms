// order-sending (S1)
// Loads NODE1/2/3_HOST and ports from `.env` (no hardcoded IPs).

mod config;

use config::init_config;
use rand::Rng;
use serde_json::json;
use std::net::UdpSocket;
use std::thread;
use std::time::Duration;

fn main() {
    let cfg = init_config();
    let socket = UdpSocket::bind((cfg.bind_host.as_str(), cfg.bind_port))
        .expect("failed to bind sender socket");
    let mut order_id: u64 = 1;
    let symbols = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];

    println!(
        "[order-sending] bound on {}:{}, targets: {:?}",
        cfg.bind_host,
        cfg.bind_port,
        cfg.nodes
            .iter()
            .map(|n| format!("{}:{}", n.host, n.order_port))
            .collect::<Vec<_>>()
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
            for node in &cfg.nodes {
                let _ = socket.send_to(&buf, (node.host.as_str(), node.order_port));
            }
        }
        println!("[order-sending] sent order {}", order);
    }
}
