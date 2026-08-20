// order-receiver (S3)
// Bind host/port from `.env` (BIND_HOST, S3_PORT / RECEIVER_BIND_PORT).

mod config;

use config::init_config;
use serde_json::Value;
use std::net::UdpSocket;

fn main() {
    let cfg = init_config();
    let socket = UdpSocket::bind((cfg.bind_host.as_str(), cfg.bind_port))
        .expect("failed to bind receiver socket");
    println!(
        "[order-receiver] listening on {}:{}",
        cfg.bind_host, cfg.bind_port
    );

    let mut buf = [0u8; 4096];
    loop {
        let (n, src) = match socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Ok(result) = serde_json::from_slice::<Value>(&buf[..n]) {
            println!(
                "[order-receiver] from {} received final result: {}",
                src, result
            );
        }
    }
}
