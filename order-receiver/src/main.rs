// order-receiver (S3)
// Deploy on Yousuf.
//
// Build: cargo build -p order-receiver --release
// Run:   ./target/release/order-receiver

mod config;

use config::{RECEIVER_BIND_HOST, RECEIVER_BIND_PORT};
use serde_json::Value;
use std::net::UdpSocket;

fn main() {
    let socket = UdpSocket::bind((RECEIVER_BIND_HOST, RECEIVER_BIND_PORT))
        .expect("failed to bind receiver socket");
    println!(
        "[order-receiver] listening on {}:{}",
        RECEIVER_BIND_HOST, RECEIVER_BIND_PORT
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
