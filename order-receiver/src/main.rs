// order-receiver (S3)
// Bind host/port from `.env` (BIND_HOST, S3_PORT).
// Appends each received result to logs/orders-received.log

mod config;

use config::init_config;
use serde_json::Value;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::net::UdpSocket;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

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

fn main() {
    let cfg = init_config();
    let socket = UdpSocket::bind((cfg.bind_host.as_str(), cfg.bind_port))
        .expect("failed to bind receiver socket");
    println!(
        "[order-receiver] listening on {}:{}, writing to {}",
        cfg.bind_host,
        cfg.bind_port,
        received_log_path().display()
    );

    let mut buf = [0u8; 4096];
    loop {
        let (n, src) = match socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Ok(mut result) = serde_json::from_slice::<Value>(&buf[..n]) {
            if let Some(obj) = result.as_object_mut() {
                obj.insert("from".to_string(), Value::String(src.to_string()));
                obj.insert("received_ts_ms".to_string(), json_u128(now_ms()));
            }
            let line = result.to_string();
            append_received_log(&line);
            println!("[order-receiver] received -> {}", line);
        }
    }
}

fn json_u128(v: u128) -> Value {
    Value::Number(serde_json::Number::from(v as u64))
}
