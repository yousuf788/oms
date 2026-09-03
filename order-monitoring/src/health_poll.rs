// Continuous background reachability poller. Maintains an always-fresh view of
// whether each order-process node is up, so a corroboration request (corroboration.rs)
// can usually answer from cache instead of doing a fresh probe on the hot path.

use crate::config::config;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::UdpSocket;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HealthMsg {
    Ping { nonce: u64 },
    Pong {
        #[allow(dead_code)]
        node_id: u8,
        nonce: u64,
    },
}

#[derive(Clone, Copy)]
pub struct PeerHealth {
    pub reachable: bool,
    pub last_seen: Instant,
}

pub type HealthTable = Arc<Mutex<HashMap<u8, PeerHealth>>>;

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)
}

fn log_path() -> PathBuf {
    PathBuf::from("logs").join("health-transitions.log")
}

fn append_log(line: &str) {
    let path = log_path();
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = writeln!(f, "{line}");
    }
}

/// One ping/pong round-trip against an existing socket. `false` on any error
/// or timeout — never panics, never blocks past `timeout`.
fn probe_once(socket: &UdpSocket, host: &str, port: u16, timeout: Duration) -> bool {
    let nonce: u64 = rand::random();
    let ping = HealthMsg::Ping { nonce };
    let Ok(payload) = serde_json::to_vec(&ping) else {
        return false;
    };
    if socket.send_to(&payload, (host, port)).is_err() {
        return false;
    }
    let deadline = Instant::now() + timeout;
    let mut buf = [0u8; 256];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return false;
        }
        let _ = socket.set_read_timeout(Some(remaining));
        match socket.recv_from(&mut buf) {
            Ok((n, _src)) => {
                if let Ok(HealthMsg::Pong { nonce: got, .. }) = serde_json::from_slice(&buf[..n]) {
                    if got == nonce {
                        return true;
                    }
                    // Stale/mismatched pong — keep waiting until the deadline.
                }
            }
            Err(_) => return false,
        }
    }
}

/// One-off fresh probe on its own ephemeral socket — used by corroboration.rs when
/// the cached entry is missing or stale, rather than answering from outdated data.
pub fn probe_now(host: &str, port: u16, timeout: Duration) -> bool {
    match UdpSocket::bind(("0.0.0.0", 0)) {
        Ok(socket) => probe_once(&socket, host, port, timeout),
        Err(_) => false,
    }
}

/// Spawns the continuous poller and returns the shared table it maintains.
pub fn start_health_poller() -> HealthTable {
    let table: HealthTable = Arc::new(Mutex::new(HashMap::new()));
    let table_handle = Arc::clone(&table);
    thread::spawn(move || {
        let cfg = config();
        let socket = UdpSocket::bind((cfg.bind_host.as_str(), 0))
            .expect("failed to bind monitoring probe socket");
        let timeout = Duration::from_millis(cfg.probe_timeout_ms);
        loop {
            for node in &cfg.nodes {
                let mut reachable = probe_once(&socket, &node.host, node.health_port, timeout);
                if !reachable {
                    if cfg.verbose {
                        println!("[monitoring] {} missed probe, retrying...", node.label());
                    }
                    for _ in 0..cfg.probe_retries {
                        if probe_once(&socket, &node.host, node.health_port, timeout) {
                            reachable = true;
                            break;
                        }
                    }
                }

                let mut t = table_handle.lock().unwrap();
                let changed = t.get(&node.id).map(|p| p.reachable) != Some(reachable);
                t.insert(node.id, PeerHealth { reachable, last_seen: Instant::now() });
                drop(t);

                if changed {
                    let state = if reachable { "reachable" } else { "unreachable" };
                    append_log(&format!(
                        "{},{},{},{},{}",
                        now_ms(),
                        node.id,
                        node.name,
                        node.host,
                        state
                    ));
                    println!("[monitoring] {} is now {state}", node.label());
                }
            }
            thread::sleep(Duration::from_millis(cfg.poll_interval_ms));
        }
    });
    table
}
