// Continuous background reachability poller. Maintains an always-fresh view of
// whether each order-process node is up, so a corroboration request (corroboration.rs)
// can usually answer from cache instead of doing a fresh probe on the hot path.
//
// Also tracks each node's last-reported Raft role/term (from the same Pong,
// piggybacked — no extra round-trip) purely for operator visibility: logs
// when the believed leader changes, and periodically confirms "all nodes
// reachable" while that holds. None of this feeds back into any
// corroboration decision — order-monitoring stays a non-sequencing arbiter;
// this is display-only.

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
    /// `role`: 0 = Follower, 1 = Candidate, 2 = Leader — mirrors
    /// order-process's leader_election.rs::Role::as_u8() convention
    /// (duplicated here since no shared crate exists to share the enum
    /// type through).
    Pong {
        #[allow(dead_code)]
        node_id: u8,
        nonce: u64,
        role: u8,
        term: u64,
    },
}

#[derive(Clone, Copy)]
pub struct PeerHealth {
    pub reachable: bool,
    pub last_seen: Instant,
}

pub type HealthTable = Arc<Mutex<HashMap<u8, PeerHealth>>>;

/// While all watched nodes stay reachable, print a status line at most this
/// often — often enough to be a useful "still alive" confirmation, rare
/// enough not to spam the console/log at the ~500ms poll_interval_ms cadence.
const ALL_CLEAR_INTERVAL: Duration = Duration::from_secs(15);

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)
}

fn health_log_path() -> PathBuf {
    PathBuf::from("logs").join("health-transitions.log")
}

fn leader_log_path() -> PathBuf {
    PathBuf::from("logs").join("leader-transitions.log")
}

fn append_log(path: PathBuf, line: &str) {
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = writeln!(f, "{line}");
    }
}

/// One ping/pong round-trip against an existing socket. Returns
/// `Some((role, term))` on success, `None` on any error, timeout, or
/// mismatched nonce — never panics, never blocks past `timeout`.
fn probe_once(socket: &UdpSocket, host: &str, port: u16, timeout: Duration) -> Option<(u8, u64)> {
    let nonce: u64 = rand::random();
    let ping = HealthMsg::Ping { nonce };
    let payload = serde_json::to_vec(&ping).ok()?;
    socket.send_to(&payload, (host, port)).ok()?;
    let deadline = Instant::now() + timeout;
    let mut buf = [0u8; 256];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return None;
        }
        let _ = socket.set_read_timeout(Some(remaining));
        match socket.recv_from(&mut buf) {
            Ok((n, _src)) => {
                if let Ok(HealthMsg::Pong { nonce: got, role, term, .. }) =
                    serde_json::from_slice(&buf[..n])
                {
                    if got == nonce {
                        return Some((role, term));
                    }
                    // Stale/mismatched pong — keep waiting until the deadline.
                }
            }
            Err(_) => return None,
        }
    }
}

/// One-off fresh probe on its own ephemeral socket — used by corroboration.rs when
/// the cached entry is missing or stale, rather than answering from outdated data.
pub fn probe_now(host: &str, port: u16, timeout: Duration) -> bool {
    match UdpSocket::bind(("0.0.0.0", 0)) {
        Ok(socket) => probe_once(&socket, host, port, timeout).is_some(),
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
        let mut known_leader: Option<u8> = None;
        let mut last_all_clear = Instant::now() - ALL_CLEAR_INTERVAL;

        loop {
            let mut leaders_seen: Vec<(u8, u64)> = Vec::new(); // (node_id, term)

            for node in &cfg.nodes {
                let mut result = probe_once(&socket, &node.host, node.health_port, timeout);
                if result.is_none() {
                    if cfg.verbose {
                        println!("[monitoring] {} missed probe, retrying...", node.label());
                    }
                    for _ in 0..cfg.probe_retries {
                        result = probe_once(&socket, &node.host, node.health_port, timeout);
                        if result.is_some() {
                            break;
                        }
                    }
                }
                let reachable = result.is_some();

                let mut t = table_handle.lock().unwrap();
                let changed = t.get(&node.id).map(|p| p.reachable) != Some(reachable);
                t.insert(node.id, PeerHealth { reachable, last_seen: Instant::now() });
                drop(t);

                if changed {
                    let state = if reachable { "reachable" } else { "unreachable" };
                    append_log(
                        health_log_path(),
                        &format!("{},{},{},{},{}", now_ms(), node.id, node.name, node.host, state),
                    );
                    println!("[monitoring] {} is now {state}", node.label());
                }

                if let Some((role, term)) = result {
                    if role == 2 {
                        leaders_seen.push((node.id, term));
                    }
                }
            }

            if leaders_seen.len() > 1 {
                let names: Vec<String> = leaders_seen
                    .iter()
                    .filter_map(|(id, t)| {
                        cfg.nodes.iter().find(|n| n.id == *id).map(|n| format!("{} (term {t})", n.name))
                    })
                    .collect();
                println!(
                    "[monitoring] WARNING: multiple nodes reporting LEADER simultaneously — {}",
                    names.join(", ")
                );
            }

            // Highest-term node among current LEADER reports is the believed
            // current leader — a lower-term one alongside it is mid-step-down.
            let current_leader = leaders_seen.iter().max_by_key(|(_, t)| *t).map(|(id, _)| *id);
            if current_leader != known_leader {
                known_leader = current_leader;
                let desc = match current_leader.and_then(|id| cfg.nodes.iter().find(|n| n.id == id)) {
                    Some(n) => format!("{} is LEADER", n.label()),
                    None => "no leader currently known".to_string(),
                };
                append_log(leader_log_path(), &format!("{},{}", now_ms(), desc));
                println!("[monitoring] leader change: {desc}");
            }

            let all_reachable = {
                let t = table_handle.lock().unwrap();
                cfg.nodes.iter().all(|n| t.get(&n.id).map(|p| p.reachable).unwrap_or(false))
            };
            if all_reachable && last_all_clear.elapsed() >= ALL_CLEAR_INTERVAL {
                let leader_desc = current_leader
                    .and_then(|id| cfg.nodes.iter().find(|n| n.id == id))
                    .map(|n| n.label())
                    .unwrap_or_else(|| "none known".to_string());
                println!(
                    "[monitoring] status: all {} nodes reachable, leader={leader_desc}, at {} (epoch ms)",
                    cfg.nodes.len(),
                    now_ms()
                );
                last_all_clear = Instant::now();
            }

            thread::sleep(Duration::from_millis(cfg.poll_interval_ms));
        }
    });
    table
}
