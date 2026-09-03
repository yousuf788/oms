// UDP responder for CorroborationRequest from order-process nodes. Answers from the
// health_poll cache (near-instant) unless the cached entry is stale, in which case it
// falls back to one fresh synchronous probe rather than answer from outdated data.
//
// Security: every inbound request must carry a valid HMAC-SHA256 tag signed with
// monitoring_HMAC_KEY. Unauthenticated packets are dropped without a response.
// Every outbound response is also signed so order-process nodes can verify it.
use crate::auth;
use crate::config::{config, find_node, other_nodes};
use crate::health_poll::{probe_now, HealthTable, PeerHealth};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::UdpSocket;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum CorroborationMsg {
    Request {
        request_id: u64,
        requester_id: u8,
        term: u64,
    },
    Response {
        request_id: u64,
        peers_checked: Vec<PeerCheck>,
        verdict: Verdict,
    },
}

#[derive(Serialize, Deserialize)]
struct PeerCheck {
    node_id: u8,
    #[serde(default)]
    name: String,
    #[serde(default)]
    host: String,
    reachable: bool,
    age_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Debug)]
enum Verdict {
    SafeToPromote,
    PeersStillUp,
}

fn now_ms() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0)
}

fn log_path() -> PathBuf {
    PathBuf::from("logs").join("corroboration.log")
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

pub fn start_corroboration_responder(table: HealthTable) {
    let cfg = config();
    let socket = UdpSocket::bind((cfg.bind_host.as_str(), cfg.monitoring_port))
        .expect("failed to bind monitoring corroboration responder");
    println!(
        "[monitoring] listening for corroboration requests on {}:{}",
        cfg.bind_host, cfg.monitoring_port
    );

    let stale_after = Duration::from_millis(cfg.poll_interval_ms.saturating_mul(2));
    let probe_timeout = Duration::from_millis(cfg.probe_timeout_ms);
    let mut buf = [0u8; 1024];

    loop {
        let (n, src) = match socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // ── HMAC verification ────────────────────────────────────────
        // Reject any corroboration request that lacks a valid monitoring_HMAC_KEY
        // signature. Without this, an attacker can forge SafeToPromote responses
        // by replaying or crafting requests to fish for them.
        let inner = match auth::verify(&buf[..n]) {
            Some(p) => p,
            None => {
                eprintln!("[monitoring] dropping corroboration request from {src}: HMAC failure");
                continue;
            }
        };

        let Ok(CorroborationMsg::Request { request_id, requester_id, term }) =
            serde_json::from_slice::<CorroborationMsg>(inner)
        else {
            continue; // garbage/unrecognized packet — silently ignored
        };

        let start = Instant::now();
        let peers = other_nodes(requester_id);
        let mut checks = Vec::with_capacity(peers.len());
        for peer in &peers {
            let cached = table.lock().unwrap().get(&peer.id).copied();
            let (reachable, age_ms) = match cached {
                Some(PeerHealth { reachable, last_seen }) if last_seen.elapsed() < stale_after => {
                    (reachable, last_seen.elapsed().as_millis() as u64)
                }
                _ => {
                    // Cache missing or stale — one fresh probe rather than a guess.
                    (probe_now(&peer.host, peer.health_port, probe_timeout), 0)
                }
            };
            checks.push(PeerCheck {
                node_id: peer.id,
                name: peer.name.clone(),
                host: peer.host.clone(),
                reachable,
                age_ms,
            });
        }

        let verdict = if checks.iter().all(|c| !c.reachable) {
            Verdict::SafeToPromote
        } else {
            Verdict::PeersStillUp
        };

        let requester_label = find_node(requester_id)
            .map(|n| n.label())
            .unwrap_or_else(|| format!("node {requester_id}"));

        append_log(&format!(
            "{},{},{},{},{},{verdict:?}",
            now_ms(),
            request_id,
            requester_label,
            term,
            checks
                .iter()
                .map(|c| format!(
                    "{} ({}):{}:{}ms",
                    c.name,
                    c.host,
                    if c.reachable { "up" } else { "down" },
                    c.age_ms
                ))
                .collect::<Vec<_>>()
                .join("|"),
        ));
        println!(
            "[monitoring] corroboration request from {requester_label} (term {term}) -> {verdict:?} ({}ms)",
            start.elapsed().as_millis()
        );

        let response = CorroborationMsg::Response { request_id, peers_checked: checks, verdict };
        if let Ok(inner) = serde_json::to_vec(&response) {
            // Sign the response so order-process nodes can verify it.
            // A response without a valid HMAC is treated as MonitoringUnreachable
            // (fail-safe: stay passive) on the receiving end.
            let frame = auth::sign(&inner);
            let _ = socket.send_to(&frame, src);
        }
    }
}
