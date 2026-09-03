// Trivial, stateless liveness responder used only by the monitoring service to test
// whether this node's process is up. Deliberately kept on its own socket/port and
// completely decoupled from the Raft control channel (`leader_election.rs`) and its
// `Message` enum/`RaftState` — a monitoring probe must never be able to perturb
// consensus state or be misread as a Raft message.
//
// It DOES report this node's current role/term in its Pong reply (for
// order-monitoring's leader display — see order-monitoring/src/health_poll.rs),
// but only ever reads it lock-free from a couple of atomics that
// LeaderElection refreshes once per Raft tick — never a lock, never the
// `Message` enum, never `RaftState` itself. See leader_election.rs's
// `role_atomic`/`term_atomic` fields for where these are written.

use serde::{Deserialize, Serialize};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HealthMsg {
    Ping { nonce: u64 },
    /// `role`: 0 = Follower, 1 = Candidate, 2 = Leader — see
    /// leader_election.rs::Role::as_u8(). Duplicated convention (no shared
    /// crate to share the enum type through), documented in both places.
    Pong { node_id: u8, nonce: u64, role: u8, term: u64 },
}

/// Spawns a background thread that answers `Ping` with `Pong` and silently
/// ignores anything else. No locks held, never touches `LeaderElection`
/// directly — `role`/`term` are lock-free atomic reads only.
pub fn start_health_responder(
    self_id: u8,
    bind_host: &str,
    health_port: u16,
    role: Arc<AtomicU8>,
    term: Arc<AtomicU64>,
) {
    let bind_host = bind_host.to_string();
    thread::spawn(move || {
        let socket = match UdpSocket::bind((bind_host.as_str(), health_port)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[health] failed to bind health responder on {bind_host}:{health_port}: {e}");
                return;
            }
        };
        let mut buf = [0u8; 256];
        loop {
            let (n, src) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if let Ok(HealthMsg::Ping { nonce }) = serde_json::from_slice(&buf[..n]) {
                let pong = HealthMsg::Pong {
                    node_id: self_id,
                    nonce,
                    role: role.load(Ordering::Relaxed),
                    term: term.load(Ordering::Relaxed),
                };
                if let Ok(bytes) = serde_json::to_vec(&pong) {
                    let _ = socket.send_to(&bytes, src);
                }
            }
        }
    });
}
