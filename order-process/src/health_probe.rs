// Trivial, stateless liveness responder used only by the monitoring service to test
// whether this node's process is up. Deliberately kept on its own socket/port and
// completely decoupled from the Raft control channel (`leader_election.rs`) and its
// `Message` enum/`RaftState` — a monitoring probe must never be able to perturb
// consensus state or be misread as a Raft message.

use serde::{Deserialize, Serialize};
use std::net::UdpSocket;
use std::thread;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
enum HealthMsg {
    Ping { nonce: u64 },
    Pong { node_id: u8, nonce: u64 },
}

/// Spawns a background thread that answers `Ping` with `Pong` and silently
/// ignores anything else. No shared state, no locks, never touches `LeaderElection`.
pub fn start_health_responder(self_id: u8, bind_host: &str, health_port: u16) {
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
                let pong = HealthMsg::Pong { node_id: self_id, nonce };
                if let Ok(bytes) = serde_json::to_vec(&pong) {
                    let _ = socket.send_to(&bytes, src);
                }
            }
        }
    });
}
