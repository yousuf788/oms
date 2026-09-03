// order-receiver -> order-process replay-request client (S2<->S3 hop).
// Broadcasts a HMAC-signed REPLAY_REQUEST to all 3 S2 nodes' replay ports —
// order-receiver doesn't track Raft leadership, so it can't address just
// the leader directly; only the current leader acts on the request while
// followers silently ignore it (see order-process's replay_server.rs).
// Same debounce+backoff design as order-process/src/replay_client.rs.

use crate::auth;
use crate::config::S2NodeAddr;
use crate::sequence_tracker::SequenceTracker;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize, Debug)]
struct ReplayRequest {
    /// order-receiver isn't a Raft node — 0 is a non-node-id sentinel;
    /// order-process's replay_server.rs doesn't need to route on it since
    /// order-receiver is the only result-channel subscriber.
    requester_id: u8,
    ranges: Vec<(u64, u64)>,
}

const TICK: Duration = Duration::from_millis(50);
const DEBOUNCE: Duration = Duration::from_millis(50);
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// `startup_watermark` is the checkpoint loaded at startup (0 if none) —
/// this unconditionally asks for everything after it once, up front, so a
/// restart recovers even if the pipeline is otherwise idle (no new live
/// result would otherwise arrive to reveal the gap via `missing_ranges()`).
pub fn start_replay_client(
    nodes: Vec<S2NodeAddr>,
    tracker: Arc<Mutex<SequenceTracker>>,
    startup_watermark: u64,
) {
    thread::spawn(move || {
        let socket = match UdpSocket::bind(("0.0.0.0", 0)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[order-receiver] failed to bind replay-request socket: {e}");
                return;
            }
        };

        broadcast(&socket, &nodes, &[(startup_watermark + 1, u64::MAX)]);

        let mut first_seen: HashMap<(u64, u64), Instant> = HashMap::new();
        let mut next_retry: HashMap<(u64, u64), Instant> = HashMap::new();
        let mut backoff: HashMap<(u64, u64), Duration> = HashMap::new();

        loop {
            thread::sleep(TICK);

            let ranges = { tracker.lock().unwrap().missing_ranges() };
            let now = Instant::now();
            let live: HashSet<(u64, u64)> = ranges.iter().copied().collect();
            first_seen.retain(|k, _| live.contains(k));
            next_retry.retain(|k, _| live.contains(k));
            backoff.retain(|k, _| live.contains(k));

            let mut due: Vec<(u64, u64)> = Vec::new();
            for range in ranges {
                let seen_at = *first_seen.entry(range).or_insert(now);
                if now.duration_since(seen_at) < DEBOUNCE {
                    continue;
                }
                let retry_at = *next_retry.entry(range).or_insert(now);
                if now < retry_at {
                    continue;
                }
                due.push(range);
                let next_backoff = backoff
                    .get(&range)
                    .map(|d| (*d * 2).min(MAX_BACKOFF))
                    .unwrap_or(INITIAL_BACKOFF);
                backoff.insert(range, next_backoff);
                next_retry.insert(range, now + next_backoff);
            }

            if due.is_empty() {
                continue;
            }
            broadcast(&socket, &nodes, &due);
        }
    });
}

fn broadcast(socket: &UdpSocket, nodes: &[S2NodeAddr], ranges: &[(u64, u64)]) {
    let req = ReplayRequest { requester_id: 0, ranges: ranges.to_vec() };
    let Ok(encoded) = bincode::serialize(&req) else { return };
    let frame = auth::sign(&encoded);
    for node in nodes {
        let _ = socket.send_to(&frame, (node.host.as_str(), node.replay_port));
    }
}
