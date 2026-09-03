// order-process -> order-sending replay-request client (S1<->S2 hop). When
// the ingest `SequenceTracker` reports a gap that has persisted past a
// short debounce window, sends a HMAC-signed REPLAY_REQUEST to
// order-sending's replay listener asking it to re-publish the missing
// order_id range(s) on the Aeron order channel. Runs on its own ticker,
// independent of the Raft tick loop, and never blocks the hot ingest path —
// it only reads a snapshot of the tracker under a brief lock.
//
// Debounce: a gap must persist for DEBOUNCE before it's requested at all,
// since most "gaps" are just generator-thread reordering that resolves
// itself within microseconds (order_id is assigned by a shared atomic
// counter across multiple generator threads, so publish order and
// order_id order aren't always identical — see order-sending/src/main.rs).
// Backoff: repeated requests for a still-outstanding range back off
// exponentially up to MAX_BACKOFF, so a prolonged outage doesn't turn into
// a tight request loop.

use crate::auth;
use crate::sequence_tracker::SequenceTracker;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Serialize, Deserialize, Debug)]
struct ReplayRequest {
    requester_id: u8,
    ranges: Vec<(u64, u64)>,
}

const TICK: Duration = Duration::from_millis(50);
const DEBOUNCE: Duration = Duration::from_millis(50);
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// `startup_watermark` is the highest `order_id` this node has already
/// committed to its own WAL (0 if none) — this unconditionally requests
/// everything after it once, up front, so a restart catches up immediately
/// instead of waiting for a new live order to happen to reveal the gap via
/// `missing_ranges()`. order-sending keeps sending regardless of whether any
/// particular node is caught up, so this is what makes catch-up automatic
/// rather than depending on when the next live order lands.
pub fn start_replay_client(
    self_id: u8,
    s1_host: String,
    s1_replay_port: u16,
    tracker: Arc<Mutex<SequenceTracker>>,
    startup_watermark: u64,
) {
    thread::spawn(move || {
        let socket = match UdpSocket::bind(("0.0.0.0", 0)) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[S2-{self_id}] failed to bind replay-request socket: {e}");
                return;
            }
        };

        send_request(&socket, &s1_host, s1_replay_port, self_id, &[(startup_watermark + 1, u64::MAX)]);
        if crate::config::verbose_raft() {
            println!(
                "[S2-{self_id}] startup catch-up: requested replay from order_id {} onward",
                startup_watermark + 1
            );
        }

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
                    continue; // give reordering a chance to resolve itself first
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

            send_request(&socket, &s1_host, s1_replay_port, self_id, &due);
            if crate::config::verbose_raft() {
                println!(
                    "[S2-{self_id}] requested replay of {} range(s) from order-sending: {:?}",
                    due.len(),
                    due
                );
            }
        }
    });
}

fn send_request(socket: &UdpSocket, host: &str, port: u16, requester_id: u8, ranges: &[(u64, u64)]) {
    let req = ReplayRequest { requester_id, ranges: ranges.to_vec() };
    if let Ok(encoded) = bincode::serialize(&req) {
        let frame = auth::sign(&encoded);
        let _ = socket.send_to(&frame, (host, port));
    }
}
