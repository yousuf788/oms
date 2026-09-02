// Node-side corroboration client. Talks to the independent witness service over its
// own dedicated UDP socket (never the Raft control socket). All I/O here is blocking
// with a bounded timeout and MUST be called outside any `RaftState` lock — see
// `LeaderElection::witness_loop()` in leader_election.rs, which is the only caller.
//
// Hard rule (per the design this implements): any failure to get an affirmative
// "peers are also down" answer — no witness configured, send error, timeout, garbage
// response — resolves to "stay passive". Uncertainty never resolves to promotion.

use crate::config::{witness_host, witness_port, witness_retry_interval_ms, witness_timeout_ms};
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::Mutex;
use std::time::{Duration, Instant};

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
        #[allow(dead_code)]
        peers_checked: Vec<PeerCheck>,
        verdict: Verdict,
    },
}

#[derive(Serialize, Deserialize)]
struct PeerCheck {
    #[allow(dead_code)]
    node_id: u8,
    #[allow(dead_code)]
    reachable: bool,
    #[allow(dead_code)]
    age_ms: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq)]
enum Verdict {
    SafeToPromote,
    PeersStillUp,
}

/// The fast, lock-only value `peers_unreachable()` reads. Never involves I/O.
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum CachedVerdict {
    Unknown,
    SafeToPromote,
    StayPassive,
}

/// Richer result of one corroboration attempt, used only for operator-facing logging
/// so "witness said no" is distinguishable from "witness didn't answer" — the only
/// diagnostic tool available in this repo (no test suite).
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum CorroborationOutcome {
    NotConfigured,
    SafeToPromote,
    DeniedByWitness,
    WitnessUnreachable,
}

struct CorroborationCache {
    verdict: CachedVerdict,
    last_outcome: Option<CorroborationOutcome>,
    updated_at: Instant,
}

pub struct WitnessClient {
    addr: Option<SocketAddr>,
    socket: Option<UdpSocket>,
    timeout: Duration,
    retry_interval: Duration,
    cache: Mutex<CorroborationCache>,
}

impl WitnessClient {
    pub fn new() -> Self {
        let addr = witness_host().and_then(|host| {
            (host.as_str(), witness_port())
                .to_socket_addrs()
                .ok()
                .and_then(|mut it| it.next())
        });
        let socket = if addr.is_some() {
            UdpSocket::bind(("0.0.0.0", 0)).ok()
        } else {
            None
        };
        WitnessClient {
            addr,
            socket,
            timeout: Duration::from_millis(witness_timeout_ms()),
            retry_interval: Duration::from_millis(witness_retry_interval_ms()),
            cache: Mutex::new(CorroborationCache {
                verdict: CachedVerdict::Unknown,
                last_outcome: None,
                updated_at: Instant::now(),
            }),
        }
    }

    pub fn is_configured(&self) -> bool {
        self.addr.is_some()
    }

    /// Non-blocking, lock-only read — safe to call from anywhere, including while
    /// another lock (e.g. `RaftState`) is held.
    pub fn cached_verdict(&self) -> CachedVerdict {
        self.cache.lock().unwrap().verdict
    }

    /// Call once per tick from the background witness loop when the node is NOT
    /// currently isolated, so a stale verdict doesn't linger once peers return.
    pub fn reset_if_not_isolated(&self) {
        let mut c = self.cache.lock().unwrap();
        c.verdict = CachedVerdict::Unknown;
        c.last_outcome = None;
    }

    /// Whether enough time has passed since the last attempt to try again —
    /// keeps this from hammering the witness every 250ms while isolated.
    pub fn due_for_attempt(&self) -> bool {
        let c = self.cache.lock().unwrap();
        c.verdict == CachedVerdict::Unknown || c.updated_at.elapsed() >= self.retry_interval
    }

    /// Performs one corroboration round-trip (blocking, up to `self.timeout`).
    /// MUST be called outside any Raft lock. Returns the outcome (for logging) and
    /// whether it differs from the last attempt's outcome (for rate-limited logging).
    pub fn attempt_corroboration(&self, requester_id: u8, term: u64) -> (CorroborationOutcome, bool) {
        let outcome = self.attempt_corroboration_inner(requester_id, term);
        let verdict = match outcome {
            CorroborationOutcome::SafeToPromote => CachedVerdict::SafeToPromote,
            CorroborationOutcome::DeniedByWitness
            | CorroborationOutcome::WitnessUnreachable
            | CorroborationOutcome::NotConfigured => CachedVerdict::StayPassive,
        };
        let mut c = self.cache.lock().unwrap();
        let changed = c.last_outcome != Some(outcome);
        c.verdict = verdict;
        c.last_outcome = Some(outcome);
        c.updated_at = Instant::now();
        (outcome, changed)
    }

    fn attempt_corroboration_inner(&self, requester_id: u8, term: u64) -> CorroborationOutcome {
        let (addr, socket) = match (self.addr, &self.socket) {
            (Some(a), Some(s)) => (a, s),
            _ => return CorroborationOutcome::NotConfigured,
        };

        let request_id: u64 = rand::random();
        let request = CorroborationMsg::Request { request_id, requester_id, term };
        let payload = match serde_json::to_vec(&request) {
            Ok(p) => p,
            Err(_) => return CorroborationOutcome::WitnessUnreachable,
        };

        let deadline = Instant::now() + self.timeout;
        let resend_at = Instant::now() + self.timeout / 3;
        let _ = socket.send_to(&payload, addr);
        let mut resent = false;

        let mut buf = [0u8; 1024];
        loop {
            let now = Instant::now();
            if now >= deadline {
                return CorroborationOutcome::WitnessUnreachable;
            }
            if !resent && now >= resend_at {
                let _ = socket.send_to(&payload, addr);
                resent = true;
            }
            let slice = deadline.saturating_duration_since(now).min(Duration::from_millis(200));
            let _ = socket.set_read_timeout(Some(slice.max(Duration::from_millis(1))));
            match socket.recv_from(&mut buf) {
                Ok((n, _src)) => {
                    if let Ok(CorroborationMsg::Response { request_id: rid, verdict, .. }) =
                        serde_json::from_slice::<CorroborationMsg>(&buf[..n])
                    {
                        if rid == request_id {
                            return match verdict {
                                Verdict::SafeToPromote => CorroborationOutcome::SafeToPromote,
                                Verdict::PeersStillUp => CorroborationOutcome::DeniedByWitness,
                            };
                        }
                        // Mismatched/stale request_id — keep waiting until deadline.
                    }
                }
                Err(_) => continue, // this recv slice's timeout elapsed; outer loop re-checks deadline
            }
        }
    }
}
