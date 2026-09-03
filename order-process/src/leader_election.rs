use crate::auth;
use crate::config::{
    allow_single_node_leader, election_timeout_max_ms, election_timeout_min_ms, find_node,
    format_role_summary, heartbeat_interval_ms, node_name, peer_silent_ms,
    require_monitoring_for_single_node_leader, s2_nodes, verbose_raft, S2Node,
};
use crate::wal::{LogEntry, ReplicatedCommand, Wal};
use crate::monitoring_client::{CachedVerdict, CorroborationOutcome, monitoringClient};
use rand::Rng;
use rusteron_client::{AeronPublication, BusySpinIdleStrategy, IdleStrategy};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Byte budget for a single `AppendEntries` UDP datagram, sized to stay
/// safely under standard Ethernet MTU (1500 bytes) after IP/UDP headers, so
/// replication never silently depends on IP fragmentation (a single lost
/// fragment loses the whole datagram).
const APPEND_BATCH_BYTE_BUDGET: usize = 1400;
const RECV_BUF_SIZE: usize = 2_000_000;

/// Takes as many `entries` (in order) as fit within `budget_bytes` once
/// bincode-encoded, always taking at least one entry even if it alone
/// exceeds the budget, so replication keeps making progress regardless of
/// how large a single command's payload gets.
fn entries_within_budget(entries: Vec<LogEntry>, budget_bytes: usize) -> Vec<LogEntry> {
    let mut taken = Vec::new();
    let mut total = 0usize;
    for entry in entries {
        let size = bincode::serialized_size(&entry).unwrap_or(0) as usize;
        if !taken.is_empty() && total + size > budget_bytes {
            break;
        }
        total += size;
        taken.push(entry);
    }
    taken
}

/// Binds a UDP socket for the Raft control channel with larger send/receive
/// buffers than the OS default — at high replication rates this raw socket
/// has no other flow control, so an undersized OS buffer is a real loss point.
fn bind_tuned_udp_socket(bind_host: &str, port: u16) -> UdpSocket {
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::ToSocketAddrs;

    let addr: std::net::SocketAddr = (bind_host, port)
        .to_socket_addrs()
        .expect("resolve raft control bind address")
        .next()
        .expect("no address for raft control bind host/port");
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
        .expect("create raft control socket");
    socket
        .set_recv_buffer_size(8 * 1024 * 1024)
        .expect("set SO_RCVBUF on raft control socket");
    socket
        .set_send_buffer_size(8 * 1024 * 1024)
        .expect("set SO_SNDBUF on raft control socket");
    socket.bind(&addr.into()).expect("bind raft control socket");
    socket.into()
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Serialize, Deserialize, Debug)]
enum Message {
    RequestVote {
        term: u64,
        candidate_id: u8,
        last_log_index: u64,
        last_log_term: u64,
    },
    VoteGranted { term: u64, voter_id: u8 },
    AppendEntries {
        term: u64,
        leader_id: u8,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendAck {
        term: u64,
        follower_id: u8,
        success: bool,
        match_index: u64,
    },
}

struct RaftState {
    term: u64,
    role: Role,
    voted_for: Option<u8>,
    leader_id: Option<u8>,
    votes: HashSet<u8>,
    next_index: HashMap<u8, u64>,
    match_index: HashMap<u8, u64>,
    commit_index: u64,
    last_applied: u64,
    last_heartbeat: Instant,
    election_timeout: Duration,
    /// Last time any raft message arrived from another node.
    last_peer_contact: Instant,
    /// Per-peer last raft contact (for availability lines).
    peer_last_contact: HashMap<u8, Instant>,
    /// true = available; used to print only on change.
    peer_available: HashMap<u8, bool>,
    /// Per-peer timestamp of last successful AppendAck — used for leader lease.
    peer_last_ack: HashMap<u8, Instant>,
    /// The Raft term at which this node first became leader in the current tenure.
    /// WAL entries at terms [leader_since_term, current_term] are safe to commit
    /// even when a reconnecting peer bumps our term while we stay leader.
    leader_since_term: u64,
}

fn random_timeout() -> Duration {
    let ms = rand::thread_rng().gen_range(election_timeout_min_ms()..=election_timeout_max_ms());
    Duration::from_millis(ms)
}

pub struct LeaderElection {
    self_id: u8,
    peers: Vec<S2Node>,
    socket: UdpSocket,
    state: Mutex<RaftState>,
    wal: Mutex<Wal>,
    is_leader_flag: AtomicBool,
    /// Resolved peer IP addresses for the source-IP allowlist in recv_loop.
    /// Built once at startup from NODE*_HOST config values.
    allowed_ips: Vec<IpAddr>,
    /// When this node started — used to enforce a startup grace period before
    /// allowing single-node self-election. Prevents all nodes self-electing
    /// simultaneously on startup before peers have had a chance to respond.
    started_at: Instant,
    /// Independent-monitoring corroboration client — see `monitoring_loop()` and
    /// `peers_unreachable()`. A local isolation timeout is never sufficient on
    /// its own to justify self-promotion; this is what corroborates it.
    monitoring: monitoringClient,
}

impl LeaderElection {
    /// Binds the control-channel socket and spawns the background
    /// recv + election/heartbeat ticker threads. Returns immediately;
    /// call `.is_leader()` from anywhere to check current status.
    pub fn start(
        self_id: u8,
        result_pub: AeronPublication,
        replay_rx: Receiver<(u64, u64)>,
    ) -> Arc<Self> {
        let self_node = find_node(self_id).expect("unknown node id");
        let peers: Vec<S2Node> = s2_nodes()
            .iter()
            .filter(|n| n.id != self_id)
            .cloned()
            .collect();
        // Peers are unknown at startup — treat contact time as now so the
        // peer_silent_ms window starts from this moment, not from the past.
        // peer_available starts false so we don't assume reachability until
        // we actually hear from each peer.
        let peer_last_contact: HashMap<u8, Instant> =
            peers.iter().map(|p| (p.id, Instant::now())).collect();
        let peer_available: HashMap<u8, bool> = peers.iter().map(|p| (p.id, false)).collect();

        let bind_host = crate::config::config().bind_host.as_str();
        let socket = bind_tuned_udp_socket(bind_host, self_node.raft_port);
        let wal = Wal::new(self_id).expect("failed to open replicated WAL");

        // Build the IP allowlist from configured peer host strings.
        // This is resolved once at startup; dynamic DNS changes require a restart.
        let allowed_ips: Vec<IpAddr> = s2_nodes()
            .iter()
            .filter_map(|n| n.host.parse::<IpAddr>().ok())
            .collect();

        // Eagerly load CLUSTER_HMAC_KEY at startup so we panic immediately if it
        // is missing, rather than the first time a packet is sent/received.
        let _ = auth::cluster_key();

        let election = Arc::new(LeaderElection {
            self_id,
            peers,
            socket,
            state: Mutex::new(RaftState {
                term: 0,
                role: Role::Follower,
                voted_for: None,
                leader_id: None,
                votes: HashSet::new(),
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                commit_index: 0,
                last_applied: 0,
                last_heartbeat: Instant::now(),
                election_timeout: random_timeout(),
                last_peer_contact: Instant::now(),
                peer_last_contact,
                peer_available,
                peer_last_ack: HashMap::new(),
                leader_since_term: 0,
            }),
            wal: Mutex::new(wal),
            is_leader_flag: AtomicBool::new(false),
            allowed_ips,
            started_at: Instant::now(),
            monitoring: monitoringClient::new(),
        });

        println!(
            "[role] {} started as FOLLOWER (waiting for leader)",
            node_name(self_id)
        );
        if !election.monitoring.is_configured() && require_monitoring_for_single_node_leader() {
            println!(
                "[monitoring] no monitoring_HOST configured — single-node self-promotion is disabled \
                 (REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER=true); set monitoring_HOST or set that flag \
                 to false to restore the legacy blind-timeout behavior"
            );
        }

        let recv_handle = Arc::clone(&election);
        thread::spawn(move || recv_handle.recv_loop());

        let tick_handle = Arc::clone(&election);
        thread::spawn(move || tick_handle.tick_loop());

        let monitoring_handle = Arc::clone(&election);
        thread::spawn(move || monitoring_handle.monitoring_loop());

        let publish_handle = Arc::clone(&election);
        thread::spawn(move || publish_handle.result_publisher_loop(result_pub, replay_rx));

        election
    }

    /// Watches `last_applied` and streams each newly-committed entry's result
    /// to S3 as soon as it commits, independent of which (if any)
    /// `propose_batch()` call originally proposed it. This is what fixes the
    /// bug where `propose_batch()`'s bounded wait timing out used to mean the
    /// result for that specific batch was never sent to S3, even though the
    /// entry was already correctly committed in the WAL.
    ///
    /// Only publishes while this node holds leadership. On losing leadership
    /// the watermark fast-forwards to the current `last_applied` (not reset
    /// to 0), so a later re-election doesn't republish old history it
    /// already sent in an earlier tenure.
    ///
    /// Note: `last_published` starts at 0 for a freshly started process. If
    /// this node's on-disk WAL already contains committed entries from a
    /// previous run the first time it becomes leader in this process's
    /// lifetime, those get republished once. This is harmless — order-receiver
    /// already deduplicates by `order_id` — and is accepted as simpler than
    /// tracking a persisted publish watermark across restarts, which this
    /// benchmark-proof scope doesn't need.
    /// Signs and offers one committed entry's command to the result
    /// channel, retrying while the error is retryable. Used for both live
    /// commits and replay traffic so the two are indistinguishable on the
    /// wire (order-receiver dedups either by order_id).
    fn offer_result(&self, result_pub: &AeronPublication, idle: &mut BusySpinIdleStrategy, command: &ReplicatedCommand) {
        let Ok(bytes) = bincode::serialize(command) else { return };
        let frame = auth::sign(&bytes);
        loop {
            match result_pub.offer(&frame) {
                Ok(_) => break,
                Err(e) if e.is_retryable() => {
                    idle.idle(0);
                    continue;
                }
                Err(e) => {
                    eprintln!("[S2-{}] result publish error: {e}", self.self_id);
                    break;
                }
            }
        }
    }

    fn result_publisher_loop(&self, result_pub: AeronPublication, replay_rx: Receiver<(u64, u64)>) {
        let mut idle = BusySpinIdleStrategy::default();
        let mut last_published: u64 = 0;
        loop {
            if !self.is_leader() {
                let last_applied = self.state.lock().unwrap().last_applied;
                last_published = last_applied;
                // Requests queued while we weren't leader are stale — a
                // follower must never publish on the result channel.
                while replay_rx.try_recv().is_ok() {}
                thread::sleep(Duration::from_millis(5));
                continue;
            }

            // Serve one pending replay request (bounded by the WAL scan's
            // own results) before resuming live publishing, so a S3 replay
            // request doesn't wait behind unbounded live traffic.
            if let Ok((from, to)) = replay_rx.try_recv() {
                let entries = { self.wal.lock().unwrap().entries_with_order_id_range(from, to) };
                for entry in &entries {
                    self.offer_result(&result_pub, &mut idle, &entry.command);
                }
                if verbose_raft() {
                    println!(
                        "[S2-{}] replayed {} committed order(s) (order_id {}..={}) to S3",
                        self.self_id, entries.len(), from, to
                    );
                }
                continue;
            }

            let last_applied = self.state.lock().unwrap().last_applied;
            if last_published >= last_applied {
                thread::sleep(Duration::from_millis(1));
                continue;
            }

            let next = last_published + 1;
            let entry = { self.wal.lock().unwrap().entry_at(next) };
            match entry {
                Some(entry) => {
                    self.offer_result(&result_pub, &mut idle, &entry.command);
                    if verbose_raft() {
                        println!(
                            "[order] {} LEADER committed order_id={} status={} filled={}/{}",
                            node_name(self.self_id), entry.command.order_id,
                            entry.command.status, entry.command.filled_qty,
                            entry.command.qty,
                        );
                    }
                    last_published = next;
                }
                None => {
                    // Not yet visible in this thread's WAL snapshot (race
                    // with the writer) - retry shortly rather than skipping.
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader_flag.load(Ordering::Relaxed)
    }

    pub fn current_leader_id(&self) -> Option<u8> {
        self.state.lock().unwrap().leader_id
    }

    pub fn current_term(&self) -> u64 {
        self.state.lock().unwrap().term
    }

    /// Role line including "not available" for silent peers.
    pub fn role_summary(&self) -> String {
        let st = self.state.lock().unwrap();
        let leader_id = if st.role == Role::Leader {
            Some(self.self_id)
        } else {
            st.leader_id
        };
        format_role_summary(leader_id, &self.unavailable_peer_ids(&st))
    }

    fn unavailable_peer_ids(&self, st: &RaftState) -> Vec<u8> {
        let silent = Duration::from_millis(peer_silent_ms());
        self.peers
            .iter()
            .filter(|p| {
                let last = st
                    .peer_last_contact
                    .get(&p.id)
                    .copied()
                    .unwrap_or_else(Instant::now);
                last.elapsed() >= silent
                    || !st.peer_available.get(&p.id).copied().unwrap_or(true)
            })
            .map(|p| p.id)
            .collect()
    }

    fn print_roles(&self, leader_id: Option<u8>) {
        let st = self.state.lock().unwrap();
        println!(
            "[role] {}",
            format_role_summary(leader_id, &self.unavailable_peer_ids(&st))
        );
    }

    pub fn propose_command(&self, command: ReplicatedCommand) -> Option<ReplicatedCommand> {
        let term = self.current_term();
        if !self.is_leader() {
            return None;
        }

        let entry = {
            let mut wal = self.wal.lock().unwrap();
            wal.append_leader_entry(term, command).ok()?
        };

        {
            let mut st = self.state.lock().unwrap();
            st.match_index.insert(self.self_id, entry.index);
        }

        self.replicate_to_peers();
        self.try_advance_commit();

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(1500) {
            {
                let st = self.state.lock().unwrap();
                // Only abort if we lost leadership entirely.
                // A term bump while staying leader (log-dominance protection) is
                // fine — the entry is still committed under our tenure.
                if st.role != Role::Leader {
                    return None;
                }
                if st.last_applied >= entry.index {
                    let wal = self.wal.lock().unwrap();
                    return wal.entry_at(entry.index).map(|committed| committed.command);
                }
            }
            self.replicate_to_peers();
            self.try_advance_commit();
            thread::sleep(Duration::from_millis(20));
        }
        None
    }

    pub fn propose_batch(&self, commands: Vec<ReplicatedCommand>) -> Vec<ReplicatedCommand> {
        if commands.is_empty() {
            return Vec::new();
        }
        let term = self.current_term();
        if !self.is_leader() {
            return Vec::new();
        }

        let entries = {
            let mut wal = self.wal.lock().unwrap();
            match wal.append_leader_batch(term, commands) {
                Ok(e) => e,
                Err(_) => return Vec::new(),
            }
        };

        if entries.is_empty() {
            return Vec::new();
        }

        let max_index = entries.last().unwrap().index;

        {
            let mut st = self.state.lock().unwrap();
            st.match_index.insert(self.self_id, max_index);
        }

        self.replicate_to_peers();
        self.try_advance_commit();

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(1500) {
            // Always drive commit + apply so single-node (all peers down) works.
            self.try_advance_commit();
            self.apply_committed_entries();
            {
                let st = self.state.lock().unwrap();
                if st.role != Role::Leader {
                    return Vec::new();
                }
                if st.last_applied >= max_index {
                    let wal = self.wal.lock().unwrap();
                    return entries
                        .iter()
                        .filter_map(|e| wal.entry_at(e.index).map(|c| c.command))
                        .collect();
                }
            }
            // Replicate to any peers that may have rejoined.
            self.replicate_to_peers();
            thread::sleep(Duration::from_millis(1));
        }
        Vec::new()
    }

    fn send(&self, peer: &S2Node, msg: &Message) {
        if let Ok(buf) = bincode::serialize(msg) {
            // Sign every outbound Raft control message with CLUSTER_HMAC_KEY.
            // recv_loop on the peer side verifies this before handle_message().
            let frame = auth::sign(&buf);
            let _ = self.socket.send_to(&frame, (peer.host.as_str(), peer.raft_port));
        }
    }

    fn broadcast(&self, msg: &Message) {
        for peer in &self.peers {
            self.send(peer, msg);
        }
    }

    fn mark_peer_contact(&self, st: &mut RaftState, peer_id: u8) {
        if peer_id == self.self_id {
            return;
        }
        st.last_peer_contact = Instant::now();
        st.peer_last_contact.insert(peer_id, Instant::now());
        st.peer_available.insert(peer_id, true);
    }

    fn refresh_peer_availability(&self) {
        let silent = Duration::from_millis(peer_silent_ms());
        let mut st = self.state.lock().unwrap();
        let peer_ids: Vec<u8> = self.peers.iter().map(|p| p.id).collect();
        let mut changed = false;
        for peer_id in peer_ids {
            let last = st
                .peer_last_contact
                .get(&peer_id)
                .copied()
                .unwrap_or_else(Instant::now);
            let available = last.elapsed() < silent;
            let was = st.peer_available.get(&peer_id).copied().unwrap_or(true);
            if was != available {
                st.peer_available.insert(peer_id, available);
                changed = true;
            }
        }
        if changed {
            let leader_id = if st.role == Role::Leader {
                Some(self.self_id)
            } else {
                st.leader_id
            };
            let line = format_role_summary(leader_id, &self.unavailable_peer_ids(&st));
            drop(st);
            println!("[role] {line}");
        }
    }

    fn peer_is_available(&self, st: &RaftState, peer_id: u8) -> bool {
        let silent = Duration::from_millis(peer_silent_ms());
        let last = st
            .peer_last_contact
            .get(&peer_id)
            .copied()
            .unwrap_or_else(Instant::now);
        last.elapsed() < silent && st.peer_available.get(&peer_id).copied().unwrap_or(true)
    }

    fn alive_node_count(&self, st: &RaftState) -> usize {
        1 + self
            .peers
            .iter()
            .filter(|p| self.peer_is_available(st, p.id))
            .count()
    }

    fn peers_unreachable(&self, st: &RaftState) -> bool {
        if !allow_single_node_leader() {
            return false;
        }
        // Enforce a startup grace period equal to peer_silent_ms before allowing
        // single-node self-election. This prevents all three nodes from treating
        // each other as absent at startup and simultaneously self-electing.
        let grace = Duration::from_millis(peer_silent_ms());
        if self.started_at.elapsed() < grace {
            return false;
        }
        if self.alive_node_count(st) != 1 {
            return false;
        }
        if !require_monitoring_for_single_node_leader() {
            // Legacy blind-timeout path (opt-out flag, local demo only) — a local
            // timeout alone is treated as sufficient, exactly as before this change.
            return true;
        }
        // A local timeout alone is never sufficient: this node cannot tell from the
        // inside whether both peers are genuinely down or whether it's the one that
        // got partitioned while its peers formed their own quorum. Promotion requires
        // the independent monitoring's corroboration (see `monitoring_loop()`). No monitoring
        // reachable is treated identically to "peers confirmed still up" — uncertainty
        // always resolves to staying passive, never to promoting.
        self.monitoring.cached_verdict() == CachedVerdict::SafeToPromote
    }

    /// Background thread: while this node looks locally isolated (per
    /// `peers_unreachable`'s own timeout check), periodically asks the independent
    /// monitoring to corroborate before caching a verdict `peers_unreachable()` can act
    /// on. Runs on its own cadence, independent of `tick_loop`'s 50ms cycle, and never
    /// performs I/O while holding `state`'s lock — a monitoring round-trip can take up to
    /// `monitoring_TIMEOUT_MS`, which would otherwise stall `recv_loop` and all consensus.
    fn monitoring_loop(&self) {
        loop {
            thread::sleep(Duration::from_millis(250));

            if !allow_single_node_leader() || !require_monitoring_for_single_node_leader() {
                continue;
            }

            let isolated = {
                let st = self.state.lock().unwrap();
                let grace = Duration::from_millis(peer_silent_ms());
                self.started_at.elapsed() >= grace && self.alive_node_count(&st) == 1
            };

            if !isolated {
                self.monitoring.reset_if_not_isolated();
                continue;
            }

            if !self.monitoring.due_for_attempt() {
                continue;
            }

            let term = self.current_term();
            let (outcome, changed) = self.monitoring.attempt_corroboration(self.self_id, term);
            if changed {
                let name = node_name(self.self_id);
                match outcome {
                    CorroborationOutcome::SafeToPromote => println!(
                        "[monitoring] corroboration confirmed both peers unreachable — {name} eligible to self-promote"
                    ),
                    CorroborationOutcome::DeniedBymonitoring => println!(
                        "[monitoring] corroboration denied: monitoring reports a peer still reachable — {name} staying passive"
                    ),
                    CorroborationOutcome::monitoringUnreachable => println!(
                        "[monitoring] monitoring unreachable after {}ms — {name} staying passive",
                        crate::config::monitoring_timeout_ms()
                    ),
                    CorroborationOutcome::NotConfigured => println!(
                        "[monitoring] no monitoring configured — {name} staying passive (REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER=true)"
                    ),
                }
            }
        }
    }

    /// Majority needed to commit / win an election.
    /// Always uses the full cluster size UNLESS all peers have been confirmed
    /// unreachable for at least peer_silent_ms (single-node fallback).
    fn quorum_for(&self, st: &RaftState) -> usize {
        if self.peers_unreachable(st) {
            // Every peer is silent — allow self-promotion with quorum of 1.
            1
        } else {
            // Standard Raft majority of the full cluster.
            (self.peers.len() + 1) / 2 + 1
        }
    }

    /// Returns true if this leader has received successful AppendAck from a
    /// quorum of followers within the last 4 heartbeat intervals.
    /// When true, the leader's authority is still fresh and it should NOT be
    /// displaced by a RequestVote from a stale/reconnecting node.
    fn has_quorum_lease(&self, st: &RaftState) -> bool {
        let lease_window = Duration::from_millis(heartbeat_interval_ms() * 4);
        let quorum_needed = (self.peers.len() + 1) / 2 + 1; // majority of full cluster
        // Count self + peers that acked within the lease window.
        let recent_acks = 1 + self
            .peers
            .iter()
            .filter(|p| {
                st.peer_last_ack
                    .get(&p.id)
                    .map(|t| t.elapsed() < lease_window)
                    .unwrap_or(false)
            })
            .count();
        recent_acks >= quorum_needed
    }

    fn become_leader_locked(&self, st: &mut RaftState) {
        st.role = Role::Leader;
        st.leader_id = Some(self.self_id);
        let next_idx = {
            let wal = self.wal.lock().unwrap();
            wal.last_index() + 1
        };
        st.next_index = self
            .peers
            .iter()
            .map(|peer| (peer.id, next_idx))
            .collect::<HashMap<_, _>>();
        st.match_index.clear();
        st.match_index.insert(self.self_id, next_idx.saturating_sub(1));
        st.last_heartbeat = Instant::now();
        // Record the term at which this leadership tenure started.
        // When a reconnecting peer bumps our term (but we stay leader via log
        // dominance), leader_since_term stays fixed so we can still commit
        // entries created during the original term.
        st.leader_since_term = st.term;
        self.is_leader_flag.store(true, Ordering::Relaxed);
        if self.peers_unreachable(st) {
            let mode = if !require_monitoring_for_single_node_leader() {
                "legacy blind timeout"
            } else {
                "monitoring-corroborated"
            };
            println!(
                "[role] {} is LEADER (single-node: other machines unreachable, {mode})",
                node_name(self.self_id)
            );
        } else {
            println!(
                "[role] {}",
                format_role_summary(Some(self.self_id), &self.unavailable_peer_ids(st))
            );
        }
    }

    fn replicate_to_peers(&self) {
        let (term, leader_commit, leader_role, next_map) = {
            let st = self.state.lock().unwrap();
            (
                st.term,
                st.commit_index,
                st.role,
                st.next_index.iter().map(|(k, v)| (*k, *v)).collect::<HashMap<_, _>>(),
            )
        };
        if leader_role != Role::Leader {
            return;
        }

        let wal = self.wal.lock().unwrap();
        let leader_last = wal.last_index();

        for peer in &self.peers {
            let next_idx = next_map.get(&peer.id).copied().unwrap_or(leader_last + 1);
            let prev_log_index = next_idx.saturating_sub(1);
            let prev_log_term = wal.get_term_at(prev_log_index).unwrap_or(0);
            let entries = if next_idx <= leader_last {
                entries_within_budget(wal.entries_from(next_idx), APPEND_BATCH_BYTE_BUDGET)
            } else {
                Vec::new()
            };

            if !entries.is_empty() && verbose_raft() {
                println!(
                    "[S2-{}] replicate -> S2-{} next_index={} batch={} leader_last={}",
                    self.self_id,
                    peer.id,
                    next_idx,
                    entries.len(),
                    leader_last
                );
            }

            self.send(
                peer,
                &Message::AppendEntries {
                    term,
                    leader_id: self.self_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                },
            );
        }
    }

    fn try_advance_commit(&self) {
        let (current_term, leader_since, current_commit, needed, match_map) = {
            let st = self.state.lock().unwrap();
            (
                st.term,
                st.leader_since_term,
                st.commit_index,
                self.quorum_for(&st),
                st.match_index.clone(),
            )
        };
        let wal = self.wal.lock().unwrap();
        let last_index = wal.last_index();

        let mut highest = current_commit;
        for idx in (current_commit + 1)..=last_index {
            let mut replicated = 1;
            for peer in &self.peers {
                if match_map.get(&peer.id).copied().unwrap_or(0) >= idx {
                    replicated += 1;
                }
            }

            if replicated >= needed {
                let entry_term = wal.get_term_at(idx).unwrap_or(0);
                if entry_term >= leader_since && entry_term <= current_term {
                    highest = idx;
                }
            } else {
                break;
            }
        }
        drop(wal);

        if highest > current_commit {
            let mut st = self.state.lock().unwrap();
            st.commit_index = highest;
            drop(st);
        }
        self.apply_committed_entries();
    }

    fn apply_committed_entries(&self) {
        let (next_to_apply, commit_index) = {
            let st = self.state.lock().unwrap();
            (st.last_applied + 1, st.commit_index)
        };
        if next_to_apply > commit_index {
            return;
        }

        let mut highest_applied = next_to_apply - 1;
        let wal = self.wal.lock().unwrap();
        for idx in next_to_apply..=commit_index {
            if wal.entry_at(idx).is_some() {
                highest_applied = idx;
            } else {
                break;
            }
        }
        drop(wal);

        if highest_applied >= next_to_apply {
            let mut st = self.state.lock().unwrap();
            if st.last_applied < highest_applied {
                st.last_applied = highest_applied;
            }
        }
    }

    fn tick_loop(&self) {
        loop {
            thread::sleep(Duration::from_millis(50));
            self.refresh_peer_availability();

            enum Action {
                None,
                Replicate,
                StartElection(u64),
                BecameLeader,
            }

            let action = {
                let mut st = self.state.lock().unwrap();
                match st.role {
                    Role::Leader => Action::Replicate,
                    Role::Candidate => {
                        if st.last_heartbeat.elapsed() >= st.election_timeout {
                            let needed = self.quorum_for(&st);
                            if st.votes.len() >= needed {
                                self.become_leader_locked(&mut st);
                                Action::BecameLeader
                            } else if self.peers_unreachable(&st) {
                                // Peers down long enough → win with self vote only.
                                self.become_leader_locked(&mut st);
                                Action::BecameLeader
                            } else {
                                // Restart election (peers alive but not enough votes yet).
                                st.term += 1;
                                st.voted_for = Some(self.self_id);
                                st.votes = HashSet::from([self.self_id]);
                                st.leader_id = None;
                                st.last_heartbeat = Instant::now();
                                st.election_timeout = random_timeout();
                                if verbose_raft() {
                                    println!(
                                        "[S2-{}] [term {}] election retry",
                                        self.self_id, st.term
                                    );
                                }
                                Action::StartElection(st.term)
                            }
                        } else {
                            Action::None
                        }
                    }
                    Role::Follower => {
                        if st.last_heartbeat.elapsed() >= st.election_timeout {
                            st.term += 1;
                            st.role = Role::Candidate;
                            st.voted_for = Some(self.self_id);
                            st.votes = HashSet::from([self.self_id]);
                            st.leader_id = None;
                            st.last_heartbeat = Instant::now();
                            st.election_timeout = random_timeout();
                            if verbose_raft() {
                                println!(
                                    "[S2-{}] [term {}] election timeout — starting election",
                                    self.self_id, st.term
                                );
                            }
                            Action::StartElection(st.term)
                        } else {
                            Action::None
                        }
                    }
                }
            };

            match action {
                Action::Replicate => self.replicate_to_peers(),
                Action::StartElection(term) => {
                    let (last_log_index, last_log_term) = {
                        let wal = self.wal.lock().unwrap();
                        (wal.last_index(), wal.last_term())
                    };
                    self.broadcast(&Message::RequestVote {
                        term,
                        candidate_id: self.self_id,
                        last_log_index,
                        last_log_term,
                    });
                    // If already alone, next candidate timeout will promote to leader.
                }
                Action::BecameLeader => {
                    self.replicate_to_peers();
                    self.try_advance_commit();
                }
                Action::None => {}
            }

            if self.is_leader() {
                thread::sleep(Duration::from_millis(
                    heartbeat_interval_ms().saturating_sub(50),
                ));
            }
        }
    }

    // ---- inbound control messages ----

    fn recv_loop(&self) {
        let mut buf = [0u8; RECV_BUF_SIZE];
        loop {
            let (n, src) = match self.socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(_) => continue,
            };

            // ── Source-IP allowlist ────────────────────────────────────────
            // Drop packets from any IP that is not a configured cluster node.
            // This is a first-pass filter; HMAC is the cryptographic guarantee.
            if !self.allowed_ips.contains(&src.ip()) {
                if verbose_raft() {
                    eprintln!("[S2-{}] dropping Raft packet from unknown src {src}", self.self_id);
                }
                continue;
            }

            // ── HMAC verification ────────────────────────────────────────
            // Reject any Raft message that lacks a valid CLUSTER_HMAC_KEY signature.
            // Without this, any host can forge RequestVote to disrupt leadership.
            let payload = match auth::verify(&buf[..n]) {
                Some(p) => p,
                None => {
                    eprintln!(
                        "[S2-{}] dropping Raft packet from {src}: HMAC failure ({n} bytes)",
                        self.self_id
                    );
                    continue;
                }
            };

            let msg: Message = match bincode::deserialize(payload) {
                Ok(m) => m,
                Err(err) => {
                    if verbose_raft() {
                        eprintln!(
                            "[S2-{}] dropped raft packet ({} bytes): {err}",
                            self.self_id, n
                        );
                    }
                    continue;
                }
            };
            self.handle_message(msg);
        }
    }

    fn handle_message(&self, msg: Message) {
        let mut st = self.state.lock().unwrap();

        let peer_from_msg = match &msg {
            Message::RequestVote { candidate_id, .. } => Some(*candidate_id),
            Message::VoteGranted { voter_id, .. } => Some(*voter_id),
            Message::AppendEntries { leader_id, .. } => Some(*leader_id),
            Message::AppendAck { follower_id, .. } => Some(*follower_id),
        };
        if let Some(peer_id) = peer_from_msg {
            self.mark_peer_contact(&mut st, peer_id);
        }

        let incoming_term = match &msg {
            Message::RequestVote { term, .. } => *term,
            Message::VoteGranted { term, .. } => *term,
            Message::AppendEntries { term, .. } => *term,
            Message::AppendAck { term, .. } => *term,
        };

        // Term fencing: anyone with a higher term is more current than us.
        if incoming_term > st.term {
            // A leader should only step down on a RequestVote if the challenger
            // actually has a more up-to-date log.  A node that was offline and
            // accumulated a high term through repeated failed elections has a
            // STALE log — it cannot have committed anything the leader hasn't.
            // Letting it displace the current leader causes unnecessary churn.
            //
            // Protections (either is enough to stay leader):
            //  1. Quorum lease  — majority of peers acked within last 4 heartbeats.
            //  2. Log dominance — our log is at least as current as the candidate's.
            let stay_as_leader = st.role == Role::Leader && {
                match &msg {
                    Message::RequestVote {
                        last_log_index,
                        last_log_term,
                        ..
                    } => {
                        // Check log freshness (Raft §5.4.1 comparison).
                        let (my_last_index, my_last_term) = {
                            let wal = self.wal.lock().unwrap();
                            (wal.last_index(), wal.last_term())
                        };
                        let candidate_log_is_current = *last_log_term > my_last_term
                            || (*last_log_term == my_last_term
                                && *last_log_index >= my_last_index);
                        // Stay if our log is better OR we still hold quorum.
                        !candidate_log_is_current || self.has_quorum_lease(&st)
                    }
                    // AppendEntries from a higher term = another node is already
                    // a valid leader → always step down.
                    _ => false,
                }
            };

            st.term = incoming_term; // always adopt the higher term

            if stay_as_leader {
                // Keep leading; claim voted_for so we don't accidentally
                // grant a vote to the stale challenger in this term.
                st.voted_for = Some(self.self_id);
                if verbose_raft() {
                    println!(
                        "[S2-{}] ignored stale RequestVote — staying LEADER at term {}",
                        self.self_id, st.term
                    );
                }
            } else {
                let was_leader = st.role == Role::Leader;
                st.role = Role::Follower;
                st.voted_for = None;
                st.votes.clear();
                st.next_index.clear();
                st.match_index.clear();
                st.last_heartbeat = Instant::now();
                st.election_timeout = random_timeout();
                if was_leader {
                    self.is_leader_flag.store(false, Ordering::Relaxed);
                    drop(st);
                    self.print_roles(None);
                    st = self.state.lock().unwrap();
                }
            }
        }

        match msg {
            Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                let (my_last_index, my_last_term) = {
                    let wal = self.wal.lock().unwrap();
                    (wal.last_index(), wal.last_term())
                };
                // Raft §5.4.1: vote only if candidate log is at least as up-to-date.
                let log_ok = last_log_term > my_last_term
                    || (last_log_term == my_last_term && last_log_index >= my_last_index);
                let can_vote = term >= st.term
                    && (st.voted_for.is_none() || st.voted_for == Some(candidate_id))
                    && log_ok;
                if can_vote {
                    st.voted_for = Some(candidate_id);
                    st.term = term;
                    st.last_heartbeat = Instant::now();
                    st.election_timeout = random_timeout();
                    drop(st);
                    if let Some(candidate) = find_node(candidate_id) {
                        if verbose_raft() {
                            println!(
                                "[S2-{}] granted vote to S2-{} (candidate log {}/{} vs local {}/{})",
                                self.self_id,
                                candidate_id,
                                last_log_index,
                                last_log_term,
                                my_last_index,
                                my_last_term
                            );
                        }
                        self.send(
                            &candidate,
                            &Message::VoteGranted {
                                term,
                                voter_id: self.self_id,
                            },
                        );
                    }
                } else if !log_ok && verbose_raft() {
                    println!(
                        "[S2-{}] denied vote to S2-{} — stale log (candidate {}/{} vs local {}/{})",
                        self.self_id,
                        candidate_id,
                        last_log_index,
                        last_log_term,
                        my_last_index,
                        my_last_term
                    );
                }
            }
            Message::VoteGranted { term, voter_id } => {
                if st.role == Role::Candidate && term == st.term {
                    st.votes.insert(voter_id);
                    // Reset the heartbeat timer so we don't immediately re-trigger
                    // an election timeout while waiting to accumulate quorum.
                    st.last_heartbeat = Instant::now();
                    if st.votes.len() >= self.quorum_for(&st) {
                        self.become_leader_locked(&mut st);
                        drop(st);
                        self.replicate_to_peers();
                        self.try_advance_commit();
                    }
                }
            }
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                if term < st.term {
                    return; // stale leader, ignore
                }
                let was_leader = st.role == Role::Leader;
                let prev_leader = st.leader_id;
                st.leader_id = Some(leader_id);
                st.last_heartbeat = Instant::now();
                st.term = term;
                if st.role != Role::Follower {
                    st.role = Role::Follower;
                }
                if was_leader {
                    self.is_leader_flag.store(false, Ordering::Relaxed);
                }
                let announce = was_leader || prev_leader != Some(leader_id);
                drop(st);
                if announce {
                    self.print_roles(Some(leader_id));
                }

                let append_result = {
                    let mut wal = self.wal.lock().unwrap();
                    wal.append_entries_from_leader(prev_log_index, prev_log_term, &entries)
                };

                let mut success = false;
                let mut match_index = {
                    let wal = self.wal.lock().unwrap();
                    wal.last_index()
                };

                if let Ok(Some(last_idx)) = append_result {
                    success = true;
                    match_index = last_idx;
                    let mut st = self.state.lock().unwrap();
                    st.commit_index = st.commit_index.max(leader_commit.min(last_idx));
                }
                self.apply_committed_entries();

                if let Some(leader) = find_node(leader_id) {
                    self.send(
                        &leader,
                        &Message::AppendAck {
                            term,
                            follower_id: self.self_id,
                            success,
                            match_index,
                        },
                    );
                }
            }
            Message::AppendAck {
                term,
                follower_id,
                success,
                match_index,
            } => {
                if st.role != Role::Leader || term != st.term {
                    return;
                }
                if success {
                    let prev_match = st.match_index.get(&follower_id).copied().unwrap_or(0);
                    st.match_index.insert(follower_id, match_index);
                    st.next_index.insert(follower_id, match_index + 1);
                    // Record the ack time for leader lease calculation.
                    st.peer_last_ack.insert(follower_id, Instant::now());
                    if match_index > prev_match && verbose_raft() {
                        println!(
                            "[S2-{}] follower S2-{} matched through index {}",
                            self.self_id, follower_id, match_index
                        );
                    }
                    drop(st);
                    self.try_advance_commit();
                } else {
                    // Jump using follower's last index instead of stepping back one-by-one.
                    let hinted = match_index.saturating_add(1).max(1);
                    let current = st.next_index.get(&follower_id).copied().unwrap_or(hinted);
                    let next = hinted.min(current.saturating_sub(1)).max(1);
                    if verbose_raft() {
                        println!(
                            "[S2-{}] follower S2-{} reject (follower_last={}) next_index {} -> {}",
                            self.self_id, follower_id, match_index, current, next
                        );
                    }
                    st.next_index.insert(follower_id, next);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(index: u64) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            command: ReplicatedCommand {
                order_id: index,
                symbol: "BTC-USDT".to_string(),
                side: "BUY".to_string(),
                qty: 1,
                status: "FILLED".to_string(),
                filled_qty: 1,
                processed_by: "Nitin (S2-1)".to_string(),
                term: 1,
            },
        }
    }

    #[test]
    fn entries_within_budget_stops_before_exceeding_but_always_makes_progress() {
        let entries: Vec<LogEntry> = (1..=1000).map(sample_entry).collect();
        let one_entry_size = bincode::serialized_size(&entries[0]).unwrap() as usize;

        let budget = one_entry_size * 5;
        let batch = entries_within_budget(entries.clone(), budget);
        assert!(!batch.is_empty());
        assert!(
            batch.len() <= 6,
            "expected roughly 5 entries for a 5x-single-entry budget, got {}",
            batch.len()
        );

        let tiny_budget = entries_within_budget(entries, 1);
        assert_eq!(
            tiny_budget.len(),
            1,
            "must always take at least one entry so replication keeps making progress"
        );
    }
}
