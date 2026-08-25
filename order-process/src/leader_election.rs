use crate::config::{
    allow_single_node_leader, election_timeout_max_ms, election_timeout_min_ms, find_node,
    format_role_summary, heartbeat_interval_ms, node_name, peer_silent_ms, s2_nodes, verbose_raft,
    S2Node,
};
use crate::wal::{LogEntry, ReplicatedCommand, Wal};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Keep AppendEntries UDP packets small so catch-up never exceeds MTU/recv buffer.
const MAX_ENTRIES_PER_APPEND: usize = 32;
const RECV_BUF_SIZE: usize = 65_535;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
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
    /// When this node started — used to enforce a startup grace period before
    /// allowing single-node self-election. Prevents all nodes self-electing
    /// simultaneously on startup before peers have had a chance to respond.
    started_at: Instant,
}

impl LeaderElection {
    /// Binds the control-channel socket and spawns the background
    /// recv + election/heartbeat ticker threads. Returns immediately;
    /// call `.is_leader()` from anywhere to check current status.
    pub fn start(self_id: u8) -> Arc<Self> {
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
        let socket = UdpSocket::bind((bind_host, self_node.raft_port))
            .expect("failed to bind control channel");
        let wal = Wal::new(self_id).expect("failed to open replicated WAL");

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
            started_at: Instant::now(),
        });

        println!(
            "[role] {} started as FOLLOWER (waiting for leader)",
            node_name(self_id)
        );

        let recv_handle = Arc::clone(&election);
        thread::spawn(move || recv_handle.recv_loop());

        let tick_handle = Arc::clone(&election);
        thread::spawn(move || tick_handle.tick_loop());

        election
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
        if let Ok(buf) = serde_json::to_vec(msg) {
            let _ = self.socket.send_to(&buf, (peer.host.as_str(), peer.raft_port));
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
        self.alive_node_count(st) == 1
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
            println!(
                "[role] {} is LEADER (single-node: other machines unreachable)",
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
                wal.entries_from(next_idx)
                    .into_iter()
                    .take(MAX_ENTRIES_PER_APPEND)
                    .collect()
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
            let (n, _src) = match self.socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let msg: Message = match serde_json::from_slice(&buf[..n]) {
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
