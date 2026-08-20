use crate::config::{
    election_timeout_max_ms, election_timeout_min_ms, find_node, heartbeat_interval_ms, s2_nodes,
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

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Message {
    RequestVote { term: u64, candidate_id: u8 },
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
            }),
            wal: Mutex::new(wal),
            is_leader_flag: AtomicBool::new(false),
        });

        println!(
            "[S2-{}] control channel bound on {}",
            self_id, self_node.raft_port
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

    pub fn current_term(&self) -> u64 {
        self.state.lock().unwrap().term
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

        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(1500) {
            {
                let st = self.state.lock().unwrap();
                if st.role != Role::Leader || st.term != term {
                    return None;
                }
                if st.last_applied >= entry.index {
                    let wal = self.wal.lock().unwrap();
                    return wal.entry_at(entry.index).map(|committed| committed.command);
                }
            }
            self.replicate_to_peers();
            thread::sleep(Duration::from_millis(20));
        }
        None
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

    fn quorum(&self) -> usize {
        (self.peers.len() + 1) / 2 + 1 // 2 of 3
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
            } else {
                Vec::new()
            };

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
        let (current_term, current_commit) = {
            let st = self.state.lock().unwrap();
            (st.term, st.commit_index)
        };
        let wal = self.wal.lock().unwrap();
        let last_index = wal.last_index();
        drop(wal);

        let mut highest = current_commit;
        for idx in (current_commit + 1)..=last_index {
            let mut replicated = 1;
            let st = self.state.lock().unwrap();
            for peer in &self.peers {
                if st.match_index.get(&peer.id).copied().unwrap_or(0) >= idx {
                    replicated += 1;
                }
            }
            drop(st);

            if replicated >= self.quorum() {
                let wal = self.wal.lock().unwrap();
                if wal.get_term_at(idx) == Some(current_term) {
                    highest = idx;
                }
            }
        }

        if highest > current_commit {
            let mut st = self.state.lock().unwrap();
            st.commit_index = highest;
        }
        self.apply_committed_entries();
    }

    fn apply_committed_entries(&self) {
        loop {
            let (next_to_apply, commit_index) = {
                let st = self.state.lock().unwrap();
                (st.last_applied + 1, st.commit_index)
            };
            if next_to_apply > commit_index {
                break;
            }

            let entry = {
                let wal = self.wal.lock().unwrap();
                wal.entry_at(next_to_apply)
            };
            if let Some(applied) = entry {
                println!(
                    "[S2-{}] applied index={} term={} order_id={}",
                    self.self_id, applied.index, applied.term, applied.command.order_id
                );
                let mut st = self.state.lock().unwrap();
                if st.last_applied < applied.index {
                    st.last_applied = applied.index;
                }
            } else {
                break;
            }
        }
    }

    fn tick_loop(&self) {
        loop {
            thread::sleep(Duration::from_millis(50));

            enum Action {
                None,
                Replicate,
                StartElection(u64),
            }

            let action = {
                let mut st = self.state.lock().unwrap();
                match st.role {
                    Role::Leader => Action::Replicate,
                    _ => {
                        if st.last_heartbeat.elapsed() >= st.election_timeout {
                            st.term += 1;
                            st.role = Role::Candidate;
                            st.voted_for = Some(self.self_id);
                            st.votes = HashSet::from([self.self_id]);
                            st.leader_id = None;
                            st.last_heartbeat = Instant::now();
                            st.election_timeout = random_timeout();
                            println!(
                                "[S2-{}] [term {}] election timeout — starting election",
                                self.self_id, st.term
                            );
                            Action::StartElection(st.term)
                        } else {
                            Action::None
                        }
                    }
                }
            };

            match action {
                Action::Replicate => self.replicate_to_peers(),
                Action::StartElection(term) => self.broadcast(&Message::RequestVote {
                    term,
                    candidate_id: self.self_id,
                }),
                Action::None => {}
            }

            // keep the heartbeat cadence roughly separate from the 50ms poll
            if self.is_leader() {
                thread::sleep(Duration::from_millis(
                    heartbeat_interval_ms().saturating_sub(50),
                ));
            }
        }
    }

    // ---- inbound control messages ----

    fn recv_loop(&self) {
        let mut buf = [0u8; 1024];
        loop {
            let (n, _src) = match self.socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let msg: Message = match serde_json::from_slice(&buf[..n]) {
                Ok(m) => m,
                Err(_) => continue,
            };
            self.handle_message(msg);
        }
    }

    fn handle_message(&self, msg: Message) {
        let mut st = self.state.lock().unwrap();

        let incoming_term = match &msg {
            Message::RequestVote { term, .. } => *term,
            Message::VoteGranted { term, .. } => *term,
            Message::AppendEntries { term, .. } => *term,
            Message::AppendAck { term, .. } => *term,
        };

        // Term fencing: anyone with a higher term is more current than us.
        if incoming_term > st.term {
            let was_leader = st.role == Role::Leader;
            st.term = incoming_term;
            st.role = Role::Follower;
            st.voted_for = None;
            st.votes.clear();
            st.next_index.clear();
            st.match_index.clear();
            st.last_heartbeat = Instant::now();
            st.election_timeout = random_timeout();
            if was_leader {
                self.is_leader_flag.store(false, Ordering::Relaxed);
                println!(
                    "[S2-{}] [term {}] stepping down, saw higher term",
                    self.self_id, incoming_term
                );
            }
        }

        match msg {
            Message::RequestVote { term, candidate_id } => {
                let can_vote =
                    term >= st.term && (st.voted_for.is_none() || st.voted_for == Some(candidate_id));
                if can_vote {
                    st.voted_for = Some(candidate_id);
                    st.term = term;
                    st.last_heartbeat = Instant::now();
                    st.election_timeout = random_timeout();
                    drop(st); // release lock before doing network I/O
                    if let Some(candidate) = find_node(candidate_id) {
                        println!("[S2-{}] granted vote to S2-{}", self.self_id, candidate_id);
                        self.send(&candidate, &Message::VoteGranted { term, voter_id: self.self_id });
                    }
                }
            }
            Message::VoteGranted { term, voter_id } => {
                if st.role == Role::Candidate && term == st.term {
                    st.votes.insert(voter_id);
                    if st.votes.len() >= self.quorum() {
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
                        self.is_leader_flag.store(true, Ordering::Relaxed);
                        println!(
                            "[S2-{}] [term {}] won election — becoming leader",
                            self.self_id, term
                        );
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
                st.leader_id = Some(leader_id);
                st.last_heartbeat = Instant::now();
                st.term = term;
                if st.role != Role::Follower {
                    st.role = Role::Follower;
                }
                if was_leader {
                    self.is_leader_flag.store(false, Ordering::Relaxed);
                }
                drop(st);

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
                    st.match_index.insert(follower_id, match_index);
                    st.next_index.insert(follower_id, match_index + 1);
                    drop(st);
                    self.try_advance_commit();
                } else {
                    let current = st.next_index.get(&follower_id).copied().unwrap_or(1);
                    st.next_index.insert(follower_id, current.saturating_sub(1).max(1));
                }
            }
        }
    }
}
