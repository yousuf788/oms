use std::env;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct S2Node {
    pub id: u8,
    pub name: String,
    pub host: String,
    pub raft_port: u16,
    pub order_port: u16,
    /// Trivial liveness-probe port, used only by the monitoring service — completely
    /// separate from `raft_port` so monitoring probes never touch Raft consensus state.
    pub health_port: u16,
    /// Dedicated port for the S2->S3 replay-request control channel — a peer
    /// (order-receiver) asks this node to re-publish committed results in a
    /// range. Separate from `raft_port`/`order_port`/`health_port` for the
    /// same reason those are separate from each other.
    pub replay_port: u16,
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub nodes: Vec<S2Node>,
    pub bind_host: String,
    pub s3_host: String,
    pub s3_port: u16,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub verbose_raft: bool,
    /// When peers are silent, allow this node to win with only its own vote (lab failover).
    pub allow_single_node_leader: bool,
    pub peer_silent_ms: u64,
    /// Address of the independent monitoring service consulted before a lone node is
    /// allowed to self-promote. `None` if `monitoring_HOST` isn't set.
    pub monitoring_host: Option<String>,
    pub monitoring_port: u16,
    pub monitoring_timeout_ms: u64,
    pub monitoring_retry_interval_ms: u64,
    /// If true (default), a monitoring must corroborate before single-node self-promotion
    /// is allowed — no monitoring reachable means no promotion. If false, falls back to
    /// the legacy blind-timeout behavior (local demo only).
    pub require_monitoring_for_single_node_leader: bool,
    /// order-sending's replay listener address — where this node sends
    /// REPLAY_REQUEST when its ingest sequence tracker detects a persistent
    /// gap in incoming orders.
    pub s1_host: String,
    pub s1_replay_port: u16,
}

static CONFIG: OnceLock<ClusterConfig> = OnceLock::new();

fn env_required(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| {
        panic!("missing {key} in environment / .env — copy from .env.example or cluster.sample")
    })
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u16(key: &str, default: u16) -> u16 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default,
    }
}

fn load_from_env() -> ClusterConfig {
    let _ = dotenvy::dotenv();

    ClusterConfig {
        nodes: vec![
            S2Node {
                id: 1,
                name: env_or("NODE1_NAME", "Nitin"),
                host: env_required("NODE1_HOST"),
                raft_port: env_u16("NODE1_RAFT_PORT", 6001),
                order_port: env_u16("NODE1_ORDER_PORT", 7001),
                health_port: env_u16("NODE1_HEALTH_PORT", 6101),
                replay_port: env_u16("NODE1_REPLAY_PORT", 6201),
            },
            S2Node {
                id: 2,
                name: env_or("NODE2_NAME", "Amit"),
                host: env_required("NODE2_HOST"),
                raft_port: env_u16("NODE2_RAFT_PORT", 6002),
                order_port: env_u16("NODE2_ORDER_PORT", 7002),
                health_port: env_u16("NODE2_HEALTH_PORT", 6102),
                replay_port: env_u16("NODE2_REPLAY_PORT", 6202),
            },
            S2Node {
                id: 3,
                name: env_or("NODE3_NAME", "Yousuf"),
                host: env_required("NODE3_HOST"),
                raft_port: env_u16("NODE3_RAFT_PORT", 6003),
                order_port: env_u16("NODE3_ORDER_PORT", 7003),
                health_port: env_u16("NODE3_HEALTH_PORT", 6103),
                replay_port: env_u16("NODE3_REPLAY_PORT", 6203),
            },
        ],
        bind_host: env_or("BIND_HOST", "0.0.0.0"),
        s3_host: env_required("S3_HOST"),
        s3_port: env_u16("S3_PORT", 8001),
        heartbeat_interval_ms: env_u64("HEARTBEAT_INTERVAL_MS", 50),
        election_timeout_min_ms: env_u64("ELECTION_TIMEOUT_MIN_MS", 150),
        election_timeout_max_ms: env_u64("ELECTION_TIMEOUT_MAX_MS", 300),
        verbose_raft: env_bool("VERBOSE_RAFT", false),
        allow_single_node_leader: env_bool("ALLOW_SINGLE_NODE_LEADER", true),
        peer_silent_ms: env_u64("PEER_SILENT_MS", 2000),
        monitoring_host: env::var("monitoring_HOST").ok(),
        monitoring_port: env_u16("monitoring_PORT", 9101),
        monitoring_timeout_ms: env_u64("monitoring_TIMEOUT_MS", 1500),
        monitoring_retry_interval_ms: env_u64("monitoring_RETRY_INTERVAL_MS", 2000),
        require_monitoring_for_single_node_leader: env_bool(
            "REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER",
            true,
        ),
        s1_host: env_required("S1_HOST"),
        s1_replay_port: env_u16("S1_REPLAY_PORT", 9001),
    }
}

pub fn init_config() -> &'static ClusterConfig {
    CONFIG.get_or_init(load_from_env)
}

pub fn config() -> &'static ClusterConfig {
    CONFIG.get().expect("config not initialized; call init_config() first")
}

fn local_ipv4_addrs() -> Vec<String> {
    let mut ips = Vec::new();
    if let Ok(output) = std::process::Command::new("hostname").arg("-I").output() {
        for token in String::from_utf8_lossy(&output.stdout).split_whitespace() {
            if token.contains('.') && !token.contains(':') {
                ips.push(token.to_string());
            }
        }
    }
    // Loopback for single-machine .env.example demos — covers both the
    // empty case and "present but missing 127.0.0.1" in one check, since
    // `any()` on an empty iterator is already `false`.
    if !ips.iter().any(|ip| ip == "127.0.0.1") {
        ips.push("127.0.0.1".to_string());
    }
    ips
}

/// Resolve this replica's id:
/// 1. `NODE_ID` env if set
/// 2. else match this machine's IP to NODE1/2/3_HOST in `.env`
pub fn resolve_node_id() -> u8 {
    if let Ok(raw) = env::var("NODE_ID") {
        return raw
            .parse::<u8>()
            .unwrap_or_else(|_| panic!("NODE_ID must be 1, 2, or 3 (got {raw})"));
    }

    let cfg = config();
    let locals = local_ipv4_addrs();
    let matches: Vec<u8> = cfg
        .nodes
        .iter()
        .filter(|n| locals.iter().any(|ip| ip == &n.host))
        .map(|n| n.id)
        .collect();

    match matches.as_slice() {
        [id] => *id,
        [] => panic!(
            "NODE_ID not set and no local IP matches NODE1/2/3_HOST in .env. \
             Local IPs: {locals:?}. Set NODE_ID=1|2|3 or fix hosts in .env."
        ),
        _ => panic!(
            "NODE_ID not set and multiple nodes match local IPs {locals:?} \
             (typical for all-127.0.0.1 local demo). Pass NODE_ID=1|2|3."
        ),
    }
}

pub fn find_node(id: u8) -> Option<S2Node> {
    config().nodes.iter().find(|n| n.id == id).cloned()
}

pub fn node_name(id: u8) -> String {
    find_node(id)
        .map(|n| n.name)
        .unwrap_or_else(|| format!("S2-{id}"))
}

/// e.g. "Nitin is not available; Amit is not available; Yousuf is LEADER"
pub fn format_role_summary(leader_id: Option<u8>, unavailable: &[u8]) -> String {
    config()
        .nodes
        .iter()
        .map(|n| {
            let status = if Some(n.id) == leader_id {
                "LEADER"
            } else if unavailable.contains(&n.id) {
                "not available"
            } else {
                "FOLLOWER"
            };
            format!("{} is {}", n.name, status)
        })
        .collect::<Vec<_>>()
        .join("; ")
}

pub fn s2_nodes() -> &'static [S2Node] {
    &config().nodes
}

pub fn verbose_raft() -> bool {
    config().verbose_raft
}

pub fn allow_single_node_leader() -> bool {
    config().allow_single_node_leader
}

pub fn peer_silent_ms() -> u64 {
    config().peer_silent_ms
}

pub fn heartbeat_interval_ms() -> u64 {
    config().heartbeat_interval_ms
}

pub fn election_timeout_min_ms() -> u64 {
    config().election_timeout_min_ms
}

pub fn election_timeout_max_ms() -> u64 {
    config().election_timeout_max_ms
}

pub fn monitoring_host() -> Option<String> {
    config().monitoring_host.clone()
}

pub fn monitoring_port() -> u16 {
    config().monitoring_port
}

pub fn monitoring_timeout_ms() -> u64 {
    config().monitoring_timeout_ms
}

pub fn monitoring_retry_interval_ms() -> u64 {
    config().monitoring_retry_interval_ms
}

pub fn require_monitoring_for_single_node_leader() -> bool {
    config().require_monitoring_for_single_node_leader
}

pub fn health_port(id: u8) -> Option<u16> {
    find_node(id).map(|n| n.health_port)
}

pub fn s1_host() -> String {
    config().s1_host.clone()
}

pub fn s1_replay_port() -> u16 {
    config().s1_replay_port
}
