use std::env;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct S2Node {
    pub id: u8,
    pub host: String,
    pub raft_port: u16,
    pub order_port: u16,
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
}

static CONFIG: OnceLock<ClusterConfig> = OnceLock::new();

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

fn load_from_env() -> ClusterConfig {
    let _ = dotenvy::dotenv();

    ClusterConfig {
        nodes: vec![
            S2Node {
                id: 1,
                host: env_or("NODE1_HOST", "127.0.0.1"),
                raft_port: env_u16("NODE1_RAFT_PORT", 6001),
                order_port: env_u16("NODE1_ORDER_PORT", 7001),
            },
            S2Node {
                id: 2,
                host: env_or("NODE2_HOST", "127.0.0.1"),
                raft_port: env_u16("NODE2_RAFT_PORT", 6002),
                order_port: env_u16("NODE2_ORDER_PORT", 7002),
            },
            S2Node {
                id: 3,
                host: env_or("NODE3_HOST", "127.0.0.1"),
                raft_port: env_u16("NODE3_RAFT_PORT", 6003),
                order_port: env_u16("NODE3_ORDER_PORT", 7003),
            },
        ],
        bind_host: env_or("BIND_HOST", "0.0.0.0"),
        s3_host: env_or("S3_HOST", "127.0.0.1"),
        s3_port: env_u16("S3_PORT", 8001),
        heartbeat_interval_ms: env_u64("HEARTBEAT_INTERVAL_MS", 100),
        election_timeout_min_ms: env_u64("ELECTION_TIMEOUT_MIN_MS", 300),
        election_timeout_max_ms: env_u64("ELECTION_TIMEOUT_MAX_MS", 600),
    }
}

pub fn init_config() -> &'static ClusterConfig {
    CONFIG.get_or_init(load_from_env)
}

pub fn config() -> &'static ClusterConfig {
    CONFIG.get().expect("config not initialized; call init_config() first")
}

pub fn find_node(id: u8) -> Option<S2Node> {
    config().nodes.iter().find(|n| n.id == id).cloned()
}

pub fn s2_nodes() -> &'static [S2Node] {
    &config().nodes
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
