use std::env;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct WatchedNodeAddr {
    pub id: u8,
    pub name: String,
    pub host: String,
    pub health_port: u16,
}

impl WatchedNodeAddr {
    /// e.g. "Amit (172.16.13.181)" — used everywhere a log line would
    /// otherwise print a bare node id.
    pub fn label(&self) -> String {
        format!("{} ({})", self.name, self.host)
    }
}

#[derive(Clone, Debug)]
pub struct WitnessConfig {
    pub bind_host: String,
    pub witness_port: u16,
    pub nodes: Vec<WatchedNodeAddr>,
    pub poll_interval_ms: u64,
    pub probe_timeout_ms: u64,
    pub probe_retries: u8,
    pub verbose: bool,
}

static CONFIG: OnceLock<WitnessConfig> = OnceLock::new();

fn env_required(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| {
        panic!("missing {key} in environment / .env — copy from .env.example")
    })
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_u16(key: &str, default: u16) -> u16 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_u8(key: &str, default: u8) -> u8 {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => default,
    }
}

fn load_from_env() -> WitnessConfig {
    let _ = dotenvy::dotenv();

    WitnessConfig {
        bind_host: env_or("BIND_HOST", "0.0.0.0"),
        witness_port: env_u16("WITNESS_PORT", 9101),
        // Mirrors order-process/.env's NODE{1,2,3}_HOST/_HEALTH_PORT — duplicated
        // rather than shared, since this repo isn't a Cargo workspace and pulling
        // in order-process's config.rs would drag its Aeron/rusteron-client native
        // build into a service that should stay minimal.
        nodes: vec![
            WatchedNodeAddr {
                id: 1,
                name: env_or("NODE1_NAME", "Nitin"),
                host: env_required("NODE1_HOST"),
                health_port: env_u16("NODE1_HEALTH_PORT", 6101),
            },
            WatchedNodeAddr {
                id: 2,
                name: env_or("NODE2_NAME", "Amit"),
                host: env_required("NODE2_HOST"),
                health_port: env_u16("NODE2_HEALTH_PORT", 6102),
            },
            WatchedNodeAddr {
                id: 3,
                name: env_or("NODE3_NAME", "Yousuf"),
                host: env_required("NODE3_HOST"),
                health_port: env_u16("NODE3_HEALTH_PORT", 6103),
            },
        ],
        poll_interval_ms: env_u64("WITNESS_POLL_INTERVAL_MS", 500),
        probe_timeout_ms: env_u64("WITNESS_PROBE_TIMEOUT_MS", 400),
        probe_retries: env_u8("WITNESS_PROBE_RETRIES", 1),
        verbose: env_bool("VERBOSE_WITNESS", false),
    }
}

pub fn init_config() -> &'static WitnessConfig {
    CONFIG.get_or_init(load_from_env)
}

pub fn config() -> &'static WitnessConfig {
    CONFIG.get().expect("config not initialized; call init_config() first")
}

/// The two nodes a given requester should be checked against — i.e. every
/// configured node except the requester itself.
pub fn other_nodes(requester_id: u8) -> Vec<WatchedNodeAddr> {
    config()
        .nodes
        .iter()
        .filter(|n| n.id != requester_id)
        .cloned()
        .collect()
}

/// Looks up a single configured node by id — used to label a requester
/// (e.g. "Amit (172.16.13.181)") in logs instead of a bare node id.
pub fn find_node(id: u8) -> Option<WatchedNodeAddr> {
    config().nodes.iter().find(|n| n.id == id).cloned()
}
