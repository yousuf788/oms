use std::env;
use std::sync::OnceLock;

/// One S2 (order-process) node's replay-request address. Duplicated from
/// order-process/order-monitoring's own node lists rather than shared,
/// matching this repo's existing convention of each crate staying a
/// standalone Cargo package (see the comment in
/// order-monitoring/src/config.rs).
#[derive(Clone, Debug)]
pub struct S2NodeAddr {
    #[allow(dead_code)] // kept for parity/debug output; not needed to route a broadcast
    pub id: u8,
    pub host: String,
    pub replay_port: u16,
}

#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub bind_host: String,
    pub bind_port: u16,
    pub s2_nodes: Vec<S2NodeAddr>,
}

static CONFIG: OnceLock<ReceiverConfig> = OnceLock::new();

fn env_required(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| {
        panic!("missing {key} in environment / .env — copy from .env.example")
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

fn load_from_env() -> ReceiverConfig {
    let _ = dotenvy::dotenv();

    ReceiverConfig {
        bind_host: env_or("BIND_HOST", "0.0.0.0"),
        bind_port: env_u16("S3_PORT", 8001),
        s2_nodes: vec![
            S2NodeAddr {
                id: 1,
                host: env_required("NODE1_HOST"),
                replay_port: env_u16("NODE1_REPLAY_PORT", 6201),
            },
            S2NodeAddr {
                id: 2,
                host: env_required("NODE2_HOST"),
                replay_port: env_u16("NODE2_REPLAY_PORT", 6202),
            },
            S2NodeAddr {
                id: 3,
                host: env_required("NODE3_HOST"),
                replay_port: env_u16("NODE3_REPLAY_PORT", 6203),
            },
        ],
    }
}

pub fn init_config() -> &'static ReceiverConfig {
    CONFIG.get_or_init(load_from_env)
}
