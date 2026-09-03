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

fn env_u16(key: &str, default: u16) -> u16 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn load_from_env() -> ReceiverConfig {
    let _ = dotenvy::dotenv();

    ReceiverConfig {
        bind_host: env_required("BIND_HOST"),
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

/// Optional Aeron channel-URI tuning params (`term-length`, `mtu`,
/// `so-sndbuf`, `so-rcvbuf`), appended to the result-stream subscription URI.
/// All four are unset by default, preserving today's behavior (Media Driver
/// defaults) — see order-process/src/config.rs's copy of this function for
/// the full rationale and the OS sysctl caveat for the socket-buffer params.
pub fn aeron_channel_tuning() -> String {
    let mut params = String::new();
    if let Ok(v) = env::var("AERON_TERM_LENGTH") {
        params.push_str(&format!("|term-length={v}"));
    }
    if let Ok(v) = env::var("AERON_MTU") {
        params.push_str(&format!("|mtu={v}"));
    }
    if let Ok(v) = env::var("AERON_SO_SNDBUF") {
        params.push_str(&format!("|so-sndbuf={v}"));
    }
    if let Ok(v) = env::var("AERON_SO_RCVBUF") {
        params.push_str(&format!("|so-rcvbuf={v}"));
    }
    params
}
