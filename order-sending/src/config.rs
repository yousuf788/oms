use std::env;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct S2Node {
    pub host: String,
    pub order_port: u16,
}

#[derive(Clone, Debug)]
pub struct SenderConfig {
    pub bind_host: String,
    pub bind_port: u16,
    pub nodes: Vec<S2Node>,
}

static CONFIG: OnceLock<SenderConfig> = OnceLock::new();

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

fn load_from_env() -> SenderConfig {
    let _ = dotenvy::dotenv();

    SenderConfig {
        bind_host: env_required("BIND_HOST"),
        bind_port: env_u16("SENDER_BIND_PORT", 9001),
        nodes: vec![
            S2Node {
                host: env_required("NODE1_HOST"),
                order_port: env_u16("NODE1_ORDER_PORT", 7001),
            },
            S2Node {
                host: env_required("NODE2_HOST"),
                order_port: env_u16("NODE2_ORDER_PORT", 7002),
            },
            S2Node {
                host: env_required("NODE3_HOST"),
                order_port: env_u16("NODE3_ORDER_PORT", 7003),
            },
        ],
    }
}

pub fn init_config() -> &'static SenderConfig {
    CONFIG.get_or_init(load_from_env)
}

/// Optional Aeron channel-URI tuning params (`term-length`, `mtu`,
/// `so-sndbuf`, `so-rcvbuf`), appended to every publication URI this crate
/// builds. All four are unset by default, preserving today's behavior
/// (Media Driver defaults) — see order-process/src/config.rs's copy of this
/// function for the full rationale and the OS sysctl caveat for the
/// socket-buffer params.
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
