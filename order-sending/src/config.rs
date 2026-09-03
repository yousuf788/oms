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
