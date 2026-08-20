use std::env;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct ReceiverConfig {
    pub bind_host: String,
    pub bind_port: u16,
}

static CONFIG: OnceLock<ReceiverConfig> = OnceLock::new();

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
    }
}

pub fn init_config() -> &'static ReceiverConfig {
    CONFIG.get_or_init(load_from_env)
}
