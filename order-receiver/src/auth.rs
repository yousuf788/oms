// auth.rs — HMAC-SHA256 message authentication for order-receiver's inbound
// channels: the Aeron result stream from order-process (previously
// unauthenticated — see main.rs) and the S2<->S3 replay-request control
// channel (replay_client.rs sends, order-process's replay_server.rs
// receives). Uses CLUSTER_HMAC_KEY, the same key order-sending and
// order-process already use for the order channel and Raft control
// messages — this is within the same cluster trust boundary, unlike
// order-monitoring's separate monitoring_HMAC_KEY.
//
// Wire format (matches every other channel in this system):
//   [ 4 bytes big-endian payload_len ][ payload bytes ][ 32 bytes HMAC-SHA256 ]
//
// Generate with: openssl rand -hex 32

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::OnceLock;

type HmacSha256 = Hmac<Sha256>;

const HMAC_TAG_LEN: usize = 32;
const LEN_PREFIX: usize = 4;

static CLUSTER_KEY: OnceLock<Vec<u8>> = OnceLock::new();

fn decode_hex_key(hex: &str, env_name: &str) -> Vec<u8> {
    if hex.len() < 32 {
        panic!(
            "{env_name} must be at least 32 hex characters (got {}). \
             Generate with: openssl rand -hex 32",
            hex.len()
        );
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&hex[i..i + 2], 16).unwrap_or_else(|_| {
                panic!("{env_name} contains non-hex character at position {i}")
            })
        })
        .collect()
}

pub fn cluster_key() -> &'static [u8] {
    CLUSTER_KEY.get_or_init(|| {
        let hex = std::env::var("CLUSTER_HMAC_KEY")
            .expect("CLUSTER_HMAC_KEY must be set (hex-encoded ≥32-byte key). Generate: openssl rand -hex 32");
        decode_hex_key(&hex, "CLUSTER_HMAC_KEY")
    })
}

pub fn sign(payload: &[u8]) -> Vec<u8> {
    let key = cluster_key();
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key length is always valid");
    mac.update(payload);
    let tag = mac.finalize().into_bytes();

    let mut frame = Vec::with_capacity(LEN_PREFIX + payload.len() + HMAC_TAG_LEN);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&tag);
    frame
}

pub fn verify(frame: &[u8]) -> Option<&[u8]> {
    if frame.len() < LEN_PREFIX + HMAC_TAG_LEN {
        return None;
    }
    let len = u32::from_be_bytes(frame[..LEN_PREFIX].try_into().ok()?) as usize;
    let expected_total = LEN_PREFIX + len + HMAC_TAG_LEN;
    if frame.len() < expected_total {
        return None;
    }
    let payload = &frame[LEN_PREFIX..LEN_PREFIX + len];
    let tag = &frame[LEN_PREFIX + len..LEN_PREFIX + len + HMAC_TAG_LEN];

    let key = cluster_key();
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key length is always valid");
    mac.update(payload);
    mac.verify_slice(tag).ok()?; // constant-time — no timing oracle
    Some(payload)
}
