// auth.rs — HMAC-SHA256 message authentication.
//
// Two independent keys are supported here:
//
//  1. CLUSTER_HMAC_KEY — authenticates Aeron order messages (S1→S2) and all
//     Raft control messages (S2 inter-node). Must be identical on every cluster
//     node (order-sending and all order-process replicas).
//
//  2. monitoring_HMAC_KEY — authenticates corroboration messages between
//     order-process nodes and the order-monitoring service. Separate from the
//     cluster key so the monitoring never has access to the Raft signing material.
//
// Wire format (all channels):
//   [ 4 bytes big-endian payload_len ][ payload bytes ][ 32 bytes HMAC-SHA256 ]
//
// Generate keys with: openssl rand -hex 32

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::OnceLock;

type HmacSha256 = Hmac<Sha256>;

pub const HMAC_TAG_LEN: usize = 32;
pub const LEN_PREFIX: usize = 4;

static CLUSTER_KEY: OnceLock<Vec<u8>> = OnceLock::new();
static monitoring_KEY: OnceLock<Vec<u8>> = OnceLock::new();

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

pub fn monitoring_key() -> &'static [u8] {
    monitoring_KEY.get_or_init(|| {
        let hex = std::env::var("monitoring_HMAC_KEY")
            .expect("monitoring_HMAC_KEY must be set (hex-encoded ≥32-byte key). Generate: openssl rand -hex 32");
        decode_hex_key(&hex, "monitoring_HMAC_KEY")
    })
}

/// Build wire frame: `[4-byte big-endian len][payload][32-byte HMAC]`
pub fn sign_with(payload: &[u8], key: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key always valid");
    mac.update(payload);
    let tag = mac.finalize().into_bytes();

    let mut frame = Vec::with_capacity(LEN_PREFIX + payload.len() + HMAC_TAG_LEN);
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame.extend_from_slice(&tag);
    frame
}

/// Verify frame and return inner payload slice. `None` on any auth failure.
pub fn verify_with<'a>(frame: &'a [u8], key: &[u8]) -> Option<&'a [u8]> {
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

    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key always valid");
    mac.update(payload);
    mac.verify_slice(tag).ok()?; // constant-time — no timing oracle
    Some(payload)
}

/// Convenience wrappers using the cluster key.
pub fn sign(payload: &[u8]) -> Vec<u8> {
    sign_with(payload, cluster_key())
}

pub fn verify(frame: &[u8]) -> Option<&[u8]> {
    verify_with(frame, cluster_key())
}

/// Convenience wrappers using the monitoring key.
pub fn sign_monitoring(payload: &[u8]) -> Vec<u8> {
    sign_with(payload, monitoring_key())
}

pub fn verify_monitoring(frame: &[u8]) -> Option<&[u8]> {
    verify_with(frame, monitoring_key())
}
