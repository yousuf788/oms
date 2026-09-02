// auth.rs — HMAC-SHA256 message authentication for the witness corroboration channel.
//
// Wire format:
//   [ 4 bytes big-endian payload_len ][ payload bytes ][ 32 bytes HMAC-SHA256 ]
//
// Key source: WITNESS_HMAC_KEY env var (hex-encoded ≥32-byte key).
// Must be identical on the witness service AND all order-process nodes.
//
// Generate with: openssl rand -hex 32

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::OnceLock;

type HmacSha256 = Hmac<Sha256>;

pub const HMAC_TAG_LEN: usize = 32;
pub const LEN_PREFIX: usize = 4;

static WITNESS_KEY: OnceLock<Vec<u8>> = OnceLock::new();

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

pub fn witness_key() -> &'static [u8] {
    WITNESS_KEY.get_or_init(|| {
        let hex = std::env::var("WITNESS_HMAC_KEY")
            .expect("WITNESS_HMAC_KEY must be set (hex-encoded ≥32-byte key). Generate: openssl rand -hex 32");
        decode_hex_key(&hex, "WITNESS_HMAC_KEY")
    })
}

/// Build wire frame: `[4-byte big-endian len][payload][32-byte HMAC]`
pub fn sign(payload: &[u8]) -> Vec<u8> {
    let key = witness_key();
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

    let key = witness_key();
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC key always valid");
    mac.update(payload);
    mac.verify_slice(tag).ok()?; // constant-time — no timing oracle
    Some(payload)
}
