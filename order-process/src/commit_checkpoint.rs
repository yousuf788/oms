// Persists this node's Raft `commit_index` so a restart can bound
// `Wal::max_order_id_up_to()` — and therefore the ingest-side
// `SequenceTracker`'s startup watermark and catch-up `REPLAY_REQUEST` — to
// entries that were actually committed, instead of whatever happens to be
// sitting in the local WAL. `RaftState.commit_index`/`last_applied` are
// in-memory-only and reset to 0 on every restart; without this, a leader
// that crashes with an uncommitted tail (`propose_batch` doesn't wait for
// quorum) seeds the tracker past what's safe, and if a later legitimate
// leader truncates that tail (`wal.rs::truncate_from`), those order_ids are
// never re-requested — silent, permanent loss.
//
// Same shape as order-receiver's checkpoint.rs: a single u64 watermark on a
// bounded interval, not order content — order-process's own WAL remains the
// durable source of truth for the entries themselves. A stale watermark (up
// to CHECKPOINT_INTERVAL behind) just means the startup catch-up path
// treats a few already-committed order_ids as unconfirmed and re-requests
// them, which replay's own dedup makes harmless. The periodic writer lives
// in leader_election.rs (as `commit_checkpoint_loop`) since it needs direct
// access to `RaftState.commit_index`; this module holds only the pure
// path/load/save helpers.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

pub const CHECKPOINT_INTERVAL_MS: u64 = 200;

fn path(node_id: u8) -> PathBuf {
    let base_dir = std::env::var("ORDER_PROCESS_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("logs"));
    // Mirrors wal.rs's own per-node naming convention for consistency.
    if std::env::var("ORDER_PROCESS_DATA_DIR").is_ok() {
        base_dir.join(format!("commit-watermark-s2-{}.dat", node_id))
    } else {
        base_dir.join("commit-watermark.dat")
    }
}

/// Loads the persisted commit_index, clamped to `wal_last_index` — a
/// checkpoint can never be trusted past what's actually present in the
/// local WAL (e.g. if the WAL file was reset independently of this one).
pub fn load(node_id: u8, wal_last_index: u64) -> u64 {
    let raw: u64 = fs::read_to_string(path(node_id))
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    raw.min(wal_last_index)
}

pub fn save(node_id: u8, commit_index: u64) {
    let p = path(node_id);
    if let Some(parent) = p.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut f) = fs::File::create(&p) {
        let _ = write!(f, "{commit_index}");
        let _ = f.sync_data();
    }
}
