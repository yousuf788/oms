// Periodically persists order-receiver's dedup/gap watermark
// (SequenceTracker::last_contiguous) so a restart can ask order-process to
// replay everything since the last checkpoint instead of starting blind —
// either silently forgetting every order_id it had already deduplicated, or
// (worse) never learning about a gap that opened while it was down and the
// pipeline went quiet.
//
// This is NOT a full result WAL: it persists a single u64 watermark on a
// bounded interval, not order content. order-process's WAL remains the
// durable source of truth for replayed content (see wal::entries_with_order_id_range
// in order-process). A watermark checkpoint can lag the true last_contiguous
// by up to CHECKPOINT_INTERVAL — on restart this just means the receiver's
// startup replay request re-asks for a little more than strictly necessary,
// which its ordinary dedup makes harmless.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::sequence_tracker::SequenceTracker;

const CHECKPOINT_INTERVAL: Duration = Duration::from_millis(200);

fn path() -> PathBuf {
    PathBuf::from("logs").join("receiver-checkpoint.dat")
}

pub fn load() -> u64 {
    fs::read_to_string(path())
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn save(last_contiguous: u64) {
    let p = path();
    if let Some(parent) = p.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut f) = fs::File::create(&p) {
        let _ = write!(f, "{last_contiguous}");
    }
}

/// Spawns the background checkpoint-writer thread. Reads `last_contiguous`
/// under a brief lock on each tick and writes it only when changed — the
/// disk write itself happens outside the lock.
pub fn start_checkpoint_writer(tracker: Arc<Mutex<SequenceTracker>>) {
    thread::spawn(move || {
        let mut last_written = 0u64;
        loop {
            thread::sleep(CHECKPOINT_INTERVAL);
            let current = { tracker.lock().unwrap().last_contiguous() };
            if current != last_written {
                save(current);
                last_written = current;
            }
        }
    });
}
