// Tracks order-receiver's dedup/gap state for the S2(order-process)->S3
// (order-receiver) hop: which order_ids have been seen (dedup — replacing
// the old unbounded, unpersisted `seen_order_ids: HashSet` in main.rs), the
// highest contiguous prefix received (`last_contiguous`), and any gaps that
// need a REPLAY_REQUEST to the current S2 leader.
//
// Same ring-bitset design as order-process/src/sequence_tracker.rs —
// duplicated rather than shared, matching this repo's existing convention
// of each crate staying a standalone Cargo package (see the comment in
// order-monitoring/src/config.rs).

use std::collections::HashSet;

const WINDOW: u64 = 1_048_576;
const WORDS: usize = (WINDOW / 64) as usize;

pub struct SequenceTracker {
    last_contiguous: u64,
    highest_seen: u64,
    bits: Vec<u64>,
    overflow: HashSet<u64>,
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self::with_watermark(0)
    }

    /// Starts with `last_contiguous` already at `watermark` — used when a
    /// checkpoint is loaded at startup (see checkpoint.rs) instead of
    /// assuming nothing has ever been seen.
    pub fn with_watermark(watermark: u64) -> Self {
        Self {
            last_contiguous: watermark,
            highest_seen: watermark,
            bits: vec![0u64; WORDS],
            overflow: HashSet::new(),
        }
    }

    /// Records `order_id` as seen. Returns true the first time it's seen
    /// (safe to process/log); false for a duplicate (including a replay of
    /// something already folded into `last_contiguous`).
    pub fn mark(&mut self, order_id: u64) -> bool {
        if order_id == 0 || order_id <= self.last_contiguous {
            return false;
        }
        self.highest_seen = self.highest_seen.max(order_id);

        let offset = order_id - self.last_contiguous - 1;
        let is_new = if offset < WINDOW {
            let idx = (order_id % WINDOW) as usize;
            let word = idx / 64;
            let bit = 1u64 << (idx % 64);
            let was_set = self.bits[word] & bit != 0;
            self.bits[word] |= bit;
            !was_set
        } else {
            self.overflow.insert(order_id)
        };

        loop {
            let next = self.last_contiguous + 1;
            let idx = (next % WINDOW) as usize;
            let word = idx / 64;
            let bit = 1u64 << (idx % 64);
            if self.bits[word] & bit != 0 {
                self.bits[word] &= !bit;
                self.last_contiguous = next;
            } else if self.overflow.remove(&next) {
                self.last_contiguous = next;
            } else {
                break;
            }
        }

        is_new
    }

    pub fn last_contiguous(&self) -> u64 {
        self.last_contiguous
    }

    /// Contiguous missing order_id ranges between `last_contiguous + 1` and
    /// `highest_seen`, inclusive. O(gap span) — call periodically, not per
    /// order.
    pub fn missing_ranges(&self) -> Vec<(u64, u64)> {
        let mut ranges = Vec::new();
        if self.highest_seen <= self.last_contiguous {
            return ranges;
        }
        let mut start: Option<u64> = None;
        for id in (self.last_contiguous + 1)..=self.highest_seen {
            let seen = if id - self.last_contiguous - 1 < WINDOW {
                let idx = (id % WINDOW) as usize;
                self.bits[idx / 64] & (1u64 << (idx % 64)) != 0
            } else {
                self.overflow.contains(&id)
            };
            match (seen, start) {
                (false, None) => start = Some(id),
                (true, Some(s)) => {
                    ranges.push((s, id - 1));
                    start = None;
                }
                _ => {}
            }
        }
        if let Some(s) = start {
            ranges.push((s, self.highest_seen));
        }
        ranges
    }
}

impl Default for SequenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contiguous_arrivals_advance_watermark_with_no_gaps() {
        let mut t = SequenceTracker::new();
        for id in 1..=1000u64 {
            assert!(t.mark(id));
        }
        assert_eq!(t.last_contiguous(), 1000);
        assert!(t.missing_ranges().is_empty());
    }

    #[test]
    fn watermark_checkpoint_start_treats_prior_ids_as_already_seen() {
        let mut t = SequenceTracker::with_watermark(100);
        assert!(!t.mark(50));
        assert!(t.mark(101));
        assert_eq!(t.last_contiguous(), 101);
    }

    #[test]
    fn duplicate_is_reported_and_does_not_advance() {
        let mut t = SequenceTracker::new();
        assert!(t.mark(1));
        assert!(!t.mark(1));
    }

    #[test]
    fn out_of_order_arrival_detects_and_closes_gap() {
        let mut t = SequenceTracker::new();
        t.mark(1);
        t.mark(2);
        t.mark(5);
        assert_eq!(t.missing_ranges(), vec![(3, 4)]);
        t.mark(3);
        t.mark(4);
        assert_eq!(t.last_contiguous(), 5);
        assert!(t.missing_ranges().is_empty());
    }
}
