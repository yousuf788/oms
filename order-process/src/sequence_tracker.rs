// Tracks S2's ingest sequence state for the S1(order-sending)->S2(order-process)
// hop: which order_ids have been seen (dedup — replacing the old per-batch
// `seen_ids` HashSet in main.rs, which only caught duplicates *within* one
// 20k-order batch, not across batches or Aeron redelivery), the highest
// contiguous prefix received (`last_contiguous`), and any gaps that need a
// REPLAY_REQUEST.
//
// Implemented as a fixed-capacity ring bitset over a window of WINDOW
// order_ids immediately ahead of the watermark, so `mark()` — called once
// per inbound order on the ingest hot path — is O(1) and allocation-free.
// `missing_ranges()` scans the outstanding gap span and is O(gap size); it's
// only called periodically by the replay-request ticker, never per order.

use std::collections::HashSet;

/// Ring bitset span: 1Mi order_ids ahead of the watermark (~128KB). Large
/// enough to absorb normal reordering/short outages without false gap
/// reports, small enough to be a non-issue at 300k orders/sec (~3.3ms of
/// buffer at full window).
const WINDOW: u64 = 1_048_576;
const WORDS: usize = (WINDOW / 64) as usize;

pub struct SequenceTracker {
    /// Highest order_id such that every order_id in [1, last_contiguous] has
    /// been seen at least once. 0 means nothing seen yet.
    last_contiguous: u64,
    highest_seen: u64,
    /// Ring bitset covering (last_contiguous, last_contiguous + WINDOW].
    /// Ring index = order_id % WINDOW.
    bits: Vec<u64>,
    /// Rare fallback for order_ids arriving further ahead than WINDOW allows
    /// (e.g. after a very long outage) — correctness backstop, not the hot
    /// path; empty in normal operation.
    overflow: HashSet<u64>,
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self::with_watermark(0)
    }

    /// Starts the tracker with `last_contiguous` already at `watermark` —
    /// used when a checkpoint is loaded at startup instead of assuming
    /// nothing has ever been seen.
    pub fn with_watermark(watermark: u64) -> Self {
        Self {
            last_contiguous: watermark,
            highest_seen: watermark,
            bits: vec![0u64; WORDS],
            overflow: HashSet::new(),
        }
    }

    /// Records `order_id` as seen. Returns true the first time it's seen
    /// (safe to process); false for a duplicate (including a replay of
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

        // Advance the watermark while the next slot is already set,
        // clearing bits behind it so the ring can be reused.
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

    pub fn highest_seen(&self) -> u64 {
        self.highest_seen
    }

    /// Contiguous missing order_id ranges between `last_contiguous + 1` and
    /// `highest_seen`, inclusive. O(gap span) — call periodically from a
    /// replay-request ticker, not per order.
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
        assert_eq!(t.highest_seen(), 1000);
        assert!(t.missing_ranges().is_empty());
    }

    #[test]
    fn duplicate_is_reported_and_does_not_advance() {
        let mut t = SequenceTracker::new();
        assert!(t.mark(1));
        assert!(!t.mark(1));
        assert_eq!(t.last_contiguous(), 1);
    }

    #[test]
    fn out_of_order_arrival_detects_gap_then_closes_it() {
        let mut t = SequenceTracker::new();
        assert!(t.mark(1));
        assert!(t.mark(2));
        assert!(t.mark(5)); // 3,4 missing
        assert_eq!(t.last_contiguous(), 2);
        assert_eq!(t.highest_seen(), 5);
        assert_eq!(t.missing_ranges(), vec![(3, 4)]);

        assert!(t.mark(3));
        assert_eq!(t.missing_ranges(), vec![(4, 4)]);
        assert!(t.mark(4));
        assert_eq!(t.last_contiguous(), 5);
        assert!(t.missing_ranges().is_empty());

        // Replaying 3 again after the gap closed is a duplicate, not new.
        assert!(!t.mark(3));
    }

    #[test]
    fn multiple_disjoint_gaps_are_all_reported() {
        let mut t = SequenceTracker::new();
        for id in [1, 2, 5, 6, 10] {
            t.mark(id);
        }
        assert_eq!(t.missing_ranges(), vec![(3, 4), (7, 9)]);
    }

    #[test]
    fn watermark_checkpoint_start_treats_prior_ids_as_already_seen() {
        let mut t = SequenceTracker::with_watermark(100);
        assert!(!t.mark(50)); // already covered by the checkpoint
        assert!(t.mark(101));
        assert_eq!(t.last_contiguous(), 101);
    }
}
