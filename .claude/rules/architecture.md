# Rule: Architectural Invariants & Cluster Constraints

This rule specifies system-wide architectural invariants for the 3-tier distributed Order Management System (OMS).

---

## 1. 3-Tier Pipeline Invariants

1. **S1 (`order-sending`)**:
   - Generates orders and broadcasts them to all S2 cluster nodes using unicast Aeron publications.
   - Must never communicate directly with S3 (`order-receiver`) or `order-monitoring`.
2. **S2 (`order-process`)**:
   - 3-replica Raft consensus cluster (`NODE1`, `NODE2`, `NODE3`).
   - Only the active **Raft Leader** processes inbound orders, appends commands to its WAL, replicates to followers, and publishes results to S3.
   - **Raft Followers** receive orders and replicate log entries, but MUST STAY SILENT on result channels (no publishing to S3).
3. **S3 (`order-receiver`)**:
   - Passive result sink. Subscribes to result stream `2001` and logs committed orders to disk.
   - Deduplicates incoming entries by `order_id` to handle leader failover duplicates safely.

---

## 2. Write-Ahead Log (WAL) Invariants

- **$O(1)$ Append-Only**: WAL records are appended incrementally to `data/wal-s2-<node_id>.log`. Full file reads, file rewrites, or truncations during normal log append operations are strictly forbidden.
- **Log Equivalence**: Raft consensus guarantees log consistency across replicas via batched `AppendEntries` RPCs.

---

## 3. Sequence Identity, Gap Detection & Replay (S1<->S2, S2<->S3)

- **Delivery model**: at-least-once with idempotent per-hop deduplication (`order_id`-based) — never claim exactly-once. Every hop can experience redelivery (Aeron, or the replay protocol below); every hop dedups.
- **Sequence identity**: `order_id` is assigned by `order-sending` from a counter that resumes from its own WAL (`logs/orders-sent.wal`) at startup — it must never silently reset to 1 while that WAL has prior entries.
- **Per-service durable state**: `order-sending`'s WAL (full order content, replay source), `order-process`'s existing Raft WAL (also serves S2<->S3 replay by `order_id`), `order-receiver`'s checkpoint file (a watermark only, not full content). None of the three implement retention/truncation/snapshotting — this is a known, deferred gap, not an oversight to "fix" opportunistically.
- **Gap detection**: each receiving side (`order-process` ingest, `order-receiver`) runs a `SequenceTracker` — an O(1)-per-order ring-bitset dedup + gap detector. `mark()` is hot-path and must stay allocation-free; `missing_ranges()` is O(gap span) and must only be called periodically, never per order.
- **Replay protocol**: a gap that persists past a short debounce window (not on first out-of-order arrival — thread-interleaving reordering resolves itself quickly) triggers a HMAC-signed `REPLAY_REQUEST` on a dedicated UDP control port, separate from the Aeron data stream — same separation-of-concerns pattern as the Raft control channel and monitoring corroboration channel. Repeated requests for a still-open gap must back off (never a tight retry loop).
- **Backpressure ↔ replay coupling — do not decouple these without re-checking both sides**:
  - `order-sending`'s per-node fan-out must use **non-blocking** sends — a node with no live subscriber or genuine backpressure must never stall delivery to the other two. This is safe only because replay exists to recover a skipped node's gap.
  - `order-process`'s ingest channel (Aeron poll callback → batch loop) must use **blocking** sends when `SequenceTracker::mark()` is called before the send — a non-blocking drop *after* marking is permanently invisible to gap detection, since the tracker already believes it saw that order.
- **S2<->S3 replay serving**: only the current Raft leader may respond to a replay request on the result channel (followers must stay silent on that channel, per §1 above) and must be enqueued through the same single thread that owns the result Aeron publication — never let two threads call `offer()` on the same publication.

See `docs/HLD.md` §7 for the full protocol detail and `docs/BENCHMARK.md` §0 for two real bugs this design surfaced under load (and their fixes).

---

## 4. Split-Brain Protection & monitoring Corroboration

- **Isolation Danger**: A node isolated from its peers cannot determine locally whether its peers are dead or whether it was partitioned.
- **monitoring Role**: `order-monitoring` is a non-sequencing arbiter running independently of S2 nodes.
- **Single-Node Promotion Rule**: When `ALLOW_SINGLE_NODE_LEADER=true` and `PEER_SILENT_MS` elapses:
  - If `REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER=true`, the isolated node MUST query `order-monitoring` via UDP.
  - Single-node self-promotion to `LEADER` is allowed ONLY IF `order-monitoring` corroborates that both peers are unreachable.
  - If monitoring reports any peer is reachable or monitoring is unresponsive, the node MUST stay passive.
- **Port Isolation**: monitoring probes use dedicated UDP health ports (`NODE{n}_HEALTH_PORT`: 6101, 6102, 6103) — NEVER the Raft consensus ports (6001, 6002, 6003). The S2<->S3 replay-request channel similarly gets its own dedicated port (`NODE{n}_REPLAY_PORT`: 6201, 6202, 6203) — never reuse the Raft, health, or order ports for it.
