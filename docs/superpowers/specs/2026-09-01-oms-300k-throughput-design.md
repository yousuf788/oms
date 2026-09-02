# OMS 300k orders/sec throughput — design spec

## Context

The OMS pipeline (`order-sending` → `order-process` 3-node Raft cluster → `order-receiver`)
was measured at ~4,744 orders/sec processed on a single-machine benchmark of the current
Aeron-based build (see chat history / `docs/BENCHMARK.md`'s pre-Aeron numbers for the older
raw-UDP baseline). The goal of this project is to **prove** — as a bounded benchmark run, not
a permanent production duty cycle — that the pipeline can sustain 300,000 orders/sec on all
three legs (sent, processed, received) with zero data loss (sent count == processed count ==
received count), on the real 3-machine lab deployment described in `docs/HLD.md`
(Nitin / Amit / Yousuf), while keeping 3-node Raft consensus and WAL durability.

Constraints established during design:
- **Topology**: real 3-physical-machine lab deployment, not the single-machine benchmark script.
- **LAN bandwidth**: unknown at design time. Design minimizes bytes-on-wire regardless (binary
  wire format) so bandwidth is not assumed to be the bottleneck, but this must be confirmed once
  tested on the real machines.
- **Wire format**: switching from JSON to a compact binary format (`bincode`) is in scope. This is
  a breaking change to the WAL file format, Aeron order/result payloads, and Raft control messages
  — acceptable since this is a lab/demo system, not production data that must remain readable.
- **WAL durability**: OS-buffered writes are sufficient (this is already the current behavior —
  `wal.rs` never calls `fsync`/`flush` on the hot append path today). No explicit fsync is being
  added. Data survives leader crash + Raft failover (replicas have it too); only a simultaneous
  power-loss across all 3 machines could lose the last few ms of unflushed writes.
- **Run duration**: this is a benchmark/proof run (tens of seconds to a couple of minutes), not an
  indefinite production process. Unbounded-growth structures (the receiver's dedup `HashSet<u64>`,
  the append-only WAL file) do not need eviction/rotation logic for this scope.
- **Known infrastructure constraint** (not solved in code): per `HLD.md`'s topology,
  `order-sending`, `order-process` Node 3, and `order-receiver` all run on the **same physical
  machine** (Server 1 / Yousuf) when Node 3 is leader. All three "legs" of the 300k/sec target
  would compete for that one machine's CPU/disk/NIC simultaneously. This is flagged as a risk to
  validate empirically, not something this design changes.

## A correctness bug found during design (fix regardless of throughput target)

In `order-process/src/leader_election.rs`, `LeaderElection::propose_batch()` writes a batch to
the WAL, replicates it, then polls for up to 1500ms for it to commit and apply. If that window
expires, it returns an empty `Vec`. The caller (`order-process/src/main.rs`,
`process_orders_batch_as_leader`) does nothing further when it gets an empty result — meaning
those orders are durably written to the leader's WAL and *do* eventually get committed/applied
by later background bookkeeping, but **the result message for that batch is never published to
S3**, because publishing only happens inside the very call that already gave up and returned
empty. Today this rarely triggers under light load; at high throughput (small `AppendEntries`
batches relative to inbound order rate) it would trigger routinely, silently dropping results
while they remain readable in the leader's own WAL. This must be fixed as part of this work,
independent of whether 300k/sec is ultimately reached.

## Design

### 1. Wire format (all binary via `bincode`)

New/changed structs, all `derive(Serialize, Deserialize)` and encoded with `bincode`:

- `OrderWire { order_id: u64, symbol: u8, side: bool, qty: u32, ts_ms: u64 }` — replaces the
  current JSON string built by hand in `order-sending/src/main.rs` and parsed via
  `serde_json::from_slice::<Order>` in `order-process/src/main.rs`. `symbol` becomes a small
  enum/index over the fixed symbol list (`BTC-USDT`/`ETH-USDT`/`SOL-USDT`); `side` becomes a
  `bool`.
- `ResultWire { order_id, symbol, side, qty, status: u8, filled_qty, processed_by: String, term,
  received_ts_ms }` — `processed_by` stays a `String` (e.g. `"Yousuf (S2-3)"`; `bincode`
  length-prefixes strings natively, no fixed-width encoding needed) — replaces the
  `serde_json::json!({...})` construction in
  `order-process/src/main.rs` and the dynamic `serde_json::Value` parse/mutate/restring in
  `order-receiver/src/main.rs`.
- `ReplicatedCommand` / `LogEntry` (`order-process/src/wal.rs`) keep their current fields —
  only their (de)serialize call sites switch from `serde_json` to `bincode`.
- The Raft `Message` enum (`order-process/src/leader_election.rs`) drops
  `#[serde(tag = "type")]` (a JSON-only internally-tagged representation) so it serializes as a
  plain enum, which `bincode` handles as a variant index + payload natively.
- **WAL file framing changes**: from newline-delimited JSON text to length-prefixed binary
  records — a `u32` little-endian length header before each `bincode`-encoded `LogEntry`. This
  touches `Wal::load_entries`, `append_single_entry`, `append_leader_batch`, and `rewrite_all`
  in `order-process/src/wal.rs`. Existing on-disk WAL files from before this change are not
  compatible (acceptable — lab data, not production).

### 2. `order-sending` — parallel per-node fan-out

Current: one publisher thread loops over all 3 `AeronExclusivePublication`s sequentially per
message (`order-sending/src/main.rs`), so a slow/backpressured node stalls delivery to the other
two.

New: after the existing generator threads (unchanged) feed a bounded channel of `OrderWire`
values, a fan-out stage distributes each order to **3 dedicated publisher threads**, one per S2
node, each owning exactly one publication and one bounded channel, retrying `offer()`
independently with its own `BusySpinIdleStrategy`. Backpressure on one node's channel no longer
blocks the other two.

**Implementation detail confirmed against `rusteron-client-0.2.5`'s generated bindings**:
`AeronExclusivePublication` (what `order-sending` uses today, via
`async_add_exclusive_publication`) has no `Send` impl at all — the crate's own generated
comment states it explicitly ("the caller must still confirm the underlying Aeron object is
thread-safe (e.g. `AeronPublication` is, `AeronExclusivePublication` is not)"), so it cannot be
moved into a dedicated thread even once, let alone shared. `AeronPublication` (the non-exclusive
variant — already what `order-process`/`order-receiver` use) **does** get an unconditional
`unsafe impl Send`, with no Cargo feature flag required, specifically so a handle can be moved to
and then used exclusively within one owning thread. This design therefore switches
`order-sending` from `async_add_exclusive_publication` to `async_add_publication` (matching the
other two services), creates all 3 publications up front in `main()` as today, then **moves**
(not shares) one into each of the 3 dedicated publisher threads. `AeronPublication` exposes the
identical `.offer()` API (same `impl_publication_methods!` macro backs both types), so no other
call-site changes are needed. Trade-off: the C library does a small amount of extra internal
synchronization per `offer()` on the non-exclusive type (it supports concurrent callers, even
though we only ever use one thread per handle here) — a minor per-call cost that removing the
single-thread head-of-line blocking across all 3 nodes should easily outweigh.

### 3. `order-process` — decoupled result delivery + right-sized replication batches

- **Bug fix (see above)**: add a dedicated background thread (spawned in
  `LeaderElection::start()` or from `main.rs` once the election handle and result publication
  exist) that continuously watches `commit_index`, and for each newly committed index not yet
  published, reads the entry back from the WAL via the existing `wal.entry_at(idx)` pattern,
  builds a `ResultWire`, and publishes it to S3 — independent of any specific `propose_batch()`
  call's timeout. Tracks its own `last_published` index (separate from `last_applied`). Only
  publishes while `is_leader()` is true, matching the existing "only the active leader emits to
  S3" behavior.
- `propose_batch()` keeps a bounded wait so its caller can decide whether to keep pulling more
  orders (flow control / backpressure signal), but its return value no longer gates whether a
  result reaches S3.
- `MAX_ENTRIES_PER_APPEND` (currently a fixed count of 32 entries) becomes a **byte-size budget**
  (~1400 bytes, safely under standard Ethernet MTU after headers) instead of a raw entry count.
  Worth noting: today's fixed count-of-32 with JSON-encoded entries can already exceed real MTU
  and silently rely on IP fragmentation (where a single lost fragment loses the whole datagram) —
  switching to a byte budget fixes this regardless of the format change, and with binary encoding
  yields more entries per datagram than JSON did at the same safety margin.
- The raw `UdpSocket` used for the Raft control channel (`leader_election.rs`) has no explicit
  `SO_RCVBUF`/`SO_SNDBUF` tuning today, unlike the earlier fix (commit `9faf1c7`) that raised
  Aeron-adjacent buffers to 8MB. At 300k/sec-driven replication rates this socket could become its
  own loss point. This design adds equivalent buffer tuning here.

### 4. `order-receiver` — fix the actual bottleneck

`append_received_log()` in `order-receiver/src/main.rs` currently opens the log file, writes a
line, and flushes **on every single received message** — it never got the async batched-writer
treatment that `order-sending` and `order-process` already have (per `docs/BENCHMARK.md`'s
"Asynchronous Background Logging" optimization). This alone likely caps receiver throughput at a
few thousand/sec independent of everything else in this design.

Fix: give it the same bounded-channel + dedicated background-writer pattern already used in
`order-sending` (`order-sending/src/main.rs`'s log writer thread — buffer to 64KB or flush on a
50ms idle timeout, whichever comes first; the receiver's writer mirrors these exact thresholds
for consistency, not just the general shape of the pattern). Also replace the generic
`serde_json::Value` parse → field mutation → `to_string()`
path with a typed `ResultWire` `bincode` decode; the log line itself is written as a cheap
`format!()` text line (not re-serialized as JSON/bincode) so it stays human-readable and
`wc -l`-friendly for scoring, matching how `run_benchmark.sh` already counts results.

Dedup (`HashSet<u64> seen_order_ids`) is left as an unbounded set, which is acceptable for this
scope (a bounded benchmark run, not an indefinite process — see Context).

### 5. Verification plan

- Extend the existing `scripts/run_benchmark.sh` pattern (currently single-machine-only, with its
  own isolated ports/config) into per-role launch steps that can run against the real `.env` on
  each of Nitin/Amit/Yousuf: `order-sending` driven with `TARGET_TPS=300000` and enough
  `SENDER_THREADS`, `order-process` started via the existing `starter.sh` on each of the 3
  machines, `order-receiver` on Yousuf.
- Success criterion: total sent == total processed == total received, sustained at ~300,000/sec
  over the run duration, read from the (now-batched) log files the same way
  `run_benchmark.sh` already scores runs (`wc -l`).
- Expectation, stated upfront: hitting exactly 300k/sec on the first real run on real hardware is
  unlikely. This will need at least one tuning pass (thread counts, channel sizes, the
  `MAX_ENTRIES_PER_APPEND` byte budget) once real core counts, NIC speed, and disk performance on
  the lab machines are observed — none of which are known at design time.

## Out of scope (explicitly rejected during design)

- Migrating the Raft control channel (`RequestVote`/`AppendEntries`/heartbeats) onto Aeron
  streams for its own retransmission/backpressure guarantees. Rejected as unnecessarily risky for
  this project — it would touch the core consensus file more invasively than the byte-budget fix
  above, and the existing tick-driven retry loop already self-heals occasional UDP loss on the
  control channel.
- Bounded-memory dedup (time-windowed/LRU) and WAL log rotation/segmentation — only needed for an
  indefinite production duty cycle, not a bounded proof run (see Context).
- Solving the Server-1 co-location constraint (order-sending + order-process leader +
  order-receiver sharing one machine) — this is an infrastructure/ops decision, not something this
  design changes in code. Flagged as a risk to observe during the real-hardware run.
