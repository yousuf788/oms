# CLAUDE.md — Rust OMS Project Guide

This file provides context, build instructions, architectural rules, performance guidelines (200k–300k TPS target), environment configuration, and strict development workflow rules for working on this repository with Claude Code or AI coding assistants.

---

## 1. High Throughput Context & Core Mandate

> [!IMPORTANT]
> **TARGET PERFORMANCE: MINIMUM 200,000 TO 300,000 ORDERS / SECOND (200k–300k TPS)**
> This repository is an ultra-low latency, high-throughput financial Order Management System (OMS). Every architectural choice, data structure, memory allocation, lock pattern, and transport setting MUST be evaluated against this 200k–300k TPS baseline.

---

## 2. STRICT DEVELOPMENT WORKFLOW POLICY

> [!WARNING]
> **MANDATORY PRE-CHANGE PROTOCOL: RESEARCH → USER REVIEW → CODE MODIFICATION**
> Claude Code / AI assistants MUST NOT make direct code modifications without completing the following three-phase process:

```
┌────────────────────────────────────────────────────────────────────────┐
│                     MANDATORY DEVELOPMENT WORKFLOW                     │
│                                                                        │
│  1. RESEARCH & ANALYSIS  ──►  2. USER PROPOSAL & REVIEW  ──►  3. CODE  │
│  (Deep dive into code,      (Present technical design,       CHANGES   │
│   locks, channels, Aeron     ask for manual verification     (Only when│
│   buffers & benchmarks)      & suggestions)                  approved) │
└────────────────────────────────────────────────────────────────────────┘
```

1. **Phase 1: Deep Research & Bottleneck Analysis**
   - Thoroughly read and trace relevant code paths, data structures, locks, memory allocations, channel capacity, and Aeron media driver settings.
   - Review benchmark metrics (`docs/BENCHMARK.md`) and evaluate performance implications.
   - DO NOT edit or modify code files during this phase.

2. **Phase 2: Technical Proposal & User Verification Request**
   - Formulate a clear, detailed implementation proposal or plan outlining:
     - Root cause or optimization target.
     - Proposed data structure / concurrency changes.
     - Impact on latency and 200k–300k TPS throughput target.
     - Edge cases, safety risks, and verification strategy.
   - Ask the USER for manual verification, feedback, or suggestions before making any code changes.

3. **Phase 3: Approved Code Execution**
   - Apply code changes ONLY after the user has reviewed the proposal and provided approval or feedback.
   - Verify changes with compilation checks (`cargo check`) and benchmarks (`./scripts/run_benchmark.sh`).

---

## 3. System Overview

This repository implements a high-throughput, low-latency, 3-tier distributed Order Management System (OMS) built in Rust (2021 edition) using **Aeron UDP/IPC transport** (`rusteron-client`) and **Raft consensus** with **Write-Ahead Logging (WAL)**.

```
┌────────────────────────────────────────────────────────────────────────┐
│                        ORDER PIPELINE ARCHITECTURE                     │
│                                                                        │
│  [order-sending] ──Aeron UDP (stream 1001)──► [order-process] Cluster  │
│       (S1)         ◄────REPLAY_REQUEST (610x)──────┤ (S2 Node 1, 2, 3) │
│         WAL                                          │                │
│  [order-monitoring] ◄──UDP Health Probes (610x)──────────┤                │
│    (Arbiter)     ──►monitoring Corroboration (9101)──────┤                │
│                                                       ▼                │
│  [order-receiver] ◄──Aeron UDP (stream 2001)── (Leader Only)           │
│       (S3)       ──────REPLAY_REQUEST (620x)────────►│                │
│       checkpoint                                                      │
└────────────────────────────────────────────────────────────────────────┘
```

Every hop (S1→S2, S2→S3) has an independent HMAC-signed REPLAY_REQUEST control
channel, separate from the Aeron data streams — see §4 below.

### Microservice Roles & Crates

| Service | Directory | Role & Responsibility | Transport / Ports |
|---|---|---|---|
| **S1** | `order-sending/` | Order Generator. Multi-threaded OrderWire builder, bincode serializer & HMAC signer. Fan-out to all S2 nodes. Rate-paced for 200k-300k TPS scaling. | Aeron UDP Unicast `7001..7003` (Stream 1001) |
| **S2** | `order-process/` | 3-node Raft consensus cluster (`NODE1` Nitin, `NODE2` Amit, `NODE3` Yousuf). Order micro-batching (up to 20,000 orders/batch), $O(1)$ WAL appends, Leader election, Result publisher. | Raft UDP `6001..6003`, Order UDP `7001..7003`, Health UDP `6101..6103` |
| **S3** | `order-receiver/` | Result Sink. Subscribes to committed results from Raft Leader only. Order deduplication and async buffered disk writer (`logs/orders-received.log`). | Aeron UDP `8001` (Stream 2001) |
| **monitoring** | `order-monitoring/` | Independent non-sequencing arbiter. Pings S2 nodes on `HEALTH_PORT` over UDP to corroborate single-node self-promotion and prevent split-brain. | UDP Corroboration `9101` |

---

## 4. Sequence Identity, Gap Detection & Replay Protocol

> [!IMPORTANT]
> **Delivery model: at-least-once, with idempotent per-hop processing (effectively-once) — NOT exactly-once.** Replay makes duplicates possible by design; every hop dedups by `order_id`. Never claim exactly-once semantics for this system.

### 4.1 Sequence identity

- `order_id: u64` is assigned by `order-sending` from a counter that resumes from `order-sending/logs/orders-sent.wal`'s highest recorded id at startup — it survives process restart. Do not assume it starts at 1 on every run.
- `order_id` assignment order and Aeron publish order are **not guaranteed identical** — a shared atomic counter is read by multiple generator threads, so brief local reordering at ingest is normal and self-resolves; only a persistent gap triggers replay (see 50ms debounce below).

### 4.2 Per-service WAL / durable state

| Service | File | Durability role |
|---|---|---|
| `order-sending` | `logs/orders-sent.wal` | Length-prefixed bincode, O(1)-offset-indexed by `order_id`. Source of truth for S1<->S2 replay and counter resumption. |
| `order-process` | `logs/orders-processed*.log` | Raft-replicated log (`LogEntry{index,term,command}`), unchanged mechanism from before this work. Also serves S2<->S3 replay via `Wal::entries_with_order_id_range()` (linear scan — acceptable for a rare, bounded control-path op; add a secondary index if this ever becomes hot). |
| `order-receiver` | `logs/receiver-checkpoint.dat` | A single `u64` watermark (`last_contiguous`), flushed every 200ms — **not** a full result WAL. Used only to ask "replay everything since X" on restart. |

None of these WALs implement retention/truncation/snapshotting yet — they grow for the life of the process. This is a known, explicitly deferred gap, not an oversight.

### 4.3 Gap detection: `SequenceTracker`

`order-process/src/sequence_tracker.rs` and `order-receiver/src/sequence_tracker.rs` (duplicated per-crate, not shared — this repo isn't a Cargo workspace) implement a fixed-capacity ring bitset over a 1Mi-`order_id` window ahead of a `last_contiguous` watermark:
- `mark(order_id) -> bool`: O(1), allocation-free, called once per inbound item on the hot path. Returns `true` only the first time an id is seen — this is also the dedup mechanism (it replaced a bug where a per-batch `HashSet` in `order-process` only caught duplicates *within* one 20k-order batch).
- `missing_ranges() -> Vec<(u64,u64)>`: O(gap span), called periodically by a replay-request ticker — never per order.

### 4.4 REPLAY_REQUEST protocol

Both hops use the same shape: a HMAC-signed (`CLUSTER_HMAC_KEY`) UDP control channel, separate from the Aeron data stream (same separation-of-concerns pattern as the Raft control channel and monitoring corroboration channel) — `{requester_id: u8, ranges: Vec<(u64,u64)>}`, `to == u64::MAX` meaning "everything from `from` onward".

- **S2 → S1** (`order-process/src/replay_client.rs` → `order-sending/src/replay.rs`): a gap must persist 50ms (debounce, lets normal reordering resolve itself) before it's requested; repeated requests for a still-outstanding range back off exponentially (100ms → 5s cap) — never a tight retry loop. S1 replays by re-`offer()`-ing the exact `OrderWire` frame read back from its WAL onto the live Aeron order channel — replayed orders are wire-identical to live ones; the requester's own `SequenceTracker` dedups them.
- **S3 → S2** (`order-receiver/src/replay_client.rs` → `order-process/src/replay_server.rs`): broadcast to all 3 nodes' `NODE{n}_REPLAY_PORT` (order-receiver doesn't track Raft leadership); only the current leader acts on it (followers must stay silent on the result channel — an existing architectural invariant). The leader enqueues the request onto a bounded channel drained by `LeaderElection::result_publisher_loop`, which is the *sole* thread allowed to call `offer()` on the result publication — this is what lets it interleave replay traffic with live commits without two threads racing on the same Aeron publication.
- On startup, `order-receiver` also fires one unconditional catch-up request for `checkpoint+1..u64::MAX`, so a restart during an otherwise-idle pipeline still recovers (nothing else would reveal the gap).

### 4.5 Backpressure interacts with replay — know this before touching either

- `order-sending`'s fan-out thread uses **non-blocking** `try_send` per node channel (a node with no live subscriber, or genuinely backpressured, must never stall delivery to the other two — this was a real bug: the original blocking `send()` let one dead node stall the whole fan-out). A skipped node is safe specifically *because* replay exists to recover it — don't revert this to blocking without re-checking that invariant.
- `order-process`'s ingest thread uses **blocking** `send()` into its internal `crossbeam_channel` (500k capacity) between the Aeron poll callback and the batching loop. This is deliberate: `SequenceTracker::mark()` is called *before* the send, so a non-blocking drop after marking would be permanently invisible to gap detection (the tracker already thinks it "saw" that order). Don't change this back to `try_send` without also changing when `mark()` happens.

## 5. Quick Command Reference

### Build & Check Commands
Run these commands from the specific crate root directory or repository root:

```bash
# Check single crate for compilation errors
cd order-process && cargo check
cd order-sending && cargo check
cd order-receiver && cargo check
cd order-monitoring && cargo check

# Build release binaries for all crates
cd order-process && cargo build --release
cd order-sending && cargo build --release
cd order-receiver && cargo build --release
cd order-monitoring && cargo build --release

# Run lints / clippy
cd order-process && cargo clippy --all-targets -- -D warnings
```

### Running Services Locally

#### Local 3-Node Simulation (`order-process`)
Use `starter.sh` with explicit `NODE_ID` parameters (1, 2, or 3) across 3 separate terminal windows:
```bash
# Terminal 1 (Node 1 - Nitin)
cd order-process && ./starter.sh 1

# Terminal 2 (Node 2 - Amit)
cd order-process && ./starter.sh 2

# Terminal 3 (Node 3 - Yousuf)
cd order-process && ./starter.sh 3
```

#### Running Auxiliary Services
```bash
# Terminal 4 (S3 Result Receiver)
cd order-receiver && cargo run --release

# Terminal 5 (Independent monitoring Arbiter)
cd order-monitoring && cargo run --release

# Terminal 6 (S1 Order Generator)
cd order-sending && cargo run --release
```

### Benchmarking
```bash
# Run 1-node baseline benchmark (1 node, 4 threads, 10s duration)
./scripts/run_benchmark.sh 1 4 10

# Run 3-node Raft consensus benchmark (3 nodes, 8 threads, 10s duration)
./scripts/run_benchmark.sh 3 8 10
```

---

## 6. Environment & Configuration Rules (`.env`)

> [!IMPORTANT]
> **NO HARDCODED IPs OR PORTS IN RUST SOURCE CODE.**
> All hostnames, IPs, ports, timeouts, and security keys MUST be read from `.env` via `dotenvy` and managed through each crate's `config.rs`.

### Critical Environment Variables

| Variable | Description | Default / Example |
|---|---|---|
| `NODE1_HOST`, `NODE2_HOST`, `NODE3_HOST` | IPv4 addresses for S2 cluster nodes | `127.0.0.1` (local) or lab LAN IPs |
| `NODE1_RAFT_PORT`, `NODE2_RAFT_PORT`, `NODE3_RAFT_PORT` | Raft RPC control ports | `6001`, `6002`, `6003` |
| `NODE1_ORDER_PORT`, `NODE2_ORDER_PORT`, `NODE3_ORDER_PORT` | Inbound order ports (Aeron stream 1001) | `7001`, `7002`, `7003` |
| `NODE1_HEALTH_PORT`, `NODE2_HEALTH_PORT`, `NODE3_HEALTH_PORT` | Liveness health probe ports (monitoring) | `6101`, `6102`, `6103` |
| `NODE1_REPLAY_PORT`, `NODE2_REPLAY_PORT`, `NODE3_REPLAY_PORT` | S2<->S3 replay-request control ports (order-receiver's REPLAY_REQUEST target) | `6201`, `6202`, `6203` |
| `S1_HOST`, `S1_REPLAY_PORT` | order-sending's replay listener — where order-process sends S1<->S2 REPLAY_REQUEST | required / `9001` (matches `SENDER_BIND_PORT`) |
| `S3_HOST`, `S3_PORT` | Result receiver bind address | `127.0.0.1` / `8001` |
| `monitoring_HOST`, `monitoring_PORT` | Independent monitoring arbiter address | `127.0.0.1` / `9101` |
| `NODE_ID` | Explicit node override (1, 2, or 3) | Auto-detected from local IP if unset |
| `CLUSTER_HMAC_KEY` | HMAC key for signing/verifying order frames, Raft control messages, the result channel, and both replay-request channels. Required on `order-sending`, `order-process`, **and `order-receiver`** | 64-char hex string (`openssl rand -hex 32`) |
| `monitoring_HMAC_KEY` | HMAC key for monitoring corroboration messages | 64-char hex string |
| `AERON_DIR` | Shared memory location for Aeron Media Driver | `/dev/shm/aeron-<uid>` |
| `ALLOW_SINGLE_NODE_LEADER` | Allow alone survivor node to self-elect | `true` |
| `REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER` | Enforce monitoring corroboration for single-node promotion. **Casing is exact and load-bearing** — this is not `REQUIRE_MONITORING_...`; it's a leftover artifact of the order-witness→order-monitoring rename that didn't normalize case (same root cause as the `monitoringClient`/`monitoring_KEY` Rust identifiers). Get the casing wrong and the env var is silently unset, which means "true" (the default) | `true` (`false` only for local blind demo) |
| `PEER_SILENT_MS` | Peer silence threshold before failover | `2000` ms |
| `TARGET_TPS` | Rate pacing target for `order-sending` | `5000` to `300000` |

---

## 7. Performance Engineering Standards (200k–300k TPS Baseline)

### Memory & Allocation Strategy
- **Zero Allocation Hot Loop**: Pre-allocate vector buffers (e.g. `Vec::with_capacity(20_000)`) and hash sets (`HashSet::with_capacity(20_000)`). Reuse buffers in long-lived processing loops instead of allocating/deallocating per batch.
- **Copy vs Reference**: Use `Copy` primitive structs for wire serialization (`OrderWire`) to eliminate allocation overhead.

### Wire Format Synchronization (`bincode`)
> [!CAUTION]
> `bincode` (v1.3) encodes structs **positionally** based on field declaration order and field types, NOT by field names.

- **Order Ingress (`OrderWire`)**:
  `order-sending/src/main.rs::OrderWire` and `order-process/src/main.rs::OrderWire` **MUST MAINTAIN IDENTICAL FIELD DECLARATION ORDER AND TYPES**:
  ```rust
  #[derive(Serialize, Deserialize, Debug, Clone, Copy)]
  struct OrderWire {
      order_id: u64,
      symbol: u8,
      side: bool,
      qty: u32,
      ts_ms: u64,
  }
  ```
- **Committed Result Egress (`ReplicatedCommand` / `ResultWire`)**:
  `order-process/src/wal.rs::ReplicatedCommand` and `order-receiver/src/main.rs::ResultWire` **MUST MAINTAIN IDENTICAL FIELD DECLARATION ORDER AND TYPES**:
  ```rust
  struct ReplicatedCommand / ResultWire {
      order_id: u64,
      symbol: String,
      side: String,
      qty: u32,
      status: String,
      filled_qty: u32,
      processed_by: String,
      term: u64,
  }
  ```

### High-Throughput & Low-Latency Patterns
- **$O(1)$ Append-Only WAL**: Never overwrite, truncate, or rewrite full log files synchronously during normal order appends. Use persistent `File` handles with line/binary appends.
- **Lock Minimization**: Minimize `Mutex` / `RwLock` acquisition inside hot replication loops. Acquire locks outside batch operations.
- **Async Non-Blocking Logging**: Do NOT execute blocking file I/O or `println!` statements per order in hot paths. Stream log entries via bounded `crossbeam-channel` or `mpsc::sync_channel` to a dedicated background writer thread with 64KB / 50ms flush buffers.
- **Order Micro-Batching**: Accumulate inbound orders into micro-batches (up to 20,000 orders) before issuing Raft replication and disk append calls.

---

## 8. Common Operational & Troubleshooting Procedures

| Issue / Error | Cause | Resolution |
|---|---|---|
| `thread 'main' panicked at NODE_ID not set...` | Machine IP doesn't match `.env` or multiple `127.0.0.1` entries exist | Pass explicit node ID: `./starter.sh 1` or `NODE_ID=1 cargo run` |
| `Aeron Media Driver already running / failed to connect` | Stale or missing Aeron IPC ring buffer in `/dev/shm` | Clean up `/dev/shm/aeron-*` or re-run `./starter.sh` |
| `HMAC failure / dropped order packet` | `CLUSTER_HMAC_KEY` missing or mismatched between `.env` files | Generate key via `openssl rand -hex 32` and copy same key to all `.env` files — **order-receiver now needs it too** (it verifies the result channel and signs replay requests) |
| `missing S1_HOST in environment` panic on `order-process` | New required var (§6) not yet in this node's `.env` | Add `S1_HOST` / `S1_REPLAY_PORT` pointing at wherever `order-sending` runs |
| WAL log divergence between S2 nodes | Unsynchronized node restarts or hard crash | Stop services, retain the longest valid WAL (`data/wal-s2-<id>.log`), delete corrupted WAL on lagging nodes, restart cluster |
| Node stays passive, never becomes LEADER | monitoring unreachable or peers still reachable | Ensure `order-monitoring` is running or verify network connectivity to `monitoring_HOST:9101` |
| Single-node self-promotion never happens even with no monitoring running | `REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER` env var name typo'd with "standard" casing (`REQUIRE_MONITORING_...`) | Match the exact casing the code reads: `REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER=false` — see §6's note |
| Leader keeps flapping under load (`[role]` cycles between all 3 nodes continuously), throughput never progresses | Running all 3 Raft nodes + sender + receiver as competing processes on one shared/contended machine — the leader's heartbeat thread misses the election timeout from scheduling delay, not a real failure. Confirmed root cause of a total pipeline stall during benchmarking (see `docs/BENCHMARK.md`) | For single-machine testing under load, widen `HEARTBEAT_INTERVAL_MS`/`ELECTION_TIMEOUT_MIN_MS`/`ELECTION_TIMEOUT_MAX_MS` (e.g. 100/800/1500) — `scripts/run_benchmark.sh` already does this. Real 3-machine deployments don't need this. |
| Large persistent gap in `order-receiver`'s log that never closes | Sustained input rate exceeds this deployment's real processing ceiling — backlog grows without bound past capacity (a cliff, not graceful degradation; there's no adaptive rate control feeding back to the sender yet) | Check `docs/BENCHMARK.md` for this environment's measured ceiling; lower `TARGET_TPS` or add capacity. Confirm it isn't the leader-flapping issue above first. |
