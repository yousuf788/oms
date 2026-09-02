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
│       (S1)                                     (S2 Node 1, 2, 3)       │
│                                                       │                │
│  [order-witness] ◄──UDP Health Probes (610x)──────────┤                │
│    (Arbiter)     ──►Witness Corroboration (9101)──────┤                │
│                                                       ▼                │
│  [order-receiver] ◄──Aeron UDP (stream 2001)── (Leader Only)           │
│       (S3)                                                             │
└────────────────────────────────────────────────────────────────────────┘
```

### Microservice Roles & Crates

| Service | Directory | Role & Responsibility | Transport / Ports |
|---|---|---|---|
| **S1** | `order-sending/` | Order Generator. Multi-threaded OrderWire builder, bincode serializer & HMAC signer. Fan-out to all S2 nodes. Rate-paced for 200k-300k TPS scaling. | Aeron UDP Unicast `7001..7003` (Stream 1001) |
| **S2** | `order-process/` | 3-node Raft consensus cluster (`NODE1` Nitin, `NODE2` Amit, `NODE3` Yousuf). Order micro-batching (up to 20,000 orders/batch), $O(1)$ WAL appends, Leader election, Result publisher. | Raft UDP `6001..6003`, Order UDP `7001..7003`, Health UDP `6101..6103` |
| **S3** | `order-receiver/` | Result Sink. Subscribes to committed results from Raft Leader only. Order deduplication and async buffered disk writer (`logs/orders-received.log`). | Aeron UDP `8001` (Stream 2001) |
| **Witness** | `order-witness/` | Independent non-sequencing arbiter. Pings S2 nodes on `HEALTH_PORT` over UDP to corroborate single-node self-promotion and prevent split-brain. | UDP Corroboration `9101` |

---

## 4. Quick Command Reference

### Build & Check Commands
Run these commands from the specific crate root directory or repository root:

```bash
# Check single crate for compilation errors
cd order-process && cargo check
cd order-sending && cargo check
cd order-receiver && cargo check
cd order-witness && cargo check

# Build release binaries for all crates
cd order-process && cargo build --release
cd order-sending && cargo build --release
cd order-receiver && cargo build --release
cd order-witness && cargo build --release

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

# Terminal 5 (Independent Witness Arbiter)
cd order-witness && cargo run --release

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

## 5. Environment & Configuration Rules (`.env`)

> [!IMPORTANT]
> **NO HARDCODED IPs OR PORTS IN RUST SOURCE CODE.**
> All hostnames, IPs, ports, timeouts, and security keys MUST be read from `.env` via `dotenvy` and managed through each crate's `config.rs`.

### Critical Environment Variables

| Variable | Description | Default / Example |
|---|---|---|
| `NODE1_HOST`, `NODE2_HOST`, `NODE3_HOST` | IPv4 addresses for S2 cluster nodes | `127.0.0.1` (local) or lab LAN IPs |
| `NODE1_RAFT_PORT`, `NODE2_RAFT_PORT`, `NODE3_RAFT_PORT` | Raft RPC control ports | `6001`, `6002`, `6003` |
| `NODE1_ORDER_PORT`, `NODE2_ORDER_PORT`, `NODE3_ORDER_PORT` | Inbound order ports (Aeron stream 1001) | `7001`, `7002`, `7003` |
| `NODE1_HEALTH_PORT`, `NODE2_HEALTH_PORT`, `NODE3_HEALTH_PORT` | Liveness health probe ports (Witness) | `6101`, `6102`, `6103` |
| `S3_HOST`, `S3_PORT` | Result receiver bind address | `127.0.0.1` / `8001` |
| `WITNESS_HOST`, `WITNESS_PORT` | Independent Witness arbiter address | `127.0.0.1` / `9101` |
| `NODE_ID` | Explicit node override (1, 2, or 3) | Auto-detected from local IP if unset |
| `CLUSTER_HMAC_KEY` | HMAC key for signing/verifying order frames | 64-char hex string (`openssl rand -hex 32`) |
| `WITNESS_HMAC_KEY` | HMAC key for witness corroboration messages | 64-char hex string |
| `AERON_DIR` | Shared memory location for Aeron Media Driver | `/dev/shm/aeron-<uid>` |
| `ALLOW_SINGLE_NODE_LEADER` | Allow alone survivor node to self-elect | `true` |
| `REQUIRE_WITNESS_FOR_SINGLE_NODE_LEADER` | Enforce witness corroboration for single-node promotion | `true` (`false` only for local blind demo) |
| `PEER_SILENT_MS` | Peer silence threshold before failover | `2000` ms |
| `TARGET_TPS` | Rate pacing target for `order-sending` | `5000` to `300000` |

---

## 6. Performance Engineering Standards (200k–300k TPS Baseline)

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

## 7. Common Operational & Troubleshooting Procedures

| Issue / Error | Cause | Resolution |
|---|---|---|
| `thread 'main' panicked at NODE_ID not set...` | Machine IP doesn't match `.env` or multiple `127.0.0.1` entries exist | Pass explicit node ID: `./starter.sh 1` or `NODE_ID=1 cargo run` |
| `Aeron Media Driver already running / failed to connect` | Stale or missing Aeron IPC ring buffer in `/dev/shm` | Clean up `/dev/shm/aeron-*` or re-run `./starter.sh` |
| `HMAC failure / dropped order packet` | `CLUSTER_HMAC_KEY` missing or mismatched between `.env` files | Generate key via `openssl rand -hex 32` and copy same key to all `.env` files |
| WAL log divergence between S2 nodes | Unsynchronized node restarts or hard crash | Stop services, retain the longest valid WAL (`data/wal-s2-<id>.log`), delete corrupted WAL on lagging nodes, restart cluster |
| Node stays passive, never becomes LEADER | Witness unreachable or peers still reachable | Ensure `order-witness` is running or verify network connectivity to `WITNESS_HOST:9101` |
