# Security & Binary Data Transmission — Technical Briefing

> Scope: this document reports strictly what is implemented and measurable in this repository today (as of 2026-09-03, branch `feature/handle-order`). It does not contain recommendations, threat-model judgments, or projected numbers. Every claim below is grounded in a specific file:line or a specific line in `docs/BENCHMARK.md`. Where a category of data (e.g. latency, payload size, CPU/memory) does not exist in the codebase or its benchmarks, that is stated explicitly rather than estimated.

---

## 1. Architecture / Data-Flow Overview

Four independent crates (not a Cargo workspace — each vendors its own copy of shared types) communicate over Aeron UDP and raw UDP control sockets:

```
[order-sending] --Aeron UDP order channel (stream 1001)--> [order-process] (3-node Raft: NODE1/2/3)
     (S1)        <---REPLAY_REQUEST, raw UDP, S1_REPLAY_PORT---+
                                                                |
[order-monitoring] <--UDP health probes (NODE{n}_HEALTH_PORT)--+
   (Arbiter)      --corroboration UDP (monitoring_PORT)------->|
                                                                v
[order-receiver] <--Aeron UDP result channel (stream 2001)-- (Raft leader only)
     (S3)         ---REPLAY_REQUEST, raw UDP, NODE{n}_REPLAY_PORT-->
```

- Aeron channel URIs are plain `aeron:udp?endpoint={host}:{port}` strings with no security parameters (`order-sending/src/main.rs:175`, `order-process/src/main.rs:135,155`, `order-receiver/src/main.rs:92`).
- Both `REPLAY_REQUEST` hops and the monitoring corroboration channel run over raw `std::net::UdpSocket`, separate from Aeron.
- The Raft control channel (`order-process/src/leader_election.rs`) is also raw UDP, independent of Aeron.

---

## 2. End-to-End Security Mechanisms

### 2.1 What is actually implemented: HMAC-SHA256 authentication (no encryption)

Every one of the four crates has its own `src/auth.rs`. All four use the identical primitive — **HMAC-SHA256** from the RustCrypto `hmac`/`sha2` crates. No other cryptographic primitive (AES, ChaCha, RSA, Ed25519, etc.) appears anywhere in the codebase.

| Crate | File | Keys used | Env var(s) |
|---|---|---|---|
| order-sending | `order-sending/src/auth.rs:1-89` | cluster key | `CLUSTER_HMAC_KEY` |
| order-process | `order-process/src/auth.rs:1-113` | cluster key + monitoring key | `CLUSTER_HMAC_KEY`, `monitoring_HMAC_KEY` |
| order-receiver | `order-receiver/src/auth.rs:1-83` | cluster key | `CLUSTER_HMAC_KEY` |
| order-monitoring | `order-monitoring/src/auth.rs:1-81` | monitoring key | `monitoring_HMAC_KEY` |

Dependency versions (identical across all four `Cargo.toml`/`Cargo.lock`): `hmac = "0.12"` → resolved `hmac 0.12.1`, `sha2 = "0.10"` → resolved `sha2 0.10.9`, plus support crates `digest 0.10.7`, `crypto-common 0.1.7`, `cpufeatures 0.2.17`, and `subtle 2.6.1` (constant-time comparison, used by `mac.verify_slice`).

**Key loading**: keys are hex-decoded from the env var once into a `static OnceLock<Vec<u8>>` (e.g. `order-sending/src/auth.rs:26-32`). A missing or malformed key panics on first use — enforced eagerly at startup (`order-sending/src/main.rs:81`, `order-monitoring/src/main.rs:19`, `order-process/src/leader_election.rs:214`).

**Function signatures** (uniform shape): `pub fn sign(payload: &[u8]) -> Vec<u8>`, `pub fn verify(frame: &[u8]) -> Option<&[u8]>`. `order-process` additionally exposes key-parameterized `sign_with`/`verify_with` plus `sign_monitoring`/`verify_monitoring` wrappers (`order-process/src/auth.rs:65,78,97,101,106,110`).

### 2.2 Signed-frame wire layout (identical on every channel)

```
[ 4 bytes big-endian payload_len ][ payload bytes ][ 32 bytes HMAC-SHA256 tag ]
```
Documented verbatim at the top of every `auth.rs` (e.g. `order-sending/src/auth.rs:3-4`). The tag is **appended** (suffix), and the MAC covers **only the payload bytes** — `mac.update(payload)` — not the length prefix or any header (`order-sending/src/auth.rs:58,85`).

### 2.3 No confidentiality anywhere

A repo-wide search for `aes|chacha|tls|dtls|encrypt|decrypt|cipher|rustls|native-tls|openssl` in application code returns nothing beyond a comment referencing the `openssl rand -hex 32` *shell command* used to generate the HMAC key, and `.cargo/config.toml` build settings for `rusteron-client`'s C bindings. `openssl`, `openssl-sys`, and `rustls-pki-types` do appear in `Cargo.lock`, but only as transitive build dependencies pulled in by `reqwest`, which `rusteron-client` uses to download precompiled Aeron media-driver binaries — no application code calls `reqwest`, `openssl`, or any TLS crate directly.

**Conclusion**: every order (symbol, side, qty), every committed result, every replay request, and every Raft log entry is transmitted as **plaintext** bincode (or JSON, see §3.4) with an appended MAC. HMAC-SHA256 provides integrity and authenticity only — there is no encryption/confidentiality mechanism anywhere in this system, and Aeron's own transport configuration (`AeronContext::new()` + `set_dir`/`set_error_handler` only — e.g. `order-sending/src/main.rs:88-93`) carries no TLS/mTLS/DTLS option.

### 2.4 No dedicated anti-replay nonce inside the HMAC layer

The MAC does not include a sequence number, timestamp, or nonce. This is an explicit, documented design choice: `order-sending/src/auth.rs:9-11` states a sequence number "is not required to prevent reordering attacks at this layer," relying instead on Aeron's in-order delivery for the data channel and on **application-layer** `order_id`-based dedup (`SequenceTracker`) plus Raft `term` numbers for anything that needs replay/reorder protection.

### 2.5 Verify-before-deserialize on every channel

| Channel | Verify call | Deserialize call | Order |
|---|---|---|---|
| Order (S1→S2, Aeron) | `order-process/src/main.rs:205` `auth::verify(buf)` | `bincode::deserialize::<OrderWire>` (`:206`) | verify → deserialize |
| Result (S2→S3, Aeron) | `order-receiver/src/main.rs:175` `auth::verify(buf)` | `bincode::deserialize::<ResultWire>` (`:181`) | verify → deserialize |
| REPLAY_REQUEST, S2→S1 | `order-sending/src/replay.rs:58` `auth::verify(...)` | `bincode::deserialize::<ReplayRequest>` (`:63`) | verify → deserialize |
| REPLAY_REQUEST, S3→S2 | `order-process/src/replay_server.rs:50` `auth::verify(...)` | `bincode::deserialize::<ReplayRequest>` (`:56`) | verify → deserialize |
| Monitoring corroboration (received by order-monitoring) | `order-monitoring/src/corroboration.rs:91` `auth::verify(...)` | `serde_json::from_slice::<CorroborationMsg>` (`:99`) | verify → deserialize (**JSON**, not bincode) |
| Monitoring corroboration (received by order-process) | `order-process/src/monitoring_client.rs:197` `auth::verify_monitoring(...)` | `serde_json::from_slice::<CorroborationMsg>` (`:211`) | verify → deserialize |
| Raft control (S2 inter-node) | IP allowlist (`leader_election.rs:996-1001`) → `auth::verify` (`:1007-1014`) | `bincode::deserialize` (`:1016`) | IP filter → verify → deserialize |

On HMAC failure every path drops the packet (`None`/`continue` + log line) — unauthenticated bytes are never deserialized. The Raft control channel is the **only** channel with an additional source-IP allowlist pre-filter; it is absent everywhere else.

---

## 3. Binary Data Format / Protocol

### 3.1 Library and configuration

`bincode = "1.3"` in `order-sending`, `order-process`, `order-receiver` Cargo.toml, resolved to `bincode 1.3.3` in all three lockfiles (`order-monitoring` does not depend on bincode at all — it only sends JSON, see §3.4). A repo-wide search for `bincode::config`, `bincode::Options`, `DefaultOptions`, `with_fixint`, `with_varint` returns **zero matches** — every call site uses the bare `bincode::serialize`/`bincode::deserialize`/`bincode::serialized_size` free functions, i.e. bincode v1.3's implicit default: little-endian, fixed-width integers, 8-byte length prefixes for `String`/`Vec`, 4-byte `u32` variant discriminant for enums.

### 3.2 Wire structs actually transmitted

| Struct | Declared at (both sides must match — bincode is positional) | Derives | Fixed size? |
|---|---|---|---|
| `OrderWire` | `order-sending/src/main.rs:34` / `order-process/src/main.rs:36` | `Serialize, Deserialize, Debug, Clone, Copy` | **Yes — 26 bytes** (`u64`+`u8`+`bool`+`u32`+`u64` = 8+1+1+4+8+4* = 26; bool encodes as 1 byte) |
| `ReplicatedCommand` / `ResultWire` | `order-process/src/wal.rs:6` / `order-receiver/src/main.rs:32` | `Clone, Serialize, Deserialize, Debug` (no `Copy` — has `String` fields) | No — 40 fixed bytes + the UTF-8 byte length of 4 `String` fields (`symbol`, `side`, `status`, `processed_by`) |
| `ReplayRequest` (4 independent copies — one per hop side) | `order-sending/src/replay.rs:27`, `order-process/src/replay_client.rs:37`, `order-process/src/replay_server.rs:20`, `order-receiver/src/replay_client.rs:22` | `Serialize, Deserialize, Debug` (no `Clone`/`Copy`) | No — `1 + 8 + 16·N` bytes for N `(u64,u64)` ranges |
| `Message` (Raft control enum) | `order-process/src/leader_election.rs:91` | `Serialize, Deserialize, Debug` | No — 4-byte variant tag + variant-specific fields (`AppendEntries` embeds `Vec<LogEntry>`, itself variable) |

`OrderWire` is the only wire struct composed entirely of fixed-width primitives and the only one deriving `Copy` — consistent with it being the highest-volume message (once per order, on the S1→S2 hot path).

Every `bincode::serialize`/`deserialize` call site was enumerated (18 total across the 3 bincode-using crates); all encode/decode either `OrderWire`, `LogEntry`/`ReplicatedCommand`, `ReplayRequest`, or the Raft `Message` enum — no other wire type exists.

### 3.3 The one non-bincode wire type in the system

`order-monitoring`'s corroboration protocol (`CorroborationMsg`) is serialized with `serde_json`, not bincode — it's the only JSON payload in the codebase, and `order-monitoring/Cargo.toml` has no `bincode` dependency at all. Separately, `Role::as_u8()` (`order-process/src/leader_election.rs:76-81`) is a hand-rolled single-byte encoding (not bincode, not JSON) used by the health-probe ping/pong protocol.

---

## 4. Performance Characteristics of the Binary Format (as implemented, no external benchmark exists to quantify these)

These are structural facts about the code, not measured numbers — §5 covers what is and isn't actually benchmarked.

1. **`OrderWire` is a fixed 26-byte, all-primitive, `Copy` struct.** It has no heap-allocated fields (no `String`/`Vec`), so encoding/decoding it involves no allocation and no variable-length parsing — unlike `ReplicatedCommand`/`ResultWire`, which carry four `String` fields and are only `Clone` (not `Copy`). This asymmetry lines up with where each struct sits in the pipeline: `OrderWire` is on the highest-frequency hot path (S1→S2, per order), while the `String`-bearing result struct is emitted once per committed order on the lower-frequency S2→S3 path.
2. **bincode's positional encoding carries no field-name strings on the wire** — nothing analogous to JSON object keys is serialized or parsed. The one place JSON is actually used in this repo (`CorroborationMsg`, §3.3) is deliberately confined to the low-frequency, non-hot-path monitoring corroboration channel, not the order or result channels.
3. **Binary size is computed without serializing.** `order-process/src/leader_election.rs:35` calls `bincode::serialized_size(&entry)` to budget how many `LogEntry` records fit under a per-`AppendEntries` byte budget (the ~1400-byte target noted in `docs/superpowers/specs/2026-09-01-oms-300k-throughput-design.md`) before actually building the batch — a property that depends on bincode's deterministic, non-self-describing encoding.
4. **No in-repo benchmark quantifies any of the above.** There is no bincode-vs-JSON timing comparison, no measured ns/encode or ns/decode figure, and no measured wire-byte-count for any message type anywhere in the codebase or in `docs/BENCHMARK.md`/`docs/HLD.md`. Point 1–3 above are verifiable structural facts about the code; they are not benchmark results.

---

## 5. Benchmark Results (from `docs/BENCHMARK.md`, quoted as recorded)

### 5.1 Current, post-replay-protocol results (§0, dated 2026-09-03)

| Configuration | Sustained clean throughput | Evidence |
|---|---|---|
| 1 node (no Raft replication), 30s run | **~20,000 orders/sec** | 593,920 sent → 595,005 received, 0 missing, 0 duplicates |
| 3 nodes (full Raft replication), 20s run | **~7,000–8,000 orders/sec** | 137,216 sent → 137,842 received, 0 missing, 0 duplicates |

Measured on a single shared 12-core desktop machine (observed load average ~6.4/12 from unrelated processes), **not** the real 3-machine lab deployment. `docs/BENCHMARK.md` §0.3 states explicitly: **"The 200k-300k TPS target has NOT been validated."** No thread-count breakdown is recorded for these two runs.

### 5.2 Historical (pre-Aeron / pre-optimization) results — §2, explicitly flagged in the doc as not reflecting the current build

**Original single-node baseline** (§2.1):

| Sender threads | Sent TPS | Processed TPS | Packet loss |
|---|---|---|---|
| 1 | 38,724 ops/s | 229 ops/s | 99.41% |
| 4 | 106,648 ops/s | 229 ops/s | 99.78% |
| 8 | 184,230 ops/s | 224 ops/s | 99.88% |
| 16 | 214,221 ops/s | 162 ops/s | 99.92% |

**Original 3-node Raft cluster** (§2.2):

| Sender threads | Sent TPS | Processed TPS | Packet loss |
|---|---|---|---|
| 1 | 46,410 ops/s | 54 ops/s | 99.88% |
| 4 | 144,856 ops/s | 54 ops/s | 99.96% |
| 8 | 258,241 ops/s | 52 ops/s | 99.98% |
| 16 | 276,612 ops/s | 52 ops/s | 99.98% |

**Post-optimization phased comparison** (§2.4, Raft micro-batching + O(1) WAL):

| Phase | 1-node processed TPS | 3-node processed TPS | Packet loss |
|---|---|---|---|
| Original | 229 ops/s | 54 ops/s | 99.88% |
| Phase 1 (O(1) WAL + lock fixes) | 439 ops/s | 54 ops/s | 98.77% (+91.7% vs baseline) |
| Phase 2 (Raft micro-batching) | 5,045 ops/s | 11,806 ops/s | 64.64% (+21,762% / +218× vs baseline) |

**Rate-paced, zero-loss run** (3-node, 4 sender threads, `TARGET_TPS=5000`, 10s): 4,936 ops/s sent, 4,986 ops/s processed, 49,868 results received, **0.00% packet loss** — the doc attributes this to matching sender rate to processing capacity rather than to the wire format.

### 5.3 What is explicitly absent from the benchmark documentation

None of the following exist anywhere in `docs/BENCHMARK.md` or `docs/HLD.md`:
- **Latency figures** (p50/p99/max/avg) — HLD.md only states generic Aeron-vendor transport characteristics ("<20µs UDP", "<1µs IPC"), which are stated transport properties, not measured results from this system.
- **Payload size figures** — only aggregate WAL *file* sizes (e.g. 384 KB, 19.8 MB) are recorded; no per-message or per-batch byte count is measured.
- **Serialization/deserialization timing** — no ns/encode or ns/decode figure for bincode anywhere.
- **CPU or memory usage** — the only adjacent figure is a load-average mention (6.4/12) cited as a confound for Raft leader flapping, not a formal CPU/memory benchmark.

`scripts/run_benchmark.sh` is a purely **end-to-end, log-based** harness: it counts sent orders by parsing `order-sending`'s WAL file, counts received orders via `wc -l` on `order-receiver`'s log, waits for convergence, and computes sent/received TPS, packet-loss %, duplicate count, and missing `order_id` ranges — all from wall-clock counts over the run duration. It does not instrument internal serialization timing, per-hop latency, payload byte sizes, or CPU/memory.

---

## 6. Summary of Findings

1. **Security mechanism in place**: HMAC-SHA256 message authentication on every channel (order, result, both replay-request hops, monitoring corroboration, Raft control), keyed from `CLUSTER_HMAC_KEY`/`monitoring_HMAC_KEY`, verified before any deserialization.
2. **No encryption/confidentiality exists anywhere** in the system — all order and result data travels in cleartext.
3. **Binary protocol**: `bincode` v1.3.3, default fixed-width/positional configuration, used for all high-frequency inter-service messages; the one JSON payload (`CorroborationMsg`) and one hand-rolled byte encoding (`Role::as_u8`) are both confined to low-frequency control/health traffic.
4. **Performance benefit of the binary format** is demonstrable structurally (fixed-size, allocation-free `OrderWire`; no field-name overhead; deterministic pre-computable size for batch budgeting) but is **not quantified by any benchmark in this repository** — no bincode-vs-JSON timing, no payload-size, no latency, and no CPU/memory measurement exists.
5. **Benchmark results that do exist** are end-to-end TPS and packet-loss numbers only, gathered via log-file counting, not the AI's estimation — and the repository's own documentation states the 200k–300k TPS target has not been validated.
