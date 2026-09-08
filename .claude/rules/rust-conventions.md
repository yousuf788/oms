# Rule: Rust Coding Conventions & Performance Standards (200k–300k TPS Target)

This rule specifies mandatory coding standards, performance rules, and memory patterns for all Rust code in this workspace to sustain a target of **200,000 to 300,000 orders/sec (200k–300k TPS)**.

---

## 1. High-Throughput Performance Constraints (200k–300k TPS Target)

- **Zero Allocation Hot Paths**: Pre-allocate `Vec` and `HashSet` capacities (e.g. `Vec::with_capacity(20_000)`). Reuse container allocations in long-running processing loops rather than allocating/deallocating per order batch.
- **Lock Minimization**: Minimize `Mutex` and `RwLock` guard retention in consensus and replication loops. Never hold state locks during blocking socket or Aeron I/O.
- **Lock-Free Queues**: Use `crossbeam_channel::bounded` for high-velocity thread handoffs (e.g., between Aeron polling threads and hot Raft loops).

---

## 2. Positional Binary Serialization (`bincode`) Synchronization

> [!CAUTION]
> `bincode` (v1.3) serializes struct fields **positionally** in declaration order. Struct field names are ignored by the binary encoder and decoder.

- **Rule**: Whenever modifying wire structs in any microservice, you MUST update the corresponding struct in the peer service to keep field declaration order, types, and count identical.
- **Pairs to sync**:
  - `OrderWire` in `order-sending/src/main.rs` <===> `OrderWire` in `order-process/src/main.rs`
  - `ReplicatedCommand` in `order-process/src/wal.rs` <===> `ResultWire` in `order-receiver/src/main.rs`
  - `ReplayRequest` in `order-sending/src/replay.rs` <===> `ReplayRequest` in `order-process/src/replay_client.rs` (S1<->S2 hop)
  - `ReplayRequest` in `order-process/src/replay_server.rs` <===> `ReplayRequest` in `order-receiver/src/replay_client.rs` (S2<->S3 hop)

```rust
// Example: OrderWire definition MUST match on both S1 and S2
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct OrderWire {
    pub order_id: u64,
    pub symbol: u8,
    pub side: bool,
    pub qty: u32,
    pub ts_ms: u64,
}
```

---

## 3. High-Throughput Async File Logging

- **Rule**: Never invoke synchronous `File::open`, `write_all`, `flush`, or `println!` on individual orders inside hot order processing loops.
- **Pattern**: Push log records to an asynchronous `mpsc::sync_channel` (capacity $\ge 1,000,000$). Use a dedicated background worker thread that buffers lines in memory (e.g., `String::with_capacity(128 * 1024)`) and flushes to disk only when:
  1. Accumulated buffer size reaches $64\text{ KB}$, or
  2. Channel has been idle for $50\text{ ms}$.

---

## 4. Aeron Transport & Poll Idle Strategies

- **Polling Loop**: Aeron subscription polling threads must process incoming fragment batches with `poll_fn`.
- **Idle Strategy**: Use `BackoffIdleStrategy::new()` or `BusySpinIdleStrategy::default()` to manage CPU spin during zero-fragment poll cycles.
- **Backpressure Handling**: Cap `offer()` retry counts (e.g., `MAX_BACKPRESSURE_RETRIES = 100_000`) so slow subscribers do not permanently freeze the publisher thread pool.
- **Fan-out to multiple peers must never block on one**: when publishing the same payload to several nodes (e.g. `order-sending`'s per-node channels), use non-blocking `try_send`/skip semantics per destination, not a blocking send in a loop over destinations — a blocking send to one struggling destination otherwise stalls delivery to every other destination behind it in the loop. This is only safe to do because a skipped destination is recoverable via the replay protocol (see `architecture.md` §3) — don't apply this pattern somewhere that has no such recovery path.
- **A dedup/sequence-tracking gate before a possibly-lossy hand-off must block, not drop**: if a channel send follows a `SequenceTracker::mark()` (or any other "have I seen this" gate) that would falsely believe the item was handled, that send must be blocking, not `try_send` — a silent drop after marking makes the loss permanently invisible to gap detection.

---

## 5. Security & HMAC Authentication

- **Rule**: All inbound network payloads across Aeron streams — order channel, result channel, AND both REPLAY_REQUEST control channels — must pass HMAC verification via `auth::verify` before bincode deserialization. `order-receiver` carries its own `auth.rs` (`CLUSTER_HMAC_KEY`) for exactly this reason — it did not need one before the result channel and replay protocol were added.
- **Key Storage**: Keys MUST be loaded from environment variables (`CLUSTER_HMAC_KEY` and `monitoring_HMAC_KEY`) via `auth::cluster_key()`. Hardcoding HMAC keys is strictly forbidden.
