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

---

## 5. Security & HMAC Authentication

- **Rule**: All inbound network payloads across Aeron streams must pass HMAC verification via `auth::verify` before bincode deserialization.
- **Key Storage**: Keys MUST be loaded from environment variables (`CLUSTER_HMAC_KEY` and `WITNESS_HMAC_KEY`) via `auth::cluster_key()`. Hardcoding HMAC keys is strictly forbidden.
