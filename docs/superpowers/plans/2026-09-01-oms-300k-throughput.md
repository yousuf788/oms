> **Status (2026-09-03)**: historical planning document — this plan's tasks were implemented
> (see `docs/BENCHMARK.md` for the current build's benchmark history). Kept as a record, not a
> live plan; it predates the sequencing/gap-detection/replay protocol (`docs/HLD.md` §7) and the
> 300k orders/sec target has still not been validated on real lab hardware (`docs/BENCHMARK.md`
> §0 — only a single-machine simulation has been measured so far).

# OMS 300k Orders/Sec Throughput Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scale the OMS pipeline (`order-sending` → `order-process` 3-node Raft cluster → `order-receiver`) to prove 300,000 orders/sec sustained on all three legs (sent, processed, received) with zero data loss, on the real 3-machine lab deployment.

**Architecture:** Switch every wire format (order messages, Raft log entries/control messages, WAL records, results) from JSON to `bincode`; fan `order-sending`'s publishing out across 3 dedicated per-node threads instead of one serialized thread; fix a bug where `order-process` silently drops a committed batch's result if consensus takes longer than 1500ms, by decoupling S3 delivery into its own background thread that streams from the WAL as entries commit; fix `order-receiver`'s per-message synchronous file I/O (the actual throughput ceiling today) with the same async batched-writer pattern `order-sending` already has.

**Tech Stack:** Rust (2021 edition), `rusteron-client` 0.2 (Aeron bindings), `bincode` 1.3, `socket2` 0.5, `serde`.

**Spec:** `docs/superpowers/specs/2026-09-01-oms-300k-throughput-design.md`

## Global Constraints

- WAL durability stays OS-buffered (no `fsync` added) — matches the spec's explicit decision and the codebase's current behavior.
- This is a bounded benchmark/proof run, not an indefinite production duty cycle — `order-receiver`'s dedup `HashSet<u64>` stays unbounded; no WAL rotation is added.
- Every duplicated wire struct (the same shape declared independently in two crates, since these are three independent Cargo crates with no shared library) MUST carry a `KEEP IN SYNC WITH <path>` doc comment, because `bincode` encodes struct fields **positionally** (by declaration order and type, not by field name) — a field reordered on only one side silently misdecodes the other side's data instead of erroring.
- `AeronExclusivePublication` (used by `order-sending` today) has no `Send` impl in `rusteron-client` 0.2.5 and cannot be moved to another thread. `AeronPublication` (the non-exclusive type, already used by `order-process`/`order-receiver`) is unconditionally `Send`. Every task that needs to move a publication into a dedicated thread uses `async_add_publication`, never `async_add_exclusive_publication`.
- `order-process`'s Raft control-channel `Message` results are reused directly as the S3 wire payload where the shapes already match (`ReplicatedCommand`) — do not invent a separate, differently-shaped result type; that would just be more code to keep in sync for no benefit.
- No unit-test harness exists in this codebase today (`grep` for `#[test]` across all three crates returns nothing). Pure-logic changes (WAL framing, the MTU-byte-budget batching function) get real `#[cfg(test)] mod tests` added inline — normal Rust convention, zero new dependencies. Aeron-integration behavior (threads, sockets, publications) has no existing test harness to extend and building one is out of scope for this plan; those changes are verified by `cargo build --release` succeeding plus the end-to-end benchmark run in Task 6, which is the same verification method every existing feature in this codebase already relies on.

---

### Task 1: `order-process` — WAL binary framing

**Files:**
- Modify: `order-process/Cargo.toml`
- Modify: `order-process/src/wal.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `Wal`'s on-disk format is now length-prefixed `bincode` records instead of newline-delimited JSON. Every other `Wal` method signature (`new`, `len`, `last_index`, `last_term`, `get_term_at`, `entries_from`, `entry_at`, `append_leader_entry`, `append_leader_batch`, `append_entries_from_leader`) is unchanged — later tasks depend on none of this task's internals, only on those unchanged signatures.

- [ ] **Step 1: Add the `bincode` dependency**

Edit `order-process/Cargo.toml`, adding this line under `[dependencies]`:

```toml
bincode = "1.3"
```

- [ ] **Step 2: Write the failing tests for framed read/write**

Add to the bottom of `order-process/src/wal.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry(index: u64) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            command: ReplicatedCommand {
                order_id: index,
                symbol: "BTC-USDT".to_string(),
                side: "BUY".to_string(),
                qty: 5,
                status: "FILLED".to_string(),
                filled_qty: 5,
                processed_by: "Nitin (S2-1)".to_string(),
                term: 1,
            },
        }
    }

    #[test]
    fn framed_entries_round_trip() {
        let mut buf = Vec::new();
        write_framed_entry(&mut buf, &sample_entry(7)).unwrap();
        write_framed_entry(&mut buf, &sample_entry(8)).unwrap();

        let decoded = read_framed_entries(&buf);

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].index, 7);
        assert_eq!(decoded[0].command.order_id, 7);
        assert_eq!(decoded[1].index, 8);
    }

    #[test]
    fn read_framed_entries_stops_at_truncated_trailing_record() {
        let mut buf = Vec::new();
        write_framed_entry(&mut buf, &sample_entry(1)).unwrap();
        // Simulate a crash mid-write of a second record: a length header with
        // no body behind it.
        buf.extend_from_slice(&999u32.to_le_bytes());

        let decoded = read_framed_entries(&buf);

        assert_eq!(decoded.len(), 1, "must not panic or return garbage for a truncated trailing record");
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail (functions don't exist yet)**

Run: `cd order-process && cargo test --lib wal::tests`
Expected: FAIL with "cannot find function `write_framed_entry`" (and `read_framed_entries`)

- [ ] **Step 4: Implement the framing functions and switch the I/O methods to use them**

In `order-process/src/wal.rs`, add the two new framing helper functions as free
module-level functions, placed **outside and just before** the `impl Wal { ... }`
block (i.e. between `pub struct Wal { ... }` and the line `impl Wal {`) — not
inside the `impl` block. This matters because every call site below invokes
them unqualified (`write_framed_entry(...)`, not `Self::write_framed_entry(...)`
or `Wal::write_framed_entry(...)`), which only compiles for true free
functions, not associated functions declared inside `impl Wal`:

```rust
/// Encodes `entry` as `bincode` and appends it to `buf` behind a 4-byte
/// little-endian length prefix, so multiple records can be concatenated in
/// one file and read back without a text delimiter (binary data isn't safely
/// newline-delimited the way the old JSON-lines format was).
fn write_framed_entry(buf: &mut Vec<u8>, entry: &LogEntry) -> io::Result<()> {
    let encoded = bincode::serialize(entry)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
    buf.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
    buf.extend_from_slice(&encoded);
    Ok(())
}

/// Reads as many complete length-prefixed records as `bytes` contains. Stops
/// (without erroring) at a truncated trailing record — e.g. a length header
/// with no body yet, from a process killed mid-write — since the WAL's
/// durability model is OS-buffered writes, not fsync'd transactions.
fn read_framed_entries(bytes: &[u8]) -> Vec<LogEntry> {
    let mut entries = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + len > bytes.len() {
            break;
        }
        if let Ok(entry) = bincode::deserialize::<LogEntry>(&bytes[pos..pos + len]) {
            entries.push(entry);
        }
        pos += len;
    }
    entries
}
```

Replace `fn load_entries`:

```rust
    fn load_entries(path: &PathBuf) -> io::Result<Vec<LogEntry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(path)?;
        let mut entries = read_framed_entries(&bytes);
        entries.sort_by_key(|entry| entry.index);
        Ok(entries)
    }
```

Replace `fn append_single_entry`:

```rust
    fn append_single_entry(&mut self, entry: &LogEntry) -> io::Result<()> {
        let mut buf = Vec::new();
        write_framed_entry(&mut buf, entry)?;
        self.file.write_all(&buf)?;
        Ok(())
    }
```

Replace `fn rewrite_all`:

```rust
    fn rewrite_all(&mut self) -> io::Result<()> {
        let mut buf = Vec::new();
        for entry in &self.entries {
            write_framed_entry(&mut buf, entry)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.write_all(&buf)?;
        file.flush()?;
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        Ok(())
    }
```

Replace `pub fn append_leader_batch`:

```rust
    pub fn append_leader_batch(
        &mut self,
        term: u64,
        commands: Vec<ReplicatedCommand>,
    ) -> io::Result<Vec<LogEntry>> {
        let mut entries = Vec::with_capacity(commands.len());
        let mut buf = Vec::with_capacity(commands.len() * 96);
        let mut last_idx = self.last_index();
        for command in commands {
            last_idx += 1;
            let entry = LogEntry {
                index: last_idx,
                term,
                command,
            };
            write_framed_entry(&mut buf, &entry)?;
            entries.push(entry);
        }
        self.file.write_all(&buf)?;
        self.entries.extend(entries.clone());
        Ok(entries)
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cd order-process && cargo test --lib wal::tests`
Expected: PASS (2 tests)

- [ ] **Step 6: Full build check**

Run: `cd order-process && cargo build --release 2>&1 | tail -30`
Expected: builds clean (existing on-disk `.log` files from before this change are now unreadable garbage to the new framing — delete any `logs/*.log` / benchmark WAL dirs left over from earlier runs before the next `cargo run`, since this is a breaking on-disk format change, not something the code migrates automatically)

- [ ] **Step 7: Commit**

```bash
git add order-process/Cargo.toml order-process/src/wal.rs
git commit -m "feat(order-process): switch WAL to length-prefixed bincode framing"
```

---

### Task 2: `order-process` — Raft control messages to bincode, MTU-sized replication batches, UDP buffer tuning

**Files:**
- Modify: `order-process/Cargo.toml`
- Modify: `order-process/src/leader_election.rs`

**Interfaces:**
- Consumes: `order-process/src/wal.rs`'s unchanged `LogEntry`/`ReplicatedCommand` types and `Wal` methods (Task 1).
- Produces: `entries_within_budget(entries: Vec<LogEntry>, budget_bytes: usize) -> Vec<LogEntry>` (free function) and `bind_tuned_udp_socket(bind_host: &str, port: u16) -> UdpSocket` (free function) — both used later in this same task, not consumed elsewhere.

- [ ] **Step 1: Add dependencies**

Edit `order-process/Cargo.toml`, adding under `[dependencies]` (alongside the `bincode` line already added in Task 1):

```toml
socket2 = "0.5"
```

- [ ] **Step 2: Write the failing test for the byte-budget batching function**

Add near the top of `order-process/src/leader_election.rs`, right after the existing `const RECV_BUF_SIZE: usize = 65_535;` line, a `#[cfg(test)] mod tests` block (place it at the very end of the file instead, after the closing brace of `impl LeaderElection`, which is the existing convention for test modules in this codebase per Task 1):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    // ReplicatedCommand/LogEntry are already in scope here via `use super::*`
    // — leader_election.rs's own top-of-file `use crate::wal::{LogEntry,
    // ReplicatedCommand, Wal};` is visible to this child module, so no
    // separate import is needed (and adding one would just warn as unused).

    fn sample_entry(index: u64) -> LogEntry {
        LogEntry {
            index,
            term: 1,
            command: ReplicatedCommand {
                order_id: index,
                symbol: "BTC-USDT".to_string(),
                side: "BUY".to_string(),
                qty: 1,
                status: "FILLED".to_string(),
                filled_qty: 1,
                processed_by: "Nitin (S2-1)".to_string(),
                term: 1,
            },
        }
    }

    #[test]
    fn entries_within_budget_stops_before_exceeding_but_always_makes_progress() {
        let entries: Vec<LogEntry> = (1..=1000).map(sample_entry).collect();
        let one_entry_size = bincode::serialized_size(&entries[0]).unwrap() as usize;

        let budget = one_entry_size * 5;
        let batch = entries_within_budget(entries.clone(), budget);
        assert!(!batch.is_empty());
        assert!(
            batch.len() <= 6,
            "expected roughly 5 entries for a 5x-single-entry budget, got {}",
            batch.len()
        );

        let tiny_budget = entries_within_budget(entries, 1);
        assert_eq!(
            tiny_budget.len(),
            1,
            "must always take at least one entry so replication keeps making progress"
        );
    }
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd order-process && cargo test --lib leader_election::tests`
Expected: FAIL with "cannot find function `entries_within_budget`"

- [ ] **Step 4: Implement the byte-budget batching function and the tuned socket helper**

In `order-process/src/leader_election.rs`, replace this line:

```rust
const MAX_ENTRIES_PER_APPEND: usize = 32;
```

with:

```rust
/// Byte budget for a single `AppendEntries` UDP datagram, sized to stay
/// safely under standard Ethernet MTU (1500 bytes) after IP/UDP headers, so
/// replication never silently depends on IP fragmentation (a single lost
/// fragment loses the whole datagram).
const APPEND_BATCH_BYTE_BUDGET: usize = 1400;
```

Add this free function near the top of the file, right after `fn random_timeout()`:

```rust
/// Takes as many `entries` (in order) as fit within `budget_bytes` once
/// bincode-encoded, always taking at least one entry even if it alone
/// exceeds the budget, so replication keeps making progress regardless of
/// how large a single command's payload gets.
fn entries_within_budget(entries: Vec<LogEntry>, budget_bytes: usize) -> Vec<LogEntry> {
    let mut taken = Vec::new();
    let mut total = 0usize;
    for entry in entries {
        let size = bincode::serialized_size(&entry).unwrap_or(0) as usize;
        if !taken.is_empty() && total + size > budget_bytes {
            break;
        }
        total += size;
        taken.push(entry);
    }
    taken
}

/// Binds a UDP socket for the Raft control channel with larger send/receive
/// buffers than the OS default, matching the buffer tuning already applied
/// to Aeron-adjacent traffic (commit 9faf1c7) — at high replication rates
/// this raw socket has no other flow control, so an undersized OS buffer is
/// a real loss point.
fn bind_tuned_udp_socket(bind_host: &str, port: u16) -> UdpSocket {
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::ToSocketAddrs;

    let addr: std::net::SocketAddr = (bind_host, port)
        .to_socket_addrs()
        .expect("resolve raft control bind address")
        .next()
        .expect("no address for raft control bind host/port");
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))
        .expect("create raft control socket");
    socket
        .set_recv_buffer_size(8 * 1024 * 1024)
        .expect("set SO_RCVBUF on raft control socket");
    socket
        .set_send_buffer_size(8 * 1024 * 1024)
        .expect("set SO_SNDBUF on raft control socket");
    socket.bind(&addr.into()).expect("bind raft control socket");
    socket.into()
}
```

Change the `fn send` method's serialization call from:

```rust
    fn send(&self, peer: &S2Node, msg: &Message) {
        if let Ok(buf) = serde_json::to_vec(msg) {
            let _ = self.socket.send_to(&buf, (peer.host.as_str(), peer.raft_port));
        }
    }
```

to:

```rust
    fn send(&self, peer: &S2Node, msg: &Message) {
        if let Ok(buf) = bincode::serialize(msg) {
            let _ = self.socket.send_to(&buf, (peer.host.as_str(), peer.raft_port));
        }
    }
```

Change the `fn recv_loop` method's deserialization call from:

```rust
            let msg: Message = match serde_json::from_slice(&buf[..n]) {
```

to:

```rust
            let msg: Message = match bincode::deserialize(&buf[..n]) {
```

Change the `Message` enum declaration from:

```rust
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum Message {
```

to (dropping `#[serde(tag = "type")]`, which is a JSON-only internally-tagged representation `bincode` can't use — `bincode` encodes a plain enum as a variant index plus payload natively, which is what we want here):

```rust
#[derive(Serialize, Deserialize, Debug)]
enum Message {
```

Change the socket construction in `LeaderElection::start` from:

```rust
        let bind_host = crate::config::config().bind_host.as_str();
        let socket = UdpSocket::bind((bind_host, self_node.raft_port))
            .expect("failed to bind control channel");
```

to:

```rust
        let bind_host = crate::config::config().bind_host.as_str();
        let socket = bind_tuned_udp_socket(bind_host, self_node.raft_port);
```

Finally, change the batch-taking call inside `fn replicate_to_peers` from:

```rust
            let entries = if next_idx <= leader_last {
                wal.entries_from(next_idx)
                    .into_iter()
                    .take(MAX_ENTRIES_PER_APPEND)
                    .collect()
            } else {
                Vec::new()
            };
```

to:

```rust
            let entries = if next_idx <= leader_last {
                entries_within_budget(wal.entries_from(next_idx), APPEND_BATCH_BYTE_BUDGET)
            } else {
                Vec::new()
            };
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd order-process && cargo test --lib leader_election::tests`
Expected: PASS (1 test)

- [ ] **Step 6: Full build check**

Run: `cd order-process && cargo build --release 2>&1 | tail -30`
Expected: builds clean, no leftover `serde_json` reference in this file (the file no longer imports `serde_json` after this change — if `cargo build` reports an unused-import warning for it in this file, remove that `use` line; there should not have been one here in the first place since `serde_json::to_vec`/`from_slice` were called via full paths, not an import)

- [ ] **Step 7: Commit**

```bash
git add order-process/Cargo.toml order-process/src/leader_election.rs
git commit -m "feat(order-process): bincode Raft messages, MTU-sized replication batches, tuned UDP buffers"
```

---

### Task 3: `order-process` — OrderWire decode + decoupled result-publisher thread

This is the task that fixes the silent-drop bug described in the spec: today, `propose_batch()`'s 1500ms timeout gates whether a committed batch's result ever reaches S3. This task moves S3 delivery into an independent background thread that streams from the WAL as entries commit, so no caller's timeout can discard a result that's already durably committed.

**Files:**
- Modify: `order-process/Cargo.toml`
- Modify: `order-process/src/main.rs`
- Modify: `order-process/src/leader_election.rs`

**Interfaces:**
- Consumes: `Wal::entry_at(index: u64) -> Option<LogEntry>` (unchanged, from Task 1); `entries_within_budget`/`bind_tuned_udp_socket` (Task 2, untouched by this task).
- Produces: `LeaderElection::start(self_id: u8, result_pub: AeronPublication) -> Arc<Self>` (signature changed — was `start(self_id: u8)`). Any future caller of `start` must supply the S3 `AeronPublication`.

- [ ] **Step 1: Replace the `Order` struct with `OrderWire` in `order-process/src/main.rs`**

Replace:

```rust
use rand::Rng;
use rusteron_client::*;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
```

with:

```rust
use rand::Rng;
use rusteron_client::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
```

Replace:

```rust
#[derive(Deserialize, Debug)]
struct Order {
    order_id: u64,
    symbol: String,
    side: String,
    qty: u32,
}
```

with:

```rust
/// Wire format for an inbound order from order-sending's Aeron order
/// channel. KEEP IN SYNC WITH order-sending/src/main.rs::OrderWire — bincode
/// encodes struct fields positionally (by declaration order and type, not by
/// field name), so both sides must declare identical field order and types.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct OrderWire {
    order_id: u64,
    symbol: u8,
    side: bool,
    qty: u32,
    #[allow(dead_code)] // received for wire compatibility; not used on this side
    ts_ms: u64,
}

const SYMBOLS: [&str; 3] = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];

impl OrderWire {
    fn symbol_str(&self) -> &'static str {
        SYMBOLS.get(self.symbol as usize).copied().unwrap_or("UNKNOWN")
    }
    fn side_str(&self) -> &'static str {
        if self.side { "BUY" } else { "SELL" }
    }
}
```

- [ ] **Step 2: Switch the subscription decode path and drop the `result_pub` plumbing from the drain loop**

Replace:

```rust
    let seen_ids: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let pending_orders: Arc<Mutex<Vec<Order>>> = Arc::new(Mutex::new(Vec::with_capacity(500)));
```

with:

```rust
    let seen_ids: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let pending_orders: Arc<Mutex<Vec<OrderWire>>> = Arc::new(Mutex::new(Vec::with_capacity(500)));
```

Replace the fragment handler:

```rust
            let fragments = order_subscription
                .poll_fn(move |buf: &[u8], _hdr: AeronHeader| {
                    if let Ok(order) = serde_json::from_slice::<Order>(buf) {
                        let mut pending = pending_orders.lock().unwrap();
                        if pending.len() >= 500 {
                            return; // batch full
                        }
                        let mut seen = seen_ids.lock().unwrap();
                        if seen.insert(order.order_id) {
                            pending.push(order);
                        }
                    }
                }, 500)
                .unwrap_or(0);
```

with:

```rust
            let fragments = order_subscription
                .poll_fn(move |buf: &[u8], _hdr: AeronHeader| {
                    if let Ok(order) = bincode::deserialize::<OrderWire>(buf) {
                        let mut pending = pending_orders.lock().unwrap();
                        if pending.len() >= 500 {
                            return; // batch full
                        }
                        let mut seen = seen_ids.lock().unwrap();
                        if seen.insert(order.order_id) {
                            pending.push(order);
                        }
                    }
                }, 500)
                .unwrap_or(0);
```

- [ ] **Step 3: Stop creating `Arc<AeronPublication>` in `main()` — the publication is now moved into `LeaderElection`**

Replace:

```rust
    let result_pub = aeron
        .async_add_publication(&result_channel_cstr, RESULT_STREAM_ID)
        .expect("async_add_publication (results)")
        .poll_blocking(Duration::from_secs(10))
        .expect("result publication ready");

    let result_pub = Arc::new(result_pub);

    // ── Start Raft election ────────────────────────────────────────────────────
    let election = LeaderElection::start(node_id);
```

with:

```rust
    let result_pub = aeron
        .async_add_publication(&result_channel_cstr, RESULT_STREAM_ID)
        .expect("async_add_publication (results)")
        .poll_blocking(Duration::from_secs(10))
        .expect("result publication ready");

    // ── Start Raft election ────────────────────────────────────────────────────
    // LeaderElection takes ownership of result_pub and moves it into its own
    // background publisher thread — S3 delivery is now decoupled entirely
    // from this main loop (see leader_election.rs::result_publisher_loop).
    let election = LeaderElection::start(node_id, result_pub);
```

- [ ] **Step 4: Simplify `process_orders_batch_as_leader` — it no longer builds or sends results**

Replace the call site:

```rust
        if election.is_leader() {
            process_orders_batch_as_leader(
                node_id,
                &orders,
                &election,
                &result_pub,
            );
        }
```

with:

```rust
        if election.is_leader() {
            process_orders_batch_as_leader(node_id, &orders, &election);
        }
```

Replace the entire `process_orders_batch_as_leader` function with:

```rust
fn process_orders_batch_as_leader(
    node_id: u8,
    orders: &[OrderWire],
    election: &LeaderElection,
) {
    let leader = node_name(node_id);
    let current_term = election.current_term();
    let outcomes = ["FILLED", "PARTIALLY_FILLED", "REJECTED"];
    let mut rng = rand::thread_rng();

    let commands: Vec<ReplicatedCommand> = orders
        .iter()
        .map(|order| {
            let status = outcomes[rng.gen_range(0..outcomes.len())];
            let filled_qty: u32 = if status == "REJECTED" {
                0
            } else {
                rng.gen_range(1..=order.qty)
            };
            ReplicatedCommand {
                order_id: order.order_id,
                symbol: order.symbol_str().to_string(),
                side: order.side_str().to_string(),
                qty: order.qty,
                status: status.to_string(),
                filled_qty,
                processed_by: format!("{} (S2-{})", leader, node_id),
                term: current_term,
            }
        })
        .collect();

    // Result delivery to S3 is handled asynchronously by LeaderElection's
    // background publisher thread once each entry commits (see
    // leader_election.rs::result_publisher_loop) — this call's return value
    // is used only as a flow-control signal here, never to gate delivery.
    election.propose_batch(commands);
}
```

- [ ] **Step 5: Remove the now-unused `serde_json` dependency**

Check no other file in `order-process/src/` still references `serde_json` (Tasks 1 and 2 already removed its uses from `wal.rs` and `leader_election.rs`):

Run: `cd order-process && grep -rn "serde_json" src/`
Expected: no output

Remove this line from `order-process/Cargo.toml`:

```toml
serde_json = "1"
```

- [ ] **Step 6: Add the decoupled result-publisher thread in `leader_election.rs`**

Add this import near the top of `order-process/src/leader_election.rs`, alongside the existing `use` block:

```rust
use rusteron_client::{AeronPublication, BusySpinIdleStrategy};
```

Change `LeaderElection::start`'s signature and body. Replace:

```rust
    pub fn start(self_id: u8) -> Arc<Self> {
```

with:

```rust
    pub fn start(self_id: u8, result_pub: AeronPublication) -> Arc<Self> {
```

Replace the thread-spawning block at the end of `start` (right before the final `election` return):

```rust
        let recv_handle = Arc::clone(&election);
        thread::spawn(move || recv_handle.recv_loop());

        let tick_handle = Arc::clone(&election);
        thread::spawn(move || tick_handle.tick_loop());

        election
```

with:

```rust
        let recv_handle = Arc::clone(&election);
        thread::spawn(move || recv_handle.recv_loop());

        let tick_handle = Arc::clone(&election);
        thread::spawn(move || tick_handle.tick_loop());

        let publish_handle = Arc::clone(&election);
        thread::spawn(move || publish_handle.result_publisher_loop(result_pub));

        election
```

Add this new method to `impl LeaderElection` (place it right after `fn become_leader_locked`):

```rust
    /// Watches `last_applied` and streams each newly-committed entry's result
    /// to S3 as soon as it commits, independent of which (if any)
    /// `propose_batch()` call originally proposed it. This is what fixes the
    /// bug where `propose_batch()`'s bounded wait timing out used to mean the
    /// result for that specific batch was never sent to S3, even though the
    /// entry was already correctly committed in the WAL.
    ///
    /// Only publishes while this node holds leadership. On losing leadership
    /// the watermark fast-forwards to the current `last_applied` (not reset
    /// to 0), so a later re-election doesn't republish old history it
    /// already sent in an earlier tenure.
    ///
    /// Note: `last_published` starts at 0 for a freshly started process. If
    /// this node's on-disk WAL already contains committed entries from a
    /// previous run the first time it becomes leader in this process's
    /// lifetime, those get republished once. This is harmless — S3
    /// (`order-receiver`) already deduplicates by `order_id` — and is
    /// accepted as simpler than tracking a persisted publish watermark
    /// across restarts, which this benchmark-proof scope doesn't need.
    fn result_publisher_loop(&self, result_pub: AeronPublication) {
        let mut idle = BusySpinIdleStrategy::default();
        let mut last_published: u64 = 0;
        loop {
            if !self.is_leader() {
                let last_applied = self.state.lock().unwrap().last_applied;
                last_published = last_applied;
                thread::sleep(Duration::from_millis(5));
                continue;
            }

            let last_applied = self.state.lock().unwrap().last_applied;
            if last_published >= last_applied {
                thread::sleep(Duration::from_millis(1));
                continue;
            }

            let next = last_published + 1;
            let entry = { self.wal.lock().unwrap().entry_at(next) };
            match entry {
                Some(entry) => {
                    if let Ok(bytes) = bincode::serialize(&entry.command) {
                        loop {
                            match result_pub.offer(&bytes) {
                                Ok(_) => break,
                                Err(e) if e.is_retryable() => {
                                    idle.idle(0);
                                    continue;
                                }
                                Err(e) => {
                                    eprintln!(
                                        "[S2-{}] result publish error: {e}",
                                        self.self_id
                                    );
                                    break;
                                }
                            }
                        }
                        if verbose_raft() {
                            println!(
                                "[order] {} LEADER committed order_id={} status={} filled={}/{}",
                                node_name(self.self_id), entry.command.order_id,
                                entry.command.status, entry.command.filled_qty,
                                entry.command.qty,
                            );
                        }
                    }
                    last_published = next;
                }
                None => {
                    // Not yet visible in this thread's WAL snapshot (race
                    // with the writer) - retry shortly rather than skipping.
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }
```

- [ ] **Step 7: Full build check**

Run: `cd order-process && cargo build --release 2>&1 | tail -40`
Expected: builds clean

- [ ] **Step 8: Commit**

```bash
git add order-process/Cargo.toml order-process/src/main.rs order-process/src/leader_election.rs
git commit -m "feat(order-process): decode orders via bincode, decouple S3 result delivery from propose_batch timeout"
```

---

### Task 4: `order-sending` — OrderWire encode + parallel per-node publisher fan-out

**Files:**
- Modify: `order-sending/Cargo.toml`
- Modify: `order-sending/src/main.rs`

**Interfaces:**
- Consumes: nothing from other tasks (order-sending is a fully independent crate).
- Produces: `OrderWire` struct (must stay byte-for-byte positionally compatible with `order-process/src/main.rs::OrderWire` from Task 3 — same field order, same types).

- [ ] **Step 1: Add `bincode`, remove the unused `serde_json`**

Edit `order-sending/Cargo.toml`. Remove:

```toml
serde_json = "1"
```

(confirm it was never actually imported first: `cd order-sending && grep -n "serde_json" src/main.rs` should print nothing — the original JSON payload was hand-built with `format!`, never via the `serde_json` crate)

Add under `[dependencies]`:

```toml
serde = { version = "1", features = ["derive"] }
bincode = "1.3"
```

- [ ] **Step 2: Replace the whole file content**

The changes touch imports, the wire type, the channel types, the publication-creation loop, the generator threads, and the log writer together closely enough that a full-file replacement is clearer than a sequence of partial edits. Replace the entire content of `order-sending/src/main.rs` with:

```rust
// order-sending (S1) — Aeron high-throughput publisher
// Architecture:
//   - N generator threads produce OrderWire values → bounded channel
//   - 1 fan-out thread serializes once and distributes to 3 dedicated
//     per-node publisher threads (one AeronPublication each), so a slow or
//     backpressured node no longer stalls delivery to the other two
//   - Backpressure: if a node's publisher thread is slow, its channel fills
//     and the fan-out thread blocks on that node's send() — no silent drops
// Rate-paced to TARGET_TPS orders/sec (default 5000).

mod config;

use config::init_config;
use rand::Rng;
use rusteron_client::*;
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

const ORDER_STREAM_ID: i32 = 1001;

/// Wire format sent to order-process's Aeron order channel. KEEP IN SYNC
/// WITH order-process/src/main.rs::OrderWire — bincode encodes struct fields
/// positionally (by declaration order and type, not by field name), so both
/// sides must declare identical field order and types.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
struct OrderWire {
    order_id: u64,
    symbol: u8,
    side: bool,
    qty: u32,
    ts_ms: u64,
}

const SYMBOLS: [&str; 3] = ["BTC-USDT", "ETH-USDT", "SOL-USDT"];

fn sent_log_path() -> PathBuf {
    PathBuf::from("logs").join("orders-sent.log")
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn aeron_dir() -> String {
    std::env::var("AERON_DIR").unwrap_or_else(|_| {
        #[cfg(target_os = "linux")]
        unsafe {
            extern "C" { fn getuid() -> u32; }
            format!("/dev/shm/aeron-{}", getuid())
        }
        #[cfg(not(target_os = "linux"))]
        "/dev/shm/aeron-0".to_string()
    })
}

fn main() {
    let cfg = init_config();

    let target_tps: u64 = std::env::var("TARGET_TPS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(5000);
    let num_gen_threads: usize = std::env::var("SENDER_THREADS")
        .ok().and_then(|v| v.parse().ok()).unwrap_or(8);
    let channel_capacity = target_tps.max(10000) as usize;

    // ── Connect to Aeron Media Driver ──────────────────────────────────────────
    let aeron_dir_path = aeron_dir();
    println!("[order-sending] connecting to Aeron Media Driver at {aeron_dir_path}");

    let ctx = AeronContext::new().expect("Aeron context");
    let aeron_dir_cstr = CString::new(aeron_dir_path).unwrap();
    ctx.set_dir(&aeron_dir_cstr).expect("set aeron dir");
    ctx.set_error_handler(Some(|code: i32, msg: &str| {
        eprintln!("[aeron] error {code}: {msg}");
    })).expect("set error handler");

    let aeron = Aeron::new(&ctx).expect("Aeron client");
    aeron.start().expect("start aeron client");

    // ── Shared counters ────────────────────────────────────────────────────────
    let order_counter = Arc::new(AtomicU64::new(1));
    let sent_total = Arc::new(AtomicU64::new(0));

    // ── Background log writer (buffers order_ids, flushed on 64KB or 50ms idle) ─
    let (log_tx, log_rx) = mpsc::sync_channel::<u64>(1_000_000);
    {
        let path = sent_log_path();
        thread::spawn(move || {
            if let Some(parent) = path.parent() { let _ = create_dir_all(parent); }
            let mut file = OpenOptions::new().create(true).append(true).open(&path)
                .expect("cannot open orders-sent.log");
            let mut buf = String::with_capacity(128 * 1024);
            loop {
                match log_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(order_id) => {
                        buf.push_str(&order_id.to_string()); buf.push('\n');
                        if buf.len() >= 65536 {
                            let _ = file.write_all(buf.as_bytes());
                            let _ = file.flush();
                            buf.clear();
                        }
                    }
                    Err(_) => {
                        if !buf.is_empty() {
                            let _ = file.write_all(buf.as_bytes());
                            let _ = file.flush();
                            buf.clear();
                        }
                    }
                }
            }
        });
    }

    // ── Stats thread ───────────────────────────────────────────────────────────
    {
        let sent_total = Arc::clone(&sent_total);
        thread::spawn(move || {
            let mut last = 0u64;
            loop {
                thread::sleep(Duration::from_secs(1));
                let now = sent_total.load(Ordering::Relaxed);
                println!("[order-sending] throughput: {:>8} orders/sec  total: {}", now - last, now);
                last = now;
            }
        });
    }

    // ── Create Aeron publications — one per S2 node (unicast). Non-exclusive
    // (AeronPublication, not AeronExclusivePublication) because only the
    // non-exclusive type is Send — each is moved into its own dedicated
    // publisher thread below. ──────────────────────────────────────────────
    let mut node_channels: Vec<mpsc::SyncSender<Arc<Vec<u8>>>> = Vec::new();
    for (i, node) in cfg.nodes.iter().enumerate() {
        let ch = format!("aeron:udp?endpoint={}:{}", node.host, node.order_port);
        println!("[order-sending] publication[{i}] → {ch} stream {ORDER_STREAM_ID}");
        let ch_cstr = CString::new(ch).unwrap();
        let pub_ = aeron
            .async_add_publication(&ch_cstr, ORDER_STREAM_ID)
            .unwrap_or_else(|e| panic!("add_publication node {}: {e}", i + 1))
            .poll_blocking(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("poll_publication node {}: {e}", i + 1));

        let (node_tx, node_rx) = mpsc::sync_channel::<Arc<Vec<u8>>>(channel_capacity);
        node_channels.push(node_tx);
        thread::spawn(move || {
            let mut idle = BusySpinIdleStrategy::default();
            loop {
                match node_rx.recv() {
                    Ok(payload) => loop {
                        match pub_.offer(payload.as_slice()) {
                            Ok(_) => break,
                            Err(e) if e.is_retryable() => { idle.idle(0); continue; }
                            Err(e) => {
                                eprintln!("[order-sending] publish error (node {}): {e}", i + 1);
                                break;
                            }
                        }
                    },
                    Err(_) => break, // fan-out thread exited
                }
            }
        });
    }

    // ── Channel: generator threads → fan-out thread ──────────────────────────
    let (payload_tx, payload_rx) = mpsc::sync_channel::<OrderWire>(channel_capacity);

    // ── Fan-out thread: serialize once, distribute to all 3 per-node
    // publisher threads. Blocking send() on each node channel preserves
    // "no silent drops" — a full channel means that node's own offer() retry
    // loop is backpressured, and we wait rather than lose the order. ────────
    let fanout_handle = {
        let log_tx = log_tx.clone();
        thread::spawn(move || {
            loop {
                match payload_rx.recv() {
                    Ok(order) => {
                        let Ok(bytes) = bincode::serialize(&order) else { continue };
                        let bytes = Arc::new(bytes);
                        for node_tx in &node_channels {
                            if node_tx.send(Arc::clone(&bytes)).is_err() {
                                return; // that node's publisher thread exited
                            }
                        }
                        let _ = log_tx.try_send(order.order_id);
                    }
                    Err(_) => break, // all generator threads exited
                }
            }
        })
    };

    // ── Rate pacing ────────────────────────────────────────────────────────────
    let per_thread_tps = if target_tps > 0 {
        (target_tps as f64 / num_gen_threads as f64).max(0.1)
    } else { 0.0 };
    let nanos_per_order = if per_thread_tps > 0.0 {
        (1_000_000_000.0 / per_thread_tps) as u64
    } else { 0 };

    println!(
        "[order-sending] starting {num_gen_threads} generator threads → {target_tps} orders/sec"
    );

    // ── Generator threads: build OrderWire values ───────────────────────────────
    for _tid in 0..num_gen_threads {
        let order_counter = Arc::clone(&order_counter);
        let sent_total = Arc::clone(&sent_total);
        let payload_tx = payload_tx.clone();
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let thread_start = Instant::now();
            let mut order_index = 0u64;
            loop {
                let order_id = order_counter.fetch_add(1, Ordering::Relaxed);
                let symbol = rng.gen_range(0..SYMBOLS.len() as u8);
                let side = rng.gen_bool(0.5);
                let qty: u32 = rng.gen_range(1..=10);
                let ts_ms = now_ms() as u64;
                let order = OrderWire { order_id, symbol, side, qty, ts_ms };

                // Send to fan-out thread (blocks if it's busy → backpressure)
                if payload_tx.send(order).is_err() {
                    break; // fan-out thread exited
                }
                sent_total.fetch_add(1, Ordering::Relaxed);

                // Rate pacing
                order_index += 1;
                if nanos_per_order > 0 {
                    let expected = Duration::from_nanos(order_index * nanos_per_order);
                    let actual = thread_start.elapsed();
                    if expected > actual {
                        thread::sleep(expected - actual);
                    }
                }
            }
        });
    }

    // Generator threads and publisher threads run until killed; block here.
    let _ = fanout_handle.join();
}
```

- [ ] **Step 3: Full build check**

Run: `cd order-sending && cargo build --release 2>&1 | tail -40`
Expected: builds clean (a `symbol`/`SYMBOLS` unused-index-type warning is not expected since `rng.gen_range(0..SYMBOLS.len() as u8)` already produces a `u8`)

- [ ] **Step 4: Commit**

```bash
git add order-sending/Cargo.toml order-sending/src/main.rs
git commit -m "feat(order-sending): bincode OrderWire + parallel per-node publisher fan-out"
```

---

### Task 5: `order-receiver` — ResultWire decode + async batched log writer

**Files:**
- Modify: `order-receiver/Cargo.toml`
- Modify: `order-receiver/src/main.rs`

**Interfaces:**
- Consumes: the wire shape produced by `order-process`'s `result_publisher_loop` (Task 3), which serializes `wal::ReplicatedCommand` directly with `bincode` — field order `order_id: u64, symbol: String, side: String, qty: u32, status: String, filled_qty: u32, processed_by: String, term: u64`.
- Produces: nothing consumed elsewhere (order-receiver is the end of the pipeline).

- [ ] **Step 1: Add `bincode`, remove the now-unused `serde_json`**

Confirm `serde_json` has no remaining use once Step 2 below lands: `cd order-receiver && grep -n "serde_json" src/main.rs` — the only current use is `serde_json::from_slice::<Value>` in the poll loop, which Step 2 replaces with `bincode::deserialize::<ResultWire>`.

Edit `order-receiver/Cargo.toml`. Remove:

```toml
serde_json = "1"
```

Add under `[dependencies]`:

```toml
serde = { version = "1", features = ["derive"] }
bincode = "1.3"
```

- [ ] **Step 2: Replace the whole file content**

Replace the entire content of `order-receiver/src/main.rs` with:

```rust
// order-receiver (S3) — Aeron subscriber
// Subscribes to the result channel that the S2 leader publishes to.
// Deduplicates by order_id (handles leader failover duplicates).

mod config;

use config::init_config;
use rusteron_client::*;
use serde::Deserialize;
use std::collections::HashSet;
use std::ffi::CString;
use std::fs::{create_dir_all, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const RESULT_STREAM_ID: i32 = 2001;

/// Wire format for a committed result from order-process's Aeron result
/// channel. KEEP IN SYNC WITH order-process/src/wal.rs::ReplicatedCommand —
/// order-process serializes that struct directly as the wire payload, and
/// bincode decodes positionally (by declaration order and type, not by
/// field name), so the field order/types here must match it exactly.
#[derive(Deserialize, Debug, Clone)]
struct ResultWire {
    order_id: u64,
    symbol: String,
    side: String,
    qty: u32,
    status: String,
    filled_qty: u32,
    processed_by: String,
    term: u64,
}

fn received_log_path() -> PathBuf {
    PathBuf::from("logs").join("orders-received.log")
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn aeron_dir() -> String {
    std::env::var("AERON_DIR")
        .unwrap_or_else(|_| {
            #[cfg(target_os = "linux")]
            unsafe {
                extern "C" { fn getuid() -> u32; }
                format!("/dev/shm/aeron-{}", getuid())
            }
            #[cfg(not(target_os = "linux"))]
            "/dev/shm/aeron-0".to_string()
        })
}

fn main() {
    let cfg = init_config();

    // ── Connect to Aeron Media Driver ──────────────────────────────────────────
    let aeron_dir_path = aeron_dir();
    println!("[order-receiver] connecting to Aeron Media Driver at {aeron_dir_path}");

    let ctx = AeronContext::new().expect("Aeron context");
    let aeron_dir_cstr = CString::new(aeron_dir_path).unwrap();
    ctx.set_dir(&aeron_dir_cstr).expect("set aeron dir");
    ctx.set_error_handler(Some(|code: i32, msg: &str| {
        eprintln!("[aeron] error {code}: {msg}");
    })).expect("set error handler");

    let aeron = Aeron::new(&ctx).expect("Aeron client");
    aeron.start().expect("start aeron client");

    // ── Subscribe to result channel ────────────────────────────────────────────
    // S2 leader publishes to our host:port, so we subscribe on our own endpoint.
    let channel = format!("aeron:udp?endpoint={}:{}", cfg.bind_host, cfg.bind_port);
    println!(
        "[order-receiver] subscribing on {channel} stream {RESULT_STREAM_ID}, writing to {}",
        received_log_path().display()
    );
    let channel_cstr = CString::new(channel).unwrap();
    let subscription = aeron
        .async_add_subscription(
            &channel_cstr,
            RESULT_STREAM_ID,
            Handlers::NONE,
            Handlers::NONE,
        )
        .expect("async_add_subscription")
        .poll_blocking(Duration::from_secs(10))
        .expect("subscription ready");

    // ── Background log writer (buffers text lines, flushed on 64KB or 50ms
    // idle) — the receiver previously opened, wrote, and flushed the log
    // file on every single message, which was the actual throughput ceiling
    // for this service. This mirrors order-sending's writer thread exactly. ──
    let (log_tx, log_rx) = mpsc::sync_channel::<String>(1_000_000);
    {
        let path = received_log_path();
        thread::spawn(move || {
            if let Some(parent) = path.parent() { let _ = create_dir_all(parent); }
            let mut file = OpenOptions::new().create(true).append(true).open(&path)
                .expect("cannot open orders-received.log");
            let mut buf = String::with_capacity(128 * 1024);
            loop {
                match log_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(line) => {
                        buf.push_str(&line); buf.push('\n');
                        if buf.len() >= 65536 {
                            let _ = file.write_all(buf.as_bytes());
                            let _ = file.flush();
                            buf.clear();
                        }
                    }
                    Err(_) => {
                        if !buf.is_empty() {
                            let _ = file.write_all(buf.as_bytes());
                            let _ = file.flush();
                            buf.clear();
                        }
                    }
                }
            }
        });
    }

    // ── Stats thread (replaces the old per-message println!, which at high
    // throughput would itself become a bottleneck via stdout's internal lock) ─
    let received_total = Arc::new(AtomicU64::new(0));
    {
        let received_total = Arc::clone(&received_total);
        thread::spawn(move || {
            let mut last = 0u64;
            loop {
                thread::sleep(Duration::from_secs(1));
                let now = received_total.load(Ordering::Relaxed);
                println!("[order-receiver] throughput: {:>8} results/sec  total: {}", now - last, now);
                last = now;
            }
        });
    }

    // ── Poll loop ──────────────────────────────────────────────────────────────
    let mut seen_order_ids: HashSet<u64> = HashSet::new();
    let mut idle = BackoffIdleStrategy::new();

    println!("[order-receiver] ready, polling for results...");
    loop {
        let fragments = subscription
            .poll_fn(|buf: &[u8], _hdr: AeronHeader| {
                if let Ok(result) = bincode::deserialize::<ResultWire>(buf) {
                    if !seen_order_ids.insert(result.order_id) {
                        return; // deduplicate
                    }
                    let received_ts_ms = now_ms();
                    let line = format!(
                        "{} {} {} {} {} {} {} {} {}",
                        result.order_id, result.symbol, result.side, result.qty,
                        result.status, result.filled_qty, result.processed_by,
                        result.term, received_ts_ms,
                    );
                    let _ = log_tx.try_send(line);
                    received_total.fetch_add(1, Ordering::Relaxed);
                }
            }, 256)
            .unwrap_or(0);

        idle.idle(fragments);
    }
}
```

- [ ] **Step 3: Full build check**

Run: `cd order-receiver && cargo build --release 2>&1 | tail -40`
Expected: builds clean

- [ ] **Step 4: Commit**

```bash
git add order-receiver/Cargo.toml order-receiver/src/main.rs
git commit -m "feat(order-receiver): bincode ResultWire decode + async batched log writer, replace per-message println with stats thread"
```

---

### Task 6: Verification — 300k/sec run on the real 3-machine lab deployment

**Files:**
- Create: `scripts/run_lab_benchmark.md` (a runbook, not a script — see rationale below)

**Interfaces:**
- Consumes: the built `order-sending`, `order-process`, `order-receiver` release binaries from Tasks 1-5, plus the existing `starter.sh` / `start-media-driver.sh` / real per-machine `.env` files already in the repo.
- Produces: nothing consumed by other tasks — this is the terminal verification step.

Why a runbook instead of a script: `scripts/run_benchmark.sh` (existing) deliberately runs all 3 services as localhost subprocesses of one script on one machine, with its own isolated ports/env — that's the wrong topology for this target (the spec requires the real 3-physical-machine deployment, each running its own `.env` and its own `starter.sh`/binary independently). There is no existing single entry point that launches across 3 separate machines (doing so would need SSH orchestration, which is a bigger, separate concern this plan's spec explicitly didn't scope in — see the spec's "Out of scope" section). A runbook documents the exact manual steps precisely instead.

- [ ] **Step 1: Write the runbook**

Create `scripts/run_lab_benchmark.md`:

```markdown
# 300k orders/sec lab benchmark runbook

Run on the real 3-machine deployment (Nitin / Amit / Yousuf, per docs/HLD.md).
Each machine already has its own `order-process/.env` (real IPs) from prior
setup — do not use `.env.example` or `run_benchmark.sh`'s isolated ports for
this run.

## 1. Rebuild release binaries on every machine

On **each** of Nitin, Amit, Yousuf:

    cd order-process && cargo build --release
    cd ../order-sending && cargo build --release   # Yousuf only (S1 lives here)
    cd ../order-receiver && cargo build --release  # Yousuf only (S3 lives here)

## 2. Start the Aeron Media Driver on every machine

On **each** of Nitin, Amit, Yousuf:

    ./scripts/start-media-driver.sh

Confirm each one is actually up (not just "started" — see the JDK 17
`--add-opens java.base/sun.nio.ch=ALL-UNNAMED` fix already applied to this
script) before proceeding:

    ls "$AERON_DIR"/cnc.dat   # AERON_DIR defaults to /dev/shm/aeron-$(id -u)

## 3. Start order-process on all 3 nodes

On Nitin:    `cd order-process && ./starter.sh 1`
On Amit:     `cd order-process && ./starter.sh 2`
On Yousuf:   `cd order-process && ./starter.sh 3`

Wait for a `[role] ... is LEADER` line to appear on all three consoles
before continuing (confirms the cluster has elected a leader).

## 4. Start order-receiver on Yousuf

    cd order-receiver && ./target/release/order-receiver

## 5. Start order-sending on Yousuf, driving 300k/sec

    cd order-sending
    TARGET_TPS=300000 SENDER_THREADS=16 ./target/release/order-sending

Let it run for at least 30 seconds before stopping (Ctrl-C on order-sending
first, then wait a few seconds for order-process/order-receiver to drain
in-flight orders before stopping them).

## 6. Score the run

    wc -l order-sending/logs/orders-sent.log
    wc -l order-process/logs/orders-processed*.log   # sum across all 3 nodes' WAL-adjacent logs if present
    wc -l order-receiver/logs/orders-received.log

Success: sent count == received count (processed count on the leader's own
WAL should also match — a follower's replicated WAL is not a duplicate
count, it's the same logical entries).

## 7. If short of 300k/sec

Expected on the first run — the spec calls this out explicitly. Things to
check, in order of likely impact:
- `order-sending`'s per-second throughput printout (stats thread) — is the
  bottleneck at the sender, or is send throughput fine but processed/received
  falling behind?
- CPU on Yousuf specifically — it runs order-sending + order-process (if it's
  leader) + order-receiver simultaneously (see the spec's flagged
  co-location constraint). `top`/`htop` during a run will show if this
  single machine is the ceiling.
- Raise `SENDER_THREADS` if generator threads (not the fan-out/publisher
  threads) are the bottleneck.
- Actual LAN bandwidth between the 3 machines (unknown at design time) —
  `iperf3` between Nitin/Amit/Yousuf if throughput plateaus well under
  300k/sec despite low CPU usage everywhere.
```

- [ ] **Step 2: Commit**

```bash
git add scripts/run_lab_benchmark.md
git commit -m "docs: add 300k/sec lab benchmark runbook for the real 3-machine deployment"
```

---

## Notes for whoever executes this plan

- Tasks 1-3 must run in order (each depends on the previous compiling). Tasks 4 and 5 can run in parallel with each other and with Tasks 1-3 (independent crates), but Task 6 depends on all of them.
- Every `cargo build --release` step should be run from the crate's own directory (`order-process/`, `order-sending/`, `order-receiver/`) — these are independent crates, not a workspace.
- Delete any leftover `logs/*.log` files and benchmark WAL directories from before Task 1 lands — the WAL's on-disk format changes from JSON-lines to length-prefixed bincode, and old files will not parse (they'll just read back as zero entries, not crash, but that could be confused for a real data-loss bug during verification if not anticipated).
