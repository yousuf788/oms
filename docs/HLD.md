# OMS — High Level Design (HLD)

> **System**: Order Management System (OMS)
> **Architecture**: 3-tier distributed pipeline with Raft consensus
> **Transport**: Aeron UDP (reliable, low-latency)
> **Fault Tolerance**: Automatic leader failover via Raft protocol

---

## 1. System Overview

The OMS is split into three tiers running across three physical machines:

```
┌────────────────────────────────────────────────────────────────────────┐
│                       SERVER 1 (10.10.1.69)                         │
│                                                                        │
│   ┌─────────────┐   Aeron UDP    ┌──────────────────────────────────┐  │
│   │ order-      │ ─────────────► │ order-process  Node 3 (Server 1) │  │
│   │ sending     │                │ [Raft LEADER - normal mode]       │  │
│   │   (S1)      │                └──────────────────────────────────┘  │
│   │             │                                │ Aeron UDP (results) │
│   │             │                                ▼                     │
│   │             │                ┌──────────────────────────────────┐  │
│   │             │                │ order-receiver (S3)               │  │
│   │             │                │ logs/orders-received.log          │  │
│   └─────────────┘                └──────────────────────────────────┘  │
│         │                                                               │
│         │ Aeron UDP (unicast)                                           │
└─────────┼───────────────────────────────────────────────────────────────┘
          │
          ├──────────────────────────────────────────────────────────────►
          │                SERVER 2 (172.16.12.104)
          │              ┌──────────────────────────────┐
          │              │ order-process Node 1          │
          │              │ [Raft FOLLOWER — standby]     │
          │              └──────────────────────────────┘
          │
          └──────────────────────────────────────────────────────────────►
                          SERVER 3 (10.10.0.56)
                        ┌──────────────────────────────┐
                        │ order-process Node 2          │
                        │ [Raft FOLLOWER — standby]    │
                        └──────────────────────────────┘
```

---

## 2. Three-Tier Architecture

| Tier | Service | Machine | Role |
|------|---------|---------|------|
| **S1** | `order-sending` | Server 1 | Generates and publishes order events |
| **S2** | `order-process` | Server 1 + Server 2 + Server 3 | Raft cluster — consensus + processing |
| **S3** | `order-receiver` | Server 1 | Receives and persists committed results |

---

## 3. Data Streaming — How Orders Flow

### 3.1 Transport Layer: Aeron

All inter-service communication uses **Aeron**, a high-performance messaging system built on top of UDP.

**Why Aeron?**

| Feature | Aeron |
|---------|-------|
| Delivery guarantee | ✅ NAK-based retransmit |
| Backpressure | ✅ `offer()` blocks on retry |
| Ordering | ✅ In-order per stream |
| Throughput | 500K–10M msgs/sec (Aeron's own vendor-stated transport ceiling — not a number this system has measured; see §9.1) |
| Latency | <20µs (UDP), <1µs (IPC) (Aeron's own vendor-stated figures) |

**How Aeron works:**
Every machine runs an **Aeron Media Driver** — a lightweight Java daemon that manages shared memory ring buffers and handles UDP I/O. Rust services connect to this driver via shared memory (IPC) and don't touch the network directly.

```
Rust service → [shared memory] → Aeron Media Driver → [UDP] → Aeron Media Driver → [shared memory] → Rust service
```

### 3.2 Order Ingress (S1 → S2): Unicast per Node

`order-sending` creates **3 separate Aeron publications** — one for each S2 node.

```
order-sending (Server 1)
    │
    ├─ Publication → aeron:udp?endpoint=172.16.12.104:7001 [Stream 1001] → Node 1 (Server 2)
    ├─ Publication → aeron:udp?endpoint=10.10.0.56:7002 [Stream 1001] → Node 2 (Server 3)
    └─ Publication → aeron:udp?endpoint=10.10.1.69:7003 [Stream 1001] → Node 3 (Server 1)
```

**Why unicast (not multicast)?**
All 3 nodes need to receive every order because any of them can become the leader at any time. Unicast is more reliable across different switch configurations and doesn't require multicast routing.

`order-sending` also runs a replay listener on `SENDER_BIND_PORT`/`S1_REPLAY_PORT` (default `9001`) — a separate, HMAC-signed UDP control channel (not Aeron) that any S2 node can use to ask for a missing `order_id` range to be re-published. See §7.

**Backpressure in `order-sending`:**
```
8 generator threads
     │ (bounded channel — blocks if publisher is slow)
     ▼
1 publisher thread
     │ offer() → retries with BusySpinIdleStrategy if back-pressured
     ▼
3 Aeron publications (one per S2 node)
```

### 3.3 Order Processing (S2 Raft Cluster)

Only the **Raft leader** processes orders. Followers receive the same orders via Aeron but silently discard them (they are only needed as Raft replicas for fault tolerance).

```
order arrives via Aeron subscription
         │
         ▼
   Is this node the leader?
    YES → propose_batch() → Raft consensus → WAL commit → send result to S3
    NO  → discard order (followers only replicate, not process)
```

### 3.4 Result Egress (S2 → S3): Unicast

All 3 S2 nodes have an **Aeron publication pointing to S3** on Server 1. Only the active leader calls `offer()`. Followers stay silent.

```
S2 Leader (any node)
    └─ Publication → aeron:udp?endpoint=10.10.1.69:8001 [Stream 2001] → order-receiver (S3)
```

---

## 4. Raft Consensus — How Orders Are Committed

The `order-process` cluster uses the **Raft consensus algorithm** to ensure that every committed order is durably stored and agreed upon by a majority of nodes before being sent to S3.

### 4.1 Normal Operation (Server 1 is Leader)

```
Step 1: Leader receives batch of orders from Aeron subscription

Step 2: Leader writes entries to its Write-Ahead Log (WAL)
        WAL file: logs/orders-processed.log

Step 3: Leader replicates entries to followers via Raft AppendEntries (UDP)
        Server 1 → Server 2  (172.16.12.104:6001)
        Server 1 → Server 3  (10.10.0.56:6002)

Step 4: Followers write to their WALs and send AppendAck back

Step 5: Leader receives ACKs from majority (2 out of 3 nodes = quorum)
        commit_index advances

Step 6: Leader applies committed entries → sends results to S3 via Aeron
```

**Quorum rule**: An entry is committed when **⌊N/2⌋ + 1 = 2 out of 3** nodes confirm it.
This means the system can tolerate **1 node failure** without data loss.

### 4.2 Write-Ahead Log (WAL)

Every committed order is written to disk on the leader before being sent to S3:

```
logs/
  orders-processed.log          ← Node 3 (Server 1 / leader in normal mode)
  orders-processed-s2-1.log     ← Node 1 (Server 2 replica WAL)
  orders-processed-s2-2.log     ← Node 2 (Server 3 replica WAL)
```

---

## 5. Leader Election — What Happens When a Node Goes Down

### 5.1 Detection: Heartbeat Timeout

The leader sends a **heartbeat** (empty AppendEntries) to all followers every **50ms**.

If a follower does not receive a heartbeat within its **election timeout** (randomly chosen between **150ms and 300ms**), it assumes the leader is dead and starts a new election.

The randomisation is critical — it prevents all nodes from starting elections simultaneously.

### 5.2 Election Process (Step by Step)

```
T=0ms    Leader (Server 1) goes offline — heartbeats stop

T=150ms~ First follower (e.g. Server 2) hits its random election timeout
         Server 2 increments its term (e.g. term 4 → 5)
         Server 2 transitions to CANDIDATE state
         Server 2 votes for itself

T=150ms  Server 2 broadcasts RequestVote { term: 5, last_log_index: X, last_log_term: Y }
                                     │
                       ┌─────────────┴─────────────┐
                       ▼                           ▼
                  Server 3 (follower)         Server 1 (offline)
                  Checks log freshness        No response
                  Grants vote if ok

T=152ms  Server 2 receives VoteGranted from Server 3
         Server 2 now has 2 votes (self + Server 3) = quorum (2/3)
         Server 2 transitions to LEADER state

T=152ms  Server 2 immediately sends AppendEntries heartbeat to all peers
         (establishes authority, resets all follower election timers)

T=152ms~ Server 2 starts processing orders and publishing results to S3
```

### 5.3 Exact Leader Election Time

| Phase | Duration |
|-------|----------|
| Heartbeat miss detection (election timeout) | **150 – 300ms** (random per node) |
| RequestVote broadcast + VoteGranted round-trip | **~2 – 5ms** (LAN) |
| New leader sends first heartbeat | **<1ms** |
| **Total failover time (typical)** | **~152 – 307ms** |
| **Total failover time (worst case)** | **<350ms** |

> **Why randomised?** If Server 2 and Server 3 hit their timeouts at exactly the same time, they'd both become candidates and split votes (no winner). The random range (150–300ms) makes it statistically unlikely that two nodes hit the timeout simultaneously.

> **Why not go lower than 150ms?** The election timeout must be significantly larger than the heartbeat interval (50ms) to avoid false elections caused by a delayed heartbeat (network jitter, CPU spike). A ratio of 3× is the minimum safe threshold. Going below 100ms can cause spurious leader churn on a busy LAN.

### 5.4 Election Safety Rules (Raft §5.4)

A node only grants a vote if the candidate's log is **at least as up-to-date** as its own:

```
candidate.last_log_term > my.last_log_term
    OR
(candidate.last_log_term == my.last_log_term AND candidate.last_log_index >= my.last_log_index)
```

This guarantees that the new leader always has all committed entries — **no data is ever lost** during failover.

### 5.5 Leader Lease (Anti-disruption Guard)

To prevent an old stale node from disrupting a healthy leader when it rejoins, the system implements a **leader lease**:

- If the current leader received `AppendAck` from a quorum of followers within the last **4 × heartbeat = 200ms**, it **ignores** `RequestVote` messages from reconnecting nodes.
- The reconnecting node's accumulated term (from repeated failed elections while offline) is still adopted, but the leader stays in place.

---

## 6. Single-Node Failover (Extreme Case)

If **both Server 2 and Server 3 go offline**, and only Server 1 is running:

```
Configuration: ALLOW_SINGLE_NODE_LEADER=true
               PEER_SILENT_MS=2000ms
               REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER=true
               monitoring_HOST=<order-monitoring machine>
```

- After **2000ms** of silence from all peers, Server 1 detects it is *locally* isolated.
- A local timeout alone is **not** sufficient grounds to promote — Server 1 cannot tell,
  from the inside, whether Server 2 and Server 3 are genuinely down or whether it's the
  one that got partitioned while they formed their own quorum between themselves.
  Treating those two cases the same is how split-brain happens. See §6.1.
- Server 1 asks the independent **monitoring** service (`order-monitoring`, §6.1) whether it
  can reach Server 2 and Server 3. Only if the monitoring also can't reach them does
  Server 1 promote itself with **quorum = 1** (single-node cluster).
- If the monitoring reports either peer is actually reachable — or the monitoring itself is
  unreachable — Server 1 **stays passive**: it keeps accepting/queuing orders on its
  Aeron subscription but does not process, commit, or send results. Uncertainty always
  resolves to caution, never to promotion.
- Once corroborated and promoted: orders continue to be processed and committed to
  Server 1's WAL, and results are sent to S3 as normal.

When Server 2 or Server 3 comes back online:
- They initiate an election (they've been accumulating terms while offline).
- Server 1's **log dominance** means reconnecting nodes will catch up via `AppendEntries` replication.
- Server 1 stays leader (no unnecessary re-election).

Setting `REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER=false` restores the original
blind-timeout behavior (no monitoring consulted) — used only by the single-machine local
demo, which doesn't run a monitoring process by default.

### 6.1 monitoring Corroboration

A new, independent, **non-sequencing** service, `order-monitoring`, exists solely to answer
one question for an isolated `order-process` node: *"can you reach my two peers right
now?"* It never processes orders, never holds Raft/consensus state, and never becomes
leader itself.

```
order-process node (locally isolated, PEER_SILENT_MS elapsed)
         │
         ▼
   send CorroborationRequest { requester_id, term } ──UDP──► order-monitoring
         │                                                        │
         │                                          independently pings the
         │                                          requester's two peers on
         │                                          their dedicated HEALTH_PORT
         │                                          (never the Raft port)
         │                                                        │
         │◄──UDP── CorroborationResponse { verdict } ─────────────┘
         ▼
   SafeToPromote  → node promotes to LEADER (quorum = 1)
   PeersStillUp   → node stays passive (it is the one partitioned)
   (no response / timeout) → node stays passive (fail-safe default)
```

- **Health probe port**: each `order-process` node runs a trivial, stateless UDP
  responder on `NODE{n}_HEALTH_PORT` (6101/6102/6103 by default) — completely separate
  from the Raft control port (6001/6002/6003), so a monitoring probe can never be
  misread as a Raft message or perturb consensus state.
- **Decision rule** (monitoring-side): `SafeToPromote` iff **both** of the requester's
  peers are currently unreachable. If either is reachable, `PeersStillUp` — one live
  peer is enough to mean the "genuine dual failure" precondition doesn't hold.
- **Corroboration timeout**: bounded at `monitoring_TIMEOUT_MS` (default 1500ms). No
  response within that window is treated identically to `PeersStillUp` — never guessed
  as safe.
- **Placement**: the monitoring must run on infrastructure that fails independently of the
  `order-process` nodes it watches — co-locating it with one of them defeats the
  purpose (a machine failure would take out both the node and the ability to
  corroborate its isolation at the same time). In this lab's 3-machine layout, the
  monitoring runs on Yousuf's machine.
- **Audit trail**: every corroboration request/response and every reachability state
  change is logged to flat files under `order-monitoring/logs/` (see §11).

### 6.2 Leader Visibility (Display-Only)

`order-monitoring` also shows which node it currently believes is leader — piggybacked
on the same health-probe `Ping`/`Pong` round-trip it already does every
`monitoring_POLL_INTERVAL_MS` (500ms default), not a new channel. Each `order-process`
node keeps two lock-free atomics (`role`, `term`) refreshed once per Raft tick (50ms) by
`LeaderElection`, and its health responder (`health_probe.rs`) reads them — never
`RaftState` itself, never a lock — when building each `Pong` reply. This preserves the
existing invariant that a health probe can never perturb or depend on consensus state.

`order-monitoring` logs a line to `logs/leader-transitions.log` (and console) only when
the believed leader changes — mirroring the existing `health-transitions.log` pattern —
and, separately, prints a rate-limited status line (at most once per 15s) confirming "all
N nodes reachable, leader=X" while every watched node stays reachable, so an operator
watching the console gets both event-driven changes and a periodic live confirmation.

**This is strictly observational.** Nothing here feeds back into any corroboration
decision or promotion logic — `order-monitoring` remains the same non-sequencing arbiter
described above; it just now also displays what it passively observes about leadership,
the same way it already displays reachability.

If more than one node reports `LEADER` in the same poll round (should only ever be a
transient artifact of the ~50ms tick lag during a handoff, never a steady state — a real
persistent occurrence would indicate an actual split-brain and is worth investigating
immediately), monitoring logs a `WARNING` line naming every node involved and its term;
the higher-term report is treated as current for display purposes.

---

## 7. Sequence Identity, Gap Detection & Replay Protocol

> **Delivery model: at-least-once with idempotent per-hop deduplication (effectively-once) — NOT exactly-once.** Replay makes duplicates possible by design; every hop dedups by `order_id`. Raft consensus (§4-§6) guarantees the S2 cluster's *internal* log agreement; it says nothing about whether an order ever reached S2 from S1, or a result ever reached S3 from S2 — that gap is what this section closes.

### 7.1 Why Raft alone doesn't solve this

Raft guarantees that once a majority of S2 nodes agree on a log entry, that entry is durable and correctly ordered *within the S2 cluster*. It says nothing about the S1→S2 or S2→S3 hops:

- Aeron provides reliable, in-order delivery *once a publication and subscription are connected* — but a publication that can't connect (dead node, not-yet-started subscriber) or hits a bounded retry budget can still fail to deliver a specific message. Aeron's own guarantees are a transport property, not an application-durability one — don't conflate the two.
- Before this protocol existed, a message dropped at either hop was silently gone: `order-sending`'s per-node publisher gave up after 100,000 retries and moved on; `order-receiver` deduped with an in-memory `HashSet` that forgot everything on restart, with no way to ask for what it never got.

### 7.2 Sequence identity

`order_id: u64` is assigned by `order-sending` from a counter that resumes from `order-sending/logs/orders-sent.wal`'s highest recorded id at startup — a restart never collides with previously-issued ids. Assignment order and Aeron publish order are not guaranteed identical (a shared atomic counter is read by several generator threads), so brief local reordering at ingest is normal and self-resolving.

### 7.3 Per-service durable state

| Service | File | Role |
|---|---|---|
| `order-sending` | `logs/orders-sent.wal` | Length-prefixed bincode, O(1)-offset-indexed by `order_id`. Source of truth for S1<->S2 replay and for resuming the counter. |
| `order-process` | `logs/orders-processed*.log` | The same Raft-replicated WAL described in §4.2 — also serves S2<->S3 replay via a linear scan filtered by `order_id` (the WAL's native index is Raft log `index`, not `order_id`; these aren't guaranteed identical, so this is a scan, not an indexed lookup — acceptable for a rare, bounded control-path operation). |
| `order-receiver` | `logs/receiver-checkpoint.dat` | A single `u64` watermark, flushed every 200ms — not full result content. Lets a restart ask "replay everything since X" instead of starting blind. |

None of these three implement retention, truncation, or snapshotting yet — each grows for the life of its process. This is a known, explicitly deferred gap.

### 7.4 Gap detection: the `SequenceTracker`

Both `order-process` (ingest side) and `order-receiver` run a `SequenceTracker`: a fixed-capacity ring bitset covering a 1Mi-`order_id` window ahead of a `last_contiguous` watermark.

```
mark(order_id):
    if order_id <= last_contiguous: return false   // duplicate, already delivered
    set bit for order_id in the ring
    while bit for (last_contiguous+1) is set:
        clear it, advance last_contiguous            // watermark catches up
    return "was this bit newly set?"

missing_ranges(): contiguous gaps between last_contiguous+1 and highest_seen
```

`mark()` is O(1) and allocation-free — called once per inbound item on the hot ingest path. `missing_ranges()` is O(gap span) and is only called periodically by a replay-request ticker, never per order. This structure also replaced a real bug: `order-process`'s previous per-batch `HashSet` only caught duplicates *within* one 20,000-order batch, not across batches or Aeron redelivery.

### 7.5 REPLAY_REQUEST protocol

Both hops use an identical message shape — `{requester_id: u8, ranges: Vec<(u64,u64)>}`, HMAC-signed with `CLUSTER_HMAC_KEY` — sent over a dedicated UDP control port, never the Aeron data stream (the same separation already used for the Raft control channel and monitoring corroboration channel in §6.1).

```
S2 → S1 (a gap in incoming orders):
   order-process's SequenceTracker reports a gap that has persisted 50ms
   (long enough that ordinary reordering has had time to resolve itself)
        │
        ▼
   REPLAY_REQUEST { ranges } ──UDP, HMAC-signed──► order-sending's replay listener
        │                                                   │
        │                                     reads the range back from its
        │                                     own WAL (O(1) offset lookup)
        │                                                   │
        ◄──────────── re-published on the live Aeron order channel ─────┘
        │
        ▼
   SequenceTracker.mark() dedups it like any other order
```

```
S3 → S2 (a gap in incoming results):
   order-receiver's SequenceTracker reports a gap
        │
        ▼
   REPLAY_REQUEST { ranges } ──broadcast to all 3 NODE{n}_REPLAY_PORT──►
        │
        ├─► follower: is_leader() false → silently ignored (architectural
        │              invariant: followers never speak on the result channel)
        │
        └─► leader: enqueues the range onto a bounded channel drained by
                     the same thread that owns the result Aeron publication
                     (LeaderElection::result_publisher_loop) — this is what
                     lets it interleave replay traffic with live commits
                     without two threads racing on one publication
                        │
                        ▼
                     entries scanned from the leader's own WAL by order_id,
                     re-published on the live result channel
```

Repeated requests for a still-outstanding range back off exponentially (100ms → 5s cap) — this never becomes a tight retry loop, and it converges eventually rather than giving up.

Both `order-process` and `order-receiver` also fire one unconditional catch-up request at startup — `last_committed_order_id+1..∞` (read from `order-process`'s own WAL) and `checkpoint+1..∞` respectively — instead of only detecting a gap reactively once a new live order happens to arrive. This matters because order-sending never stops sending on its own: without a proactive startup request, a freshly (re)started node would only notice how far behind it is the moment the *next* live order lands, which works but is passive; the proactive version means a restarted node starts recovering immediately, verified end-to-end (a node started fresh against an 8k-order backlog with nothing listening caught up to zero gaps and zero duplicates within seconds, with order-sending never pausing).

### 7.6 Backpressure design — two related fixes found by load testing

- **`order-sending`'s fan-out** uses non-blocking `try_send` per node channel. Originally this blocked, which meant one node with no live subscriber (or genuine backpressure) could stall delivery to the *other two* healthy nodes — directly contradicting the reason this file has one channel/thread per node in the first place. A skipped node is safe now specifically because replay exists to recover it.
- **`order-process`'s ingest channel** (Aeron poll callback → batching loop) uses *blocking* `send()`, deliberately. `SequenceTracker::mark()` happens before that send; a non-blocking drop after marking would be permanently invisible to gap detection, since the tracker would already believe it had seen that order. Blocking here safely throttles Aeron polling instead.

Both were found and fixed during the load-testing effort documented in `docs/BENCHMARK.md`.

---

## 8. Full Data Flow Diagram

```
                        ┌──────────────────────────────────────────────┐
                        │               SERVER 1 (10.10.1.69)       │
                        │                                              │
  ┌──────────────┐      │  ┌───────────────────────────────────────┐   │
  │  Orders      │      │  │         Aeron Media Driver            │   │
  │  (BTC, ETH,  │      │  │    (Java daemon — shared memory IPC)  │   │
  │   SOL trades)│      │  └───────────────────────────────────────┘   │
  └──────┬───────┘      │         │                   ▲                │
         │              │         │                   │                │
         ▼              │  ┌──────▼──────┐    ┌───────┴────────┐      │
  ┌──────────────┐      │  │order-sending│    │order-receiver  │      │
  │  8 generator │      │  │    (S1)     │    │    (S3)        │      │
  │  threads     │      │  │             │    │                │      │
  │  (rate-paced │      │  │ Publications│    │ Subscription   │      │
  │   5000 TPS)  │      │  │ × 3 nodes   │    │ stream 2001    │      │
  └──────────────┘      │  └──────┬──────┘    └───────┬────────┘      │
                        │         │                   │                │
                        └─────────┼───────────────────┼────────────────┘
                                  │                   │
          Aeron UDP unicast        │ Stream 1001       │ Stream 2001
          ┌───────────────────────┤                   │
          │                       │                   │
          ▼                       ▼                   │
  ┌───────────────┐   ┌───────────────────────────────────────────┐
  │  SERVER 2     │   │             SERVER 1 (order-process)      │
  │  Node 1       │   │                                           │
  │               │   │  Aeron Subscription (stream 1001)         │
  │  Follower     │   │                    │                      │
  │               │   │                   ▼                       │
  │  Replicates   │   │   ┌───────────────────────────────────┐   │
  │  WAL entries  │   │   │        Raft Consensus Engine       │   │
  │               │   │   │                                   │   │
  └───────────────┘   │   │  1. Write to WAL (disk)           │   │
          ▲           │   │  2. Replicate to Server 2 + 3     │   │
          │           │   │  3. Wait for quorum ACK (2/3)     │   │
  ┌───────────────┐   │   │  4. Commit entry                  │   │
  │  SERVER 3     │   │   │  5. offer() result → S3           │   │
  │  Node 2       │   │   └───────────────────────────────────┘   │
  │               │   │                                           │
  │  Follower     │   └───────────────────────────────────────────┘
  │               │
  │  Replicates   │
  │  WAL entries  │
  │               │
  └───────────────┘

  Raft control channel: UDP (port 6001/6002/6003) — separate from Aeron order channel
```

---

## 9. Key Configuration Parameters

| Parameter | Value | Effect |
|-----------|-------|--------|
| `HEARTBEAT_INTERVAL_MS` | 50ms | Leader sends heartbeat every 50ms |
| `ELECTION_TIMEOUT_MIN_MS` | 150ms | Earliest a follower starts an election |
| `ELECTION_TIMEOUT_MAX_MS` | 300ms | Latest a follower starts an election |
| `PEER_SILENT_MS` | 2000ms | Time before a peer is marked "unavailable" |
| `ALLOW_SINGLE_NODE_LEADER` | true | Server 1 can self-elect if both others are down |
| `REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER` | true | Single-node self-election requires monitoring corroboration (§6.1); `false` = legacy blind timeout. Casing is exact — see CLAUDE.md §6 |
| `monitoring_HOST` / `monitoring_PORT` | — / 9101 | Address of the `order-monitoring` service |
| `monitoring_TIMEOUT_MS` | 1500ms | Max wait for monitoring corroboration before staying passive |
| `NODE1/2/3_HEALTH_PORT` | 6101/6102/6103 | Liveness-probe ports the monitoring pings (separate from Raft) |
| `NODE1/2/3_REPLAY_PORT` | 6201/6202/6203 | S2<->S3 replay-request ports (§7.5) |
| `S1_HOST` / `S1_REPLAY_PORT` | required / 9001 | order-sending's replay listener address, for S1<->S2 replay (§7.5) |
| `TARGET_TPS` | 5000 (default), 300000 in order-sending/.env | Orders per second sent by order-sending — see §9.1 for what's actually been measured |
| Aeron Order Stream | 1001 | Logical stream ID for order messages |
| Aeron Result Stream | 2001 | Logical stream ID for result messages |

### 9.1 Measured throughput vs. the 300k target

`TARGET_TPS=300000` is the *design target*, not a validated result — and it is itself an order of magnitude below the current 500K–2M orders/sec design target under active evaluation (see `docs/BENCHMARK.md` §0.3). On a single shared machine simulating all 3 S2 nodes plus sender and receiver (i.e. `scripts/run_benchmark.sh`, not the real 3-machine lab deployment), the most recently re-verified measured sustained throughput with zero missing orders and zero duplicates was **~24,600 orders/sec with 1 node** and **~5,800-6,000 orders/sec with the full 3-node Raft cluster** — see `docs/BENCHMARK.md` §0.2 for the full results and methodology. Neither the 200k-300k TPS target nor the 500K–2M orders/sec target has been validated end-to-end; both require the real multi-machine deployment this document describes (`scripts/run_lab_benchmark.md`), since a single shared machine can't give each Raft node dedicated CPU the way the tuned 50ms/150-300ms heartbeat/election timings assume — and, independent of hardware, the current single-threaded propose-then-wait-for-quorum-commit design in `order-process` (nothing pipelines batch N+1's build against batch N's commit) is architecturally why 3-node throughput is *lower* than 1-node, not just slower.

---

## 10. Fault Tolerance Matrix

| Scenario | Behaviour | Orders Lost? | Downtime |
|----------|-----------|--------------|----------|
| Server 1 order-process restarts | Server 2 or Server 3 elected leader in 150–300ms | 0 | ~200ms |
| Server 2 goes offline | Server 1 + Server 3 maintain quorum, no election | 0 | 0ms |
| Server 3 goes offline | Server 1 + Server 2 maintain quorum, no election | 0 | 0ms |
| Server 2 + Server 3 both offline | Server 1 self-elects after 2000ms (`PEER_SILENT_MS`) **and** monitoring corroboration (§6.1) | 0 | ~2000ms + monitoring round-trip |
| Server 2 + Server 3 alive, but Server 1 partitioned from them | Server 1 stays passive (monitoring reports peers reachable) — no split-brain | 0 | N/A (queues, doesn't process) |
| monitoring unreachable while Server 1 is isolated | Server 1 stays passive (fail-safe default) | 0 | N/A (queues, doesn't process) |
| All 3 nodes offline | No leader — orders queue in Aeron buffers | 0 (buffered) | Until 1 node returns |
| order-sending restarts | Aeron reconnects automatically; `order_id` counter resumes from its own WAL (§7.2), not from 1 | 0 | <1s reconnect |
| order-receiver restarts | Aeron reconnects; dedup/gap watermark resumes from its checkpoint (§7.3) and fires a catch-up REPLAY_REQUEST | 0 | <1s reconnect + replay catch-up |
| order-monitoring restarts | No effect on quorum-based election (3 or 2 live nodes); only single-node self-promotion is gated | 0 | N/A |
| A node's Aeron publication can't connect / hits backpressure (§7.6) | That order is skipped for live delivery to that node only — not dropped for the other two, and recovered via REPLAY_REQUEST (§7.5) once the node can process again | 0 (recovered, not instant) | Bounded by 50ms debounce + backoff, not by the outage duration |
| Sustained input rate exceeds this deployment's processing ceiling | Backlog grows (no data loss — WAL + replay still converge given enough time) but delivery latency grows unboundedly; this is a cliff, not graceful degradation, since there's no adaptive rate control feeding back to the sender | 0 (eventually consistent, not currently rate-limited) | Until input rate drops back under the ceiling |

---

## 11. Log Files

| File | Machine | Content | Format |
|------|---------|---------|--------|
| `order-sending/logs/orders-sent.wal` | Server 1 | Every order generated by S1 — also the S1<->S2 replay source and restart-safe sequence counter (§7.2-§7.3) | binary (length-prefixed bincode) |
| `order-process/logs/orders-processed.log` | Server 1 (Node 3) | Orders committed by leader — also the S2<->S3 replay source (§7.3) | binary (length-prefixed bincode) |
| `order-process/logs/orders-processed-s2-1.log` | Server 2 (Node 1) | Replica WAL | binary (length-prefixed bincode) |
| `order-process/logs/orders-processed-s2-2.log` | Server 3 (Node 2) | Replica WAL | binary (length-prefixed bincode) |
| `order-receiver/logs/orders-received.log` | Server 1 | Results received by S3 | text (space-separated, `order_id` first field) |
| `order-receiver/logs/receiver-checkpoint.dat` | Server 1 | Dedup/gap watermark (§7.3) — not order content | text (`u64`) |
| `order-monitoring/logs/health-transitions.log` | monitoring machine | Reachability state changes for each watched node | text |
| `order-monitoring/logs/corroboration.log` | monitoring machine | Every corroboration request + verdict (audit trail) | text |
| `order-monitoring/logs/leader-transitions.log` | monitoring machine | Believed-leader changes only (§6.2) — display-only, not used for any decision | text |
