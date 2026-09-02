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
│                       SERVER 1 (172.16.12.252)                         │
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
                          SERVER 3 (172.16.13.181)
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
| Throughput | 500K–10M msgs/sec |
| Latency | <20µs (UDP), <1µs (IPC) |

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
    ├─ Publication → aeron:udp?endpoint=172.16.13.181:7002 [Stream 1001] → Node 2 (Server 3)
    └─ Publication → aeron:udp?endpoint=172.16.12.252:7003 [Stream 1001] → Node 3 (Server 1)
```

**Why unicast (not multicast)?**
All 3 nodes need to receive every order because any of them can become the leader at any time. Unicast is more reliable across different switch configurations and doesn't require multicast routing.

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
    └─ Publication → aeron:udp?endpoint=172.16.12.252:8001 [Stream 2001] → order-receiver (S3)
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
        Server 1 → Server 3  (172.16.13.181:6002)

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
               REQUIRE_WITNESS_FOR_SINGLE_NODE_LEADER=true
               WITNESS_HOST=<order-witness machine>
```

- After **2000ms** of silence from all peers, Server 1 detects it is *locally* isolated.
- A local timeout alone is **not** sufficient grounds to promote — Server 1 cannot tell,
  from the inside, whether Server 2 and Server 3 are genuinely down or whether it's the
  one that got partitioned while they formed their own quorum between themselves.
  Treating those two cases the same is how split-brain happens. See §6.1.
- Server 1 asks the independent **witness** service (`order-witness`, §6.1) whether it
  can reach Server 2 and Server 3. Only if the witness also can't reach them does
  Server 1 promote itself with **quorum = 1** (single-node cluster).
- If the witness reports either peer is actually reachable — or the witness itself is
  unreachable — Server 1 **stays passive**: it keeps accepting/queuing orders on its
  Aeron subscription but does not process, commit, or send results. Uncertainty always
  resolves to caution, never to promotion.
- Once corroborated and promoted: orders continue to be processed and committed to
  Server 1's WAL, and results are sent to S3 as normal.

When Server 2 or Server 3 comes back online:
- They initiate an election (they've been accumulating terms while offline).
- Server 1's **log dominance** means reconnecting nodes will catch up via `AppendEntries` replication.
- Server 1 stays leader (no unnecessary re-election).

Setting `REQUIRE_WITNESS_FOR_SINGLE_NODE_LEADER=false` restores the original
blind-timeout behavior (no witness consulted) — used only by the single-machine local
demo, which doesn't run a witness process by default.

### 6.1 Witness Corroboration

A new, independent, **non-sequencing** service, `order-witness`, exists solely to answer
one question for an isolated `order-process` node: *"can you reach my two peers right
now?"* It never processes orders, never holds Raft/consensus state, and never becomes
leader itself.

```
order-process node (locally isolated, PEER_SILENT_MS elapsed)
         │
         ▼
   send CorroborationRequest { requester_id, term } ──UDP──► order-witness
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
  from the Raft control port (6001/6002/6003), so a witness probe can never be
  misread as a Raft message or perturb consensus state.
- **Decision rule** (witness-side): `SafeToPromote` iff **both** of the requester's
  peers are currently unreachable. If either is reachable, `PeersStillUp` — one live
  peer is enough to mean the "genuine dual failure" precondition doesn't hold.
- **Corroboration timeout**: bounded at `WITNESS_TIMEOUT_MS` (default 1500ms). No
  response within that window is treated identically to `PeersStillUp` — never guessed
  as safe.
- **Placement**: the witness must run on infrastructure that fails independently of the
  `order-process` nodes it watches — co-locating it with one of them defeats the
  purpose (a machine failure would take out both the node and the ability to
  corroborate its isolation at the same time). In this lab's 3-machine layout, the
  witness runs on Yousuf's machine.
- **Audit trail**: every corroboration request/response and every reachability state
  change is logged to flat files under `order-witness/logs/` (see §10).

---

## 7. Full Data Flow Diagram

```
                        ┌──────────────────────────────────────────────┐
                        │               SERVER 1 (172.16.12.252)       │
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

## 8. Key Configuration Parameters

| Parameter | Value | Effect |
|-----------|-------|--------|
| `HEARTBEAT_INTERVAL_MS` | 50ms | Leader sends heartbeat every 50ms |
| `ELECTION_TIMEOUT_MIN_MS` | 150ms | Earliest a follower starts an election |
| `ELECTION_TIMEOUT_MAX_MS` | 300ms | Latest a follower starts an election |
| `PEER_SILENT_MS` | 2000ms | Time before a peer is marked "unavailable" |
| `ALLOW_SINGLE_NODE_LEADER` | true | Server 1 can self-elect if both others are down |
| `REQUIRE_WITNESS_FOR_SINGLE_NODE_LEADER` | true | Single-node self-election requires witness corroboration (§6.1); `false` = legacy blind timeout |
| `WITNESS_HOST` / `WITNESS_PORT` | — / 9101 | Address of the `order-witness` service |
| `WITNESS_TIMEOUT_MS` | 1500ms | Max wait for witness corroboration before staying passive |
| `NODE1/2/3_HEALTH_PORT` | 6101/6102/6103 | Liveness-probe ports the witness pings (separate from Raft) |
| `TARGET_TPS` | 5000 | Orders per second sent by order-sending |
| Aeron Order Stream | 1001 | Logical stream ID for order messages |
| Aeron Result Stream | 2001 | Logical stream ID for result messages |

---

## 9. Fault Tolerance Matrix

| Scenario | Behaviour | Orders Lost? | Downtime |
|----------|-----------|--------------|----------|
| Server 1 order-process restarts | Server 2 or Server 3 elected leader in 150–300ms | 0 | ~200ms |
| Server 2 goes offline | Server 1 + Server 3 maintain quorum, no election | 0 | 0ms |
| Server 3 goes offline | Server 1 + Server 2 maintain quorum, no election | 0 | 0ms |
| Server 2 + Server 3 both offline | Server 1 self-elects after 2000ms (`PEER_SILENT_MS`) **and** witness corroboration (§6.1) | 0 | ~2000ms + witness round-trip |
| Server 2 + Server 3 alive, but Server 1 partitioned from them | Server 1 stays passive (witness reports peers reachable) — no split-brain | 0 | N/A (queues, doesn't process) |
| Witness unreachable while Server 1 is isolated | Server 1 stays passive (fail-safe default) | 0 | N/A (queues, doesn't process) |
| All 3 nodes offline | No leader — orders queue in Aeron buffers | 0 (buffered) | Until 1 node returns |
| order-sending restarts | Aeron reconnects automatically | 0 | <1s reconnect |
| order-receiver restarts | Aeron reconnects, WAL intact on S2 | 0 | <1s reconnect |
| order-witness restarts | No effect on quorum-based election (3 or 2 live nodes); only single-node self-promotion is gated | 0 | N/A |

---

## 10. Log Files

| File | Machine | Content |
|------|---------|---------| 
| `order-sending/logs/orders-sent.log` | Server 1 | All orders generated by S1 |
| `order-process/logs/orders-processed.log` | Server 1 (Node 3) | Orders committed by leader |
| `order-process/logs/orders-processed-s2-1.log` | Server 2 (Node 1) | Replica WAL |
| `order-process/logs/orders-processed-s2-2.log` | Server 3 (Node 2) | Replica WAL |
| `order-receiver/logs/orders-received.log` | Server 1 | Results received by S3 |
| `order-witness/logs/health-transitions.log` | Witness machine | Reachability state changes for each watched node |
| `order-witness/logs/corroboration.log` | Witness machine | Every corroboration request + verdict (audit trail) |
