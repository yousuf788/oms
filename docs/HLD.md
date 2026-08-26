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
│                       YOUSUF'S LAPTOP (172.16.12.252)                  │
│                                                                        │
│   ┌─────────────┐   Aeron UDP    ┌──────────────────────────────────┐  │
│   │ order-      │ ─────────────► │ order-process  Node 3 (Yousuf)   │  │
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
          │                VIVEK'S LAPTOP (172.16.12.104)
          │              ┌──────────────────────────────┐
          │              │ order-process Node 1 (Vivek) │
          │              │ [Raft FOLLOWER — standby]     │
          │              └──────────────────────────────┘
          │
          └──────────────────────────────────────────────────────────────►
                          AMIT'S LAPTOP (172.16.13.181)
                        ┌──────────────────────────────┐
                        │ order-process Node 2 (Amit)  │
                        │ [Raft FOLLOWER — standby]    │
                        └──────────────────────────────┘
```

---

## 2. Three-Tier Architecture

| Tier | Service | Machine | Role |
|------|---------|---------|------|
| **S1** | `order-sending` | Yousuf | Generates and publishes order events |
| **S2** | `order-process` | Vivek + Amit + Yousuf | Raft cluster — consensus + processing |
| **S3** | `order-receiver` | Yousuf | Receives and persists committed results |

---

## 3. Data Streaming — How Orders Flow

### 3.1 Transport Layer: Aeron

All inter-service communication uses **Aeron**, a high-performance messaging system built on top of UDP.

**Why Aeron instead of raw UDP?**

| Feature | Raw UDP (old) | Aeron (new) |
|---------|--------------|-------------|
| Delivery guarantee | ❌ Silent drops | ✅ NAK-based retransmit |
| Backpressure | ❌ None | ✅ `offer()` blocks on retry |
| Ordering | ❌ Not guaranteed | ✅ In-order per stream |
| Throughput | ~5K ops/sec | 500K–10M msgs/sec |
| Latency | ~1ms+ | <20µs (UDP), <1µs (IPC) |

**How Aeron works:**
Every machine runs an **Aeron Media Driver** — a lightweight Java daemon that manages shared memory ring buffers and handles UDP I/O. Rust services connect to this driver via shared memory (IPC) and don't touch the network directly.

```
Rust service → [shared memory] → Aeron Media Driver → [UDP] → Aeron Media Driver → [shared memory] → Rust service
```

### 3.2 Order Ingress (S1 → S2): Unicast per Node

`order-sending` creates **3 separate Aeron publications** — one for each S2 node.

```
order-sending (Yousuf)
    │
    ├─ Publication → aeron:udp?endpoint=172.16.12.104:7001 [Stream 1001] → Node 1 (Vivek)
    ├─ Publication → aeron:udp?endpoint=172.16.13.181:7002 [Stream 1001] → Node 2 (Amit)
    └─ Publication → aeron:udp?endpoint=172.16.12.252:7003 [Stream 1001] → Node 3 (Yousuf)
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

All 3 S2 nodes have an **Aeron publication pointing to S3** on Yousuf's machine. Only the active leader calls `offer()`. Followers stay silent.

```
S2 Leader (any node)
    └─ Publication → aeron:udp?endpoint=172.16.12.252:8001 [Stream 2001] → order-receiver (S3)
```

---

## 4. Raft Consensus — How Orders Are Committed

The `order-process` cluster uses the **Raft consensus algorithm** to ensure that every committed order is durably stored and agreed upon by a majority of nodes before being sent to S3.

### 4.1 Normal Operation (Yousuf is Leader)

```
Step 1: Leader receives batch of orders from Aeron subscription

Step 2: Leader writes entries to its Write-Ahead Log (WAL)
        WAL file: logs/orders-processed.log

Step 3: Leader replicates entries to followers via Raft AppendEntries (UDP)
        Yousuf → Vivek  (172.16.12.104:6001)
        Yousuf → Amit   (172.16.13.181:6002)

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
  orders-processed.log   ← Node 3 (Yousuf / leader in normal mode)
  orders-processed-s2-1.log  ← Node 1 (Vivek replica WAL)
  orders-processed-s2-2.log  ← Node 2 (Amit replica WAL)
```

---

## 5. Leader Election — What Happens When a Node Goes Down

### 5.1 Detection: Heartbeat Timeout

The leader sends a **heartbeat** (empty AppendEntries) to all followers every **100ms**.

If a follower does not receive a heartbeat within its **election timeout** (randomly chosen between **300ms and 600ms**), it assumes the leader is dead and starts a new election.

The randomisation is critical — it prevents all nodes from starting elections simultaneously.

### 5.2 Election Process (Step by Step)

```
T=0ms    Leader (Yousuf) goes offline — heartbeats stop

T=300ms~ First follower (e.g. Vivek) hits its random election timeout
         Vivek increments its term (e.g. term 4 → 5)
         Vivek transitions to CANDIDATE state
         Vivek votes for itself

T=300ms  Vivek broadcasts RequestVote { term: 5, last_log_index: X, last_log_term: Y }
                                    │
                      ┌─────────────┴─────────────┐
                      ▼                           ▼
                  Amit (follower)            Yousuf (offline)
                  Checks log freshness       No response
                  Grants vote if ok

T=302ms  Vivek receives VoteGranted from Amit
         Vivek now has 2 votes (self + Amit) = quorum (2/3)
         Vivek transitions to LEADER state

T=302ms  Vivek immediately sends AppendEntries heartbeat to all peers
         (establishes authority, resets all follower election timers)

T=302ms~ Vivek starts processing orders and publishing results to S3
```

### 5.3 Exact Leader Election Time

| Phase | Duration |
|-------|----------|
| Heartbeat miss detection (election timeout) | **300 – 600ms** (random per node) |
| RequestVote broadcast + VoteGranted round-trip | **~2 – 10ms** (LAN) |
| New leader sends first heartbeat | **<1ms** |
| **Total failover time (typical)** | **~302 – 612ms** |
| **Total failover time (worst case)** | **<700ms** |

> **Why randomised?** If both Vivek and Amit hit their timeouts at exactly the same time, they'd both become candidates and split votes (no winner). The random range (300–600ms) makes it statistically unlikely that two nodes hit the timeout simultaneously.

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

- If the current leader received `AppendAck` from a quorum of followers within the last **4 × heartbeat = 400ms**, it **ignores** `RequestVote` messages from reconnecting nodes.
- The reconnecting node's accumulated term (from repeated failed elections while offline) is still adopted, but the leader stays in place.

---

## 6. Single-Node Failover (Extreme Case)

If **both Vivek and Amit go offline**, and only Yousuf is running:

```
Configuration: ALLOW_SINGLE_NODE_LEADER=true
               PEER_SILENT_MS=2000ms
```

- After **2000ms** of silence from all peers, Yousuf detects it is alone.
- Yousuf promotes itself with **quorum = 1** (single-node cluster).
- Orders continue to be processed and committed to Yousuf's WAL.
- Results are sent to S3 as normal.

When Vivek or Amit comes back online:
- They initiate an election (they've been accumulating terms while offline).
- Yousuf's **log dominance** means reconnecting nodes will catch up via `AppendEntries` replication.
- Yousuf stays leader (no unnecessary re-election).

---

## 7. Full Data Flow Diagram

```
                        ┌─────────────────────────────────────────────┐
                        │           YOUSUF's Laptop                   │
                        │                                             │
  ┌──────────────┐      │  ┌────────────────────────────────────────┐ │
  │  Orders      │      │  │         Aeron Media Driver             │ │
  │  (BTC, ETH,  │      │  │    (Java daemon — shared memory IPC)   │ │
  │   SOL trades)│      │  └────────────────────────────────────────┘ │
  └──────┬───────┘      │         │                   ▲               │
         │              │         │                   │               │
         ▼              │  ┌──────▼──────┐    ┌───────┴────────┐     │
  ┌──────────────┐      │  │order-sending│    │order-receiver  │     │
  │  8 generator │      │  │    (S1)     │    │    (S3)        │     │
  │  threads     │      │  │             │    │                │     │
  │  (rate-paced │      │  │ Publications│    │ Subscription   │     │
  │   5000 TPS)  │      │  │ × 3 nodes   │    │ stream 2001    │     │
  └──────────────┘      │  └──────┬──────┘    └───────┬────────┘     │
                        │         │                   │               │
                        └─────────┼───────────────────┼───────────────┘
                                  │                   │
          Aeron UDP unicast        │ Stream 1001       │ Stream 2001
          ┌───────────────────────┤                   │
          │                       │                   │
          ▼                       ▼                   │
  ┌───────────────┐   ┌───────────────────────────────────────────┐
  │  VIVEK        │   │             YOUSUF (order-process)        │
  │  Node 1       │   │                                           │
  │               │   │  Aeron Subscription (stream 1001)         │
  │  Follower     │   │                    │                      │
  │               │   │                   ▼                       │
  │  Replicates   │   │   ┌───────────────────────────────────┐   │
  │  WAL entries  │   │   │        Raft Consensus Engine       │   │
  │               │   │   │                                   │   │
  └───────────────┘   │   │  1. Write to WAL (disk)           │   │
          ▲           │   │  2. Replicate to Vivek + Amit     │   │
          │           │   │  3. Wait for quorum ACK (2/3)     │   │
  ┌───────────────┐   │   │  4. Commit entry                  │   │
  │  AMIT         │   │   │  5. offer() result → S3           │   │
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
| `HEARTBEAT_INTERVAL_MS` | 100ms | Leader sends heartbeat every 100ms |
| `ELECTION_TIMEOUT_MIN_MS` | 300ms | Earliest a follower starts an election |
| `ELECTION_TIMEOUT_MAX_MS` | 600ms | Latest a follower starts an election |
| `PEER_SILENT_MS` | 2000ms | Time before a peer is marked "unavailable" |
| `ALLOW_SINGLE_NODE_LEADER` | true | Yousuf can self-elect if both others are down |
| `TARGET_TPS` | 5000 | Orders per second sent by order-sending |
| Aeron Order Stream | 1001 | Logical stream ID for order messages |
| Aeron Result Stream | 2001 | Logical stream ID for result messages |

---

## 9. Fault Tolerance Matrix

| Scenario | Behaviour | Orders Lost? | Downtime |
|----------|-----------|--------------|----------|
| Yousuf order-process restarts | Vivek or Amit elected leader in 300–600ms | 0 | ~500ms |
| Vivek goes offline | Yousuf + Amit maintain quorum, no election | 0 | 0ms |
| Amit goes offline | Yousuf + Vivek maintain quorum, no election | 0 | 0ms |
| Vivek + Amit both offline | Yousuf self-elects after 2000ms (`PEER_SILENT_MS`) | 0 | ~2000ms |
| All 3 nodes offline | No leader — orders queue in Aeron buffers | 0 (buffered) | Until 1 node returns |
| order-sending restarts | Aeron reconnects automatically | 0 | <1s reconnect |
| order-receiver restarts | Aeron reconnects, WAL intact on S2 | 0 | <1s reconnect |

---

## 10. Log Files

| File | Machine | Content |
|------|---------|---------|
| `order-sending/logs/orders-sent.log` | Yousuf | All orders generated by S1 |
| `order-process/logs/orders-processed.log` | Yousuf (Node 3) | Orders committed by leader |
| `order-process/logs/orders-processed-s2-1.log` | Vivek (Node 1) | Replica WAL |
| `order-process/logs/orders-processed-s2-2.log` | Amit (Node 2) | Replica WAL |
| `order-receiver/logs/orders-received.log` | Yousuf | Results received by S3 |
