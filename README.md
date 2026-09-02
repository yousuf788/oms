# Rust OMS Demo (S1 → S2 Cluster → S3)

Microservice-style order pipeline with a 3-node Raft-style processing cluster.

| Service | Folder | Role |
|---|---|---|
| **S1** | `order-sending/` | Creates one order per second; UDP fan-out to all S2 nodes |
| **S2** | `order-process/` | 3-replica cluster: election, WAL replication, quorum commit |
| **S3** | `order-receiver/` | Prints / logs final results from the **leader only** |
| Witness | `order-witness/` | Independent arbiter: corroborates single-node self-promotion (see below) |

Each service is a standalone Rust crate (own `Cargo.toml`, own `target/`). **All hosts/IPs come from `.env` files — nothing is hardcoded in Rust.**

---

## Architecture

```mermaid
flowchart LR
    subgraph S1["order-sending (S1)"]
        Sender["Order generator\n:9001"]
    end

    subgraph S2["order-process cluster (S2)"]
        direction TB
        N1["Nitin S2-1\nraft :6001 / orders :7001\nWAL"]
        N2["Amit S2-2\nraft :6002 / orders :7002\nWAL"]
        N3["Yousuf S2-3\nraft :6003 / orders :7003\nWAL"]
        N1 <-->|"Raft UDP"| N2
        N2 <-->|"Raft UDP"| N3
        N3 <-->|"Raft UDP"| N1
    end

    subgraph S3["order-receiver (S3)"]
        Receiver["Results\n:8001"]
    end

    Sender -->|"fan-out"| N1
    Sender -->|"fan-out"| N2
    Sender -->|"fan-out"| N3
    N1 -.->|"leader only"| Receiver
    N2 -.->|"followers silent"| Receiver
    N3 -.->|"followers silent"| Receiver
```

**Happy path**

1. Sender writes order → all three S2 order ports  
2. Leader appends to its WAL and replicates to followers  
3. After quorum commit, leader applies and sends result to S3  
4. Followers apply the same committed entries (same logical log)

---

## Lab machines (example)

| Machine | Name in logs | Services | Example IP |
|---|---|---|---|
| Nitin | `NODE1_NAME=Nitin` | `order-process` (node 1) | `172.16.12.104` |
| Amit | `NODE2_NAME=Amit` | `order-process` (node 2) | `172.16.13.181` |
| Yousuf | `NODE3_NAME=Yousuf` | `order-process` (node 3) + sender + receiver + `order-witness` | `10.10.1.69` |

Use the **same** `NODE*_HOST` / ports on every machine’s `.env`. Only `NODE_ID` differs (auto-detected from local IP, or pass `./starter.sh 1|2|3`).

---

## Ports

| Purpose | Port |
|---|---|
| Raft (Nitin / Amit / Yousuf) | `6001` / `6002` / `6003` |
| Orders in (Nitin / Amit / Yousuf) | `7001` / `7002` / `7003` |
| Health probe — witness only (Nitin / Amit / Yousuf) | `6101` / `6102` / `6103` |
| Sender bind | `9001` |
| Receiver bind | `8001` |
| Witness corroboration | `9101` (on the witness machine) |

Open UDP on these ports between all machines.

---

## Config (`.env` only)

No machine IPs in source code. Each service loads dotenv:

| Service | Files |
|---|---|
| `order-process` | `.env` (active), `.env.example` (localhost), `cluster.sample` (lab IPs) |
| `order-sending` | `.env`, `.env.example`, `cluster.sample` |
| `order-receiver` | `.env`, `.env.example` |
| `order-witness` | `.env` (active), `.env.example` (localhost) |

**Local (one PC, three `order-process` processes)**

```bash
cd order-process && cp .env.example .env
# use ./starter.sh 1 / 2 / 3 in three terminals
```

**Multi-machine lab**

```bash
cd order-process && cp cluster.sample .env   # same file on Nitin, Amit, Yousuf
cd ../order-sending && cp cluster.sample .env
cd ../order-receiver && cp .env.example .env
# set S3_HOST / NODE* to match your LAN
```

### Important `order-process` knobs

| Variable | Meaning | Default |
|---|---|---|
| `NODE1/2/3_NAME` | Names in `[role]` lines | Nitin / Amit / Yousuf |
| `NODE1/2/3_HOST` | Peer IPs (**required**) | — |
| `S3_HOST` / `S3_PORT` | Where leader sends results | port `8001` |
| `ALLOW_SINGLE_NODE_LEADER` | Alone node may elect + commit | `true` |
| `PEER_SILENT_MS` | Mark peer “not available” after silence | `2000` |
| `VERBOSE_RAFT` | Print Raft catch-up spam | `false` |
| `WITNESS_HOST` / `WITNESS_PORT` | Independent arbiter consulted before single-node self-promotion | — (unset = no witness) |
| `REQUIRE_WITNESS_FOR_SINGLE_NODE_LEADER` | `false` = legacy blind-timeout self-promotion (no witness needed) | `true` |
| `NODE1/2/3_HEALTH_PORT` | Liveness-probe port the witness pings (separate from Raft) | 6101 / 6102 / 6103 |

---

## How to run

### Build

```bash
cd order-process && cargo build --release
cd ../order-sending && cargo build --release
cd ../order-receiver && cargo build --release
cd ../order-witness && cargo build --release
```

### Multi-machine (recommended)

**On Nitin / Amit / Yousuf (order-process):**

```bash
cd order-process
./starter.sh          # NODE_ID from this machine’s IP
# or: ./starter.sh 1  # Nitin   ./starter.sh 2  # Amit   ./starter.sh 3  # Yousuf
```

`starter.sh` loads `.env`, installs Rust via rustup if needed, and runs release.

### Benchmarking

Automated performance benchmarking scripts and detailed performance limit reports are available:

```bash
# Run 1-node baseline benchmark (1 node, 4 threads, 10s duration)
./scripts/run_benchmark.sh 1 4 10

# Run 3-node Raft consensus benchmark (3 nodes, 8 threads, 10s duration)
./scripts/run_benchmark.sh 3 8 10
```

Detailed metrics, bottleneck analysis ($O(N)$ WAL rewrite, UDP socket buffer overruns, `fsync` overhead), and optimization roadmap are documented in [`docs/BENCHMARK.md`](docs/BENCHMARK.md).

**On Yousuf (receiver + sender + witness):**

```bash
cd order-receiver && cargo run --release
cd order-sending  && cargo run --release
cd order-witness  && cargo run --release
```

`order-witness` needs no `starter.sh` — it's UDP-only, no Aeron/media-driver bootstrap
required. It does need `cp .env.example .env` (or the real `.env`) with `NODE*_HOST`/
`NODE*_HEALTH_PORT` matching `order-process`'s exactly. If this machine has never run
`order-process/starter.sh`, install Rust first: `curl --proto '=https' --tlsv1.2 -sSf
https://sh.rustup.rs | sh`.

### What you should see

**Role line (order-process):**

```text
[role] Nitin is LEADER; Amit is FOLLOWER; Yousuf is FOLLOWER
```

When peers are down:

```text
[role] Nitin is not available; Amit is not available; Yousuf is LEADER
```

**Order files (append-only):**

| File | Written by |
|---|---|
| `order-sending/logs/orders-sent.log` | every sent order |
| `order-process/logs/orders-processed.log` | every order committed by the **leader** |
| `order-receiver/logs/orders-received.log` | every received result |

Leader console also prints `[order] … LEADER received/committed …` lines.

---

## Quorum, failover, and “one machine left”

| Live S2 nodes | Quorum (with `ALLOW_SINGLE_NODE_LEADER=true`) | Behavior |
|---|---|---|
| 3 | 2 | Normal Raft majority |
| 2 | 2 | Still need both live nodes to agree |
| 1 | 1 | Remaining node becomes leader and **can commit alone**, but only after peers are silent ≥ `PEER_SILENT_MS` **and** `order-witness` corroborates they're genuinely down (see below) |

### Witness-corroborated single-node promotion

A node that can't reach either peer can't tell, from the inside, whether both peers are
genuinely down or whether *it's* the one that got cut off while its peers formed their
own quorum. Treating those as the same thing is how split-brain happens — so a local
timeout alone (`PEER_SILENT_MS`) is never sufficient by itself. Once that timeout
elapses, the node asks `order-witness` — a separate, non-sequencing process — "can you
reach my two peers right now?":

- Witness also can't reach them → corroborated → node promotes to `LEADER` (console
  shows `... witness-corroborated`).
- Witness *can* reach at least one → node stays passive (`[witness] corroboration
  denied ...`) — it's the one partitioned, not its peers.
- Witness itself unreachable, or not configured at all → node stays passive
  (`[witness] witness unreachable ...` / `no witness configured ...`). Uncertainty
  always resolves to staying passive, never to promoting.

Set `REQUIRE_WITNESS_FOR_SINGLE_NODE_LEADER=false` to skip this and fall back to the
old blind-timeout behavior (only `order-process/.env.example`'s local demo does this,
since it doesn't run a witness by default).

**Failover test**

1. Start all three `order-process` + receiver + sender + `order-witness`.
2. Note who is `LEADER` in `[role]` lines.
3. Stop the leader process.
4. Within ~1–2s another node should become `LEADER`; receiver keeps getting orders.
5. Stop two nodes: the last one should show the others as `not available`, then (once
   the witness corroborates they're both down) become `LEADER` — console line will read
   `... single-node: other machines unreachable, witness-corroborated` — and still send
   results to S3.
6. **Split-brain check**: instead of stopping the two peers, leave them running but
   break only the survivor's *own* view of them (e.g. temporarily point its
   `NODE*_HOST`/ports for the other two at something unreachable) while leaving the
   witness's `.env` pointed at their real addresses. The survivor must stay passive
   (`[witness] corroboration denied ...`) — never `LEADER` — while its two peers keep
   running their own quorum normally. This proves the witness prevents exactly the
   split-brain scenario this feature exists for.

Strict production Raft (no single-node) → set `ALLOW_SINGLE_NODE_LEADER=false` (then 1 of 3 **cannot** elect/commit; the witness is irrelevant in this mode).

---

## WAL (one file per machine)

Each node keeps its own durable log (not a shared NFS file):

```text
order-process/data/wal-s2-<node_id>.log
```

Override directory: `ORDER_PROCESS_DATA_DIR=/path ./starter.sh`

Raft keeps logs logically the same via batched `AppendEntries`. If logs diverge badly:

1. Stop sender and all processors.  
2. Keep the **longest correct** WAL.  
3. On lagging nodes: `rm -f data/wal-s2-*.log` (or under your data dir).  
4. Restart all three, then sender; leader will catch followers up.

---

## Message / code flow (short)

1. **S1** JSON order → UDP to `NODE1/2/3_HOST:ORDER_PORT`  
2. **S2 leader** builds `ReplicatedCommand` → WAL → replicate → quorum commit → apply  
3. **S2 leader** UDP result → `S3_HOST:S3_PORT`  
4. **S3** prints + appends `logs/orders-received.log`  
5. **S2 followers** only replicate/apply; they do **not** emit to S3  

---

## Troubleshooting

| Symptom | Check |
|---|---|
| `missing NODE1_HOST in .env` | Copy `cluster.sample` / `.env.example` to `.env` |
| `NODE_ID` panic / auto-detect fail | Run on a machine whose IP matches `NODE*_HOST`, or `./starter.sh 1\|2\|3` |
| `failed to bind` | Port in use; stop old process |
| No `[role] … LEADER` | Raft UDP `6001–6003` blocked or wrong IPs |
| Role shows LEADER but receiver silent | Confirm `S3_HOST`/`S3_PORT`; wait until peers marked not available if testing alone |
| WAL lengths differ a lot | Catch-up / wipe lagging WALs (see above); ensure latest code |
| Cross-subnet (e.g. `10.10` vs `172.16`) | All nodes must route to each other; prefer one LAN |
| Lone node stays passive forever, never becomes `LEADER` | Check `order-witness` is running and reachable (`WITNESS_HOST`/`WITNESS_PORT`), or set `REQUIRE_WITNESS_FOR_SINGLE_NODE_LEADER=false` for the legacy behavior |

---

## Operational notes

- Demo processing is random `FILLED` / `PARTIALLY_FILLED` / `REJECTED` (placeholder).  
- Transport is UDP (lab simplicity, not production messaging).  
- Default election timeout 300–600 ms; heartbeat 100 ms.  
- Set `VERBOSE_RAFT=true` only when debugging replication.
- `order-witness` should run on infrastructure independent of every `order-process`
  node it watches — if it shares a machine or network path with one of them, a
  failure there can take out both the node and the witness's ability to corroborate
  at once, defeating the point. See `order-witness/.env` for current placement.
