# Rust OMS Demo (S1 -> S2 Cluster -> S3)

This project demonstrates a microservice-style order flow with:

- `S1` (`order-sending`) generating and publishing orders
- `S2` (`order-process`) as a 3-replica Raft-style processing cluster
- `S3` (`order-receiver`) receiving final results

`order-process` uses:

- leader election (term-based)
- log replication (`AppendEntries`/`AppendAck`)
- quorum commit
- per-node durable WAL on disk

Only the current leader sends final results to `S3`.

---

## Project Structure

Each service is fully independent (own `Cargo.toml`, own `target`):

| Folder | Service | Role |
|---|---|---|
| `order-sending/` | S1 | Generates and sends orders to all S2 replicas |
| `order-process/` | S2 | 3-node replicated processing cluster |
| `order-receiver/` | S3 | Receives committed final results |

Service-specific configs:

- `order-sending/src/config.rs`
- `order-process/src/config.rs`
- `order-receiver/src/config.rs`

---

## Message Flow

1. `order-sending` creates one order every second.
2. S1 fan-outs the same order to all S2 order ports.
3. S2 leader proposes a replicated command into WAL.
4. Leader replicates to followers.
5. After quorum ack, leader marks entry committed.
6. All S2 nodes apply committed entries in order.
7. Leader publishes final result to S3 receiver.

Follower nodes do not emit external results.

---

## Ports and Channels

| Purpose | Node/Service | Port |
|---|---|---|
| Raft control | S2-1 | `6001` |
| Raft control | S2-2 | `6002` |
| Raft control | S2-3 | `6003` |
| Order ingress | S2-1 | `7001` |
| Order ingress | S2-2 | `7002` |
| Order ingress | S2-3 | `7003` |
| Sender bind | S1 | `9001` |
| Receiver bind | S3 | `8001` |

---

## Config (.env)

`order-process` loads cluster IPs/ports from `order-process/.env`.

```bash
cd order-process
cp .env.example .env
# edit NODE1_HOST / NODE2_HOST / NODE3_HOST / ports / S3_HOST
```

Example multi-machine values:

```env
BIND_HOST=0.0.0.0
NODE1_HOST=172.16.12.104
NODE1_RAFT_PORT=6001
NODE1_ORDER_PORT=7001
NODE2_HOST=172.16.13.181
NODE2_RAFT_PORT=6002
NODE2_ORDER_PORT=7002
NODE3_HOST=10.10.1.121
NODE3_RAFT_PORT=6003
NODE3_ORDER_PORT=7003
S3_HOST=10.10.1.69
S3_PORT=8001
```

Use the **same `.env` content** on all three `order-process` machines. Set only `NODE_ID` differently when starting.

---

Current default config is localhost (`127.0.0.1`) for all S2 nodes and S3.

### 1) Build each service

```bash
cd order-process && cargo build --release
cd ../order-sending && cargo build --release
cd ../order-receiver && cargo build --release
```

### 2) Run services (5 terminals)

```bash
# Terminal 1
cd order-process
NODE_ID=1 ./target/release/order-process

# Terminal 2
cd order-process
NODE_ID=2 ./target/release/order-process

# Terminal 3
cd order-process
NODE_ID=3 ./target/release/order-process

# Terminal 4
cd order-receiver
./target/release/order-receiver

# Terminal 5
cd order-sending
./target/release/order-sending
```

You should see:

- one S2 node becomes leader
- sender emits orders
- receiver prints final results

---

## Multi-Machine Setup

If running on Vivek/Amit/Nitin/Yousuf style deployment:

1. Update IPs in:
   - `order-process/src/config.rs`
   - `order-sending/src/config.rs`
   - `order-receiver/src/config.rs`
2. Build on each machine (or copy binaries).
3. Ensure firewall/network allows all required ports.
4. Start:
   - `order-process` on 3 machines with `NODE_ID=1/2/3`
   - `order-receiver` on receiver machine
   - `order-sending` on sender machine

Important:

- Do not use `127.0.0.1` for cross-machine nodes.
- Do not use Docker bridge IPs (`172.17.x.x`, `172.18.x.x`, etc.).

---

## WAL / Persistence

`order-process` stores per-node WAL under:

- default: `order-process/data/wal-s2-<node_id>.log`
- override: set `ORDER_PROCESS_DATA_DIR` env var

Example:

```bash
ORDER_PROCESS_DATA_DIR=/var/lib/order-process NODE_ID=1 ./target/release/order-process
```

---

## Failover Test (Leader Election Validation)

1. Start all services and wait until one S2 node is leader.
2. While traffic is flowing, kill leader process.
3. Observe:
   - another S2 node becomes leader (higher term)
   - receiver continues getting results
   - followers continue applying replicated entries

Example kill command:

```bash
pkill -f order-process
```

(Use carefully on the exact machine/process you want to stop.)

---

## Operational Notes

- Raft quorum for 3 nodes is 2.
- Leader election timeout defaults to 300-600 ms.
- This demo uses UDP and simplified processing logic.
- Business processing is placeholder logic for demonstration.

---

## Quick Troubleshooting

- `failed to bind ...`: port already in use -> stop old process or change port.
- No leader appears: verify inter-node reachability on Raft ports.
- Receiver not getting results: verify `S3_HOST`/`S3_PORT` in `order-process` config.
- Multi-network environment (LAN + Wi-Fi): ensure all S2 nodes can reach each other directly.
