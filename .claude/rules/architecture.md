# Rule: Architectural Invariants & Cluster Constraints

This rule specifies system-wide architectural invariants for the 3-tier distributed Order Management System (OMS).

---

## 1. 3-Tier Pipeline Invariants

1. **S1 (`order-sending`)**:
   - Generates orders and broadcasts them to all S2 cluster nodes using unicast Aeron publications.
   - Must never communicate directly with S3 (`order-receiver`) or `order-witness`.
2. **S2 (`order-process`)**:
   - 3-replica Raft consensus cluster (`NODE1`, `NODE2`, `NODE3`).
   - Only the active **Raft Leader** processes inbound orders, appends commands to its WAL, replicates to followers, and publishes results to S3.
   - **Raft Followers** receive orders and replicate log entries, but MUST STAY SILENT on result channels (no publishing to S3).
3. **S3 (`order-receiver`)**:
   - Passive result sink. Subscribes to result stream `2001` and logs committed orders to disk.
   - Deduplicates incoming entries by `order_id` to handle leader failover duplicates safely.

---

## 2. Write-Ahead Log (WAL) Invariants

- **$O(1)$ Append-Only**: WAL records are appended incrementally to `data/wal-s2-<node_id>.log`. Full file reads, file rewrites, or truncations during normal log append operations are strictly forbidden.
- **Log Equivalence**: Raft consensus guarantees log consistency across replicas via batched `AppendEntries` RPCs.

---

## 3. Split-Brain Protection & Witness Corroboration

- **Isolation Danger**: A node isolated from its peers cannot determine locally whether its peers are dead or whether it was partitioned.
- **Witness Role**: `order-witness` is a non-sequencing arbiter running independently of S2 nodes.
- **Single-Node Promotion Rule**: When `ALLOW_SINGLE_NODE_LEADER=true` and `PEER_SILENT_MS` elapses:
  - If `REQUIRE_WITNESS_FOR_SINGLE_NODE_LEADER=true`, the isolated node MUST query `order-witness` via UDP.
  - Single-node self-promotion to `LEADER` is allowed ONLY IF `order-witness` corroborates that both peers are unreachable.
  - If witness reports any peer is reachable or witness is unresponsive, the node MUST stay passive.
- **Port Isolation**: Witness probes use dedicated UDP health ports (`NODE{n}_HEALTH_PORT`: 6101, 6102, 6103) — NEVER the Raft consensus ports (6001, 6002, 6003).
