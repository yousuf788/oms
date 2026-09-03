---
name: clean-wal
description: Reset, clear, or synchronize diverge Write-Ahead Log (WAL) files across S2 nodes
---

# Skill: Clean & Reset Write-Ahead Logs (WAL)

Use this skill when WAL files diverge bad, when recovering from an unsynchronized node crash, or when resetting test state.

## Workflow Steps

### Step 1: Stop All Running Services
Terminate sender, processors, and receiver:

```bash
pkill -f order-sending
pkill -f order-process
pkill -f order-receiver
pkill -f order-monitoring
```

### Step 2: Backup Existing WAL Logs (Optional)
Before deleting logs, copy them to a backup folder. `order-process`'s WAL lives under
`logs/` by default (`orders-processed.log`, or `orders-processed-s2-<id>.log` per node
if `ORDER_PROCESS_DATA_DIR` is set) — NOT a `data/wal-s2-*.log` path:

```bash
mkdir -p /data/Antier-project/Exchange/oms/order-process/logs_backup
cp /data/Antier-project/Exchange/oms/order-process/logs/orders-processed*.log /data/Antier-project/Exchange/oms/order-process/logs_backup/ 2>/dev/null || true
```

### Step 3: Remove Divergent WAL / Sequence State
Remove WAL and checkpoint files from all three data-path services. Note that
`order-sending`'s WAL (`orders-sent.wal`) and `order-process`'s WAL
(`orders-processed*.log`) are **binary** (length-prefixed bincode) — don't try to
inspect them with `cat`/`grep`; `order-receiver`'s log and checkpoint are still text.

```bash
rm -f /data/Antier-project/Exchange/oms/order-process/logs/orders-processed*.log
rm -f /data/Antier-project/Exchange/oms/order-sending/logs/orders-sent.wal
rm -f /data/Antier-project/Exchange/oms/order-receiver/logs/orders-received.log
rm -f /data/Antier-project/Exchange/oms/order-receiver/logs/receiver-checkpoint.dat
```

> [!CAUTION]
> Deleting `order-sending`'s WAL resets its `order_id` counter back to 1 on next
> start — only do this for a genuine full reset (e.g. between benchmark runs), not
> as a routine fix, since it also means any range order-process might still be
> asking to replay from the old sequence can no longer be served.

### Step 4: Restart Cluster
Restart the S2 cluster nodes using `starter.sh`. The leader will re-initialize a fresh append-only WAL. If only `order-receiver`'s checkpoint was deleted (not order-sending's/order-process's WALs), it will re-request everything from `order_id` 1 on next start via its startup catch-up REPLAY_REQUEST (see `docs/HLD.md` §7.5) — expect a burst of replay traffic, not an error.
