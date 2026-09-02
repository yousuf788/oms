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
pkill -f order-witness
```

### Step 2: Backup Existing WAL Logs (Optional)
Before deleting logs, copy them to a backup folder:

```bash
mkdir -p /data/Antier-project/Exchange/oms/order-process/data_backup
cp /data/Antier-project/Exchange/oms/order-process/data/wal-s2-*.log /data/Antier-project/Exchange/oms/order-process/data_backup/ 2>/dev/null || true
```

### Step 3: Remove Divergent WAL Logs
Remove all WAL logs from `order-process`:

```bash
rm -f /data/Antier-project/Exchange/oms/order-process/data/wal-s2-*.log
rm -f /data/Antier-project/Exchange/oms/order-process/logs/*.log
rm -f /data/Antier-project/Exchange/oms/order-sending/logs/*.log
rm -f /data/Antier-project/Exchange/oms/order-receiver/logs/*.log
```

### Step 4: Restart Cluster
Restart the S2 cluster nodes using `starter.sh`. The leader will re-initialize a fresh append-only WAL.
