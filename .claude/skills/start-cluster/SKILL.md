---
name: start-cluster
description: Configure environment files and launch the multi-node S2 cluster, S1 sender, S3 receiver, and Witness
---

# Skill: Start Cluster (Local or Multi-Machine)

Use this skill when starting up the 3-replica `order-process` cluster, `order-sending`, `order-receiver`, and `order-witness`.

## Workflow Steps

### Step 1: Ensure `.env` Files Exist
Verify or populate `.env` files in all 4 crate directories:

```bash
# Local Single-Machine Demo Configuration
cd /data/Antier-project/Exchange/oms/order-process && [ -f .env ] || cp .env.example .env
cd /data/Antier-project/Exchange/oms/order-sending && [ -f .env ] || cp .env.example .env
cd /data/Antier-project/Exchange/oms/order-receiver && [ -f .env ] || cp .env.example .env
cd /data/Antier-project/Exchange/oms/order-witness && [ -f .env ] || cp .env.example .env
```

### Step 2: Start `order-process` S2 Nodes

#### Option A: Local Demo (3 Nodes on 1 Machine)
Run `starter.sh` in 3 separate terminal sessions:
```bash
# Node 1 (Nitin)
cd /data/Antier-project/Exchange/oms/order-process && ./starter.sh 1

# Node 2 (Amit)
cd /data/Antier-project/Exchange/oms/order-process && ./starter.sh 2

# Node 3 (Yousuf)
cd /data/Antier-project/Exchange/oms/order-process && ./starter.sh 3
```

#### Option B: Multi-Machine Lab Deployment
Copy `cluster.sample` to `.env` on each physical machine, edit `NODE*_HOST` to match actual LAN IPs, and execute:
```bash
cd /data/Antier-project/Exchange/oms/order-process && ./starter.sh
```

### Step 3: Start Auxiliary Services
Run each service in separate terminal windows:

```bash
# Start S3 Result Receiver
cd /data/Antier-project/Exchange/oms/order-receiver && cargo run --release

# Start Independent Witness Arbiter
cd /data/Antier-project/Exchange/oms/order-witness && cargo run --release

# Start S1 Order Generator
cd /data/Antier-project/Exchange/oms/order-sending && cargo run --release
```

### Step 4: Verify Cluster State & Logs
Check terminal console for the role summary line:
```text
[role] Nitin is LEADER; Amit is FOLLOWER; Yousuf is FOLLOWER
```
Confirm log files are being updated:
- `order-sending/logs/orders-sent.log`
- `order-process/logs/orders-processed.log`
- `order-receiver/logs/orders-received.log`
