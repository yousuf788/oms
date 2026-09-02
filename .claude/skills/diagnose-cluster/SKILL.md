---
name: diagnose-cluster
description: Troubleshoot common cluster errors, NODE_ID panics, Aeron Media Driver failures, and Witness blocks
---

# Skill: Diagnose Cluster Issues

Use this skill when diagnosing runtime panics, connection failures, or unexpected node behaviors.

## Diagnostic Scenarios & Workflows

---

### 1. Panic: `NODE_ID not set and no local IP matches NODE1/2/3_HOST in .env`

#### Root Cause
Auto-detection failed because either:
- The local IP address does not match any `NODE*_HOST` in `.env`.
- All host entries are `127.0.0.1` (local demo mode) and `NODE_ID` was not passed.

#### Resolution Workflow
- For local single-machine testing, pass explicit `NODE_ID`:
  ```bash
  cd order-process && ./starter.sh 1
  # or
  cd order-process && NODE_ID=1 cargo run
  ```
- For multi-machine lab setups, verify host IPs:
  ```bash
  hostname -I
  cat order-process/.env | grep NODE
  ```
  Ensure your machine's `hostname -I` IP matches `NODE1_HOST`, `NODE2_HOST`, or `NODE3_HOST`.

---

### 2. Error: `Aeron Media Driver already running` or `failed to connect to driver`

#### Root Cause
Stale Aeron shared memory ring buffers in `/dev/shm` or permission mismatch between user sessions.

#### Resolution Workflow
1. Check if Aeron Media Driver is running:
   ```bash
   pgrep -f aeron
   ls -la /dev/shm/aeron-*
   ```
2. If stale or locked:
   ```bash
   pkill -f aeron
   rm -rf /dev/shm/aeron-*
   ```
3. Relaunch `order-process/starter.sh` (it auto-detects and launches `Aeron Media Driver` daemon).

---

### 3. Log Error: `HMAC failure / dropped order packet`

#### Root Cause
`CLUSTER_HMAC_KEY` in `order-sending/.env` does not match `order-process/.env`.

#### Resolution Workflow
1. Generate a new secret key:
   ```bash
   openssl rand -hex 32
   ```
2. Copy the exact key into `CLUSTER_HMAC_KEY` across all `.env` files (`order-sending/.env` and `order-process/.env`).

---

### 4. Issue: Survivor Node Stays Passive & Never Becomes Leader

#### Root Cause
When two S2 nodes are down, single-node self-promotion requires corroboration from `order-witness`. If `order-witness` is down or unreachable, the survivor stays passive to prevent split-brain.

#### Resolution Workflow
1. Check if `order-witness` is running:
   ```bash
   cd order-witness && cargo run --release
   ```
2. Check witness logs:
   ```bash
   cat order-witness/logs/corroboration.log
   ```
3. For local single-machine testing without witness, set in `order-process/.env`:
   ```env
   REQUIRE_WITNESS_FOR_SINGLE_NODE_LEADER=false
   ```
