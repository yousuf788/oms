---
name: diagnose-cluster
description: Troubleshoot common cluster errors, NODE_ID panics, Aeron Media Driver failures, and monitoring blocks
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
When two S2 nodes are down, single-node self-promotion requires corroboration from `order-monitoring`. If `order-monitoring` is down or unreachable, the survivor stays passive to prevent split-brain. This is also the symptom of a very common typo — see below.

#### Resolution Workflow
1. Check if `order-monitoring` is running:
   ```bash
   cd order-monitoring && cargo run --release
   ```
2. Check monitoring logs:
   ```bash
   cat order-monitoring/logs/corroboration.log
   ```
3. For local single-machine testing without monitoring, set in `order-process/.env`:
   ```env
   REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER=false
   ```
   **The casing above is exact and load-bearing** — `REQUIRE_MONITORING_FOR_SINGLE_NODE_LEADER`
   (standard shouty-case) is a *different* env var the code never reads; it silently falls back
   to the default (`true`), which is indistinguishable from this bug except that nothing you set
   ever takes effect. This is a leftover artifact of the order-witness→order-monitoring rename
   that didn't normalize case (same root cause as the `monitoringClient`/`monitoring_KEY` Rust
   identifier naming you may see flagged by clippy). Grep the exact string
   `REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER` (note lowercase `monitoring`) if unsure.

---

### 5. Issue: `missing S1_HOST in .env` panic on `order-process`, or a similar panic on `order-receiver`

#### Root Cause
The sequencing/gap-detection/replay protocol (see `docs/HLD.md` §7) added new required config:
`order-process` needs `S1_HOST`/`S1_REPLAY_PORT` (where order-sending's replay listener is) and
`order-receiver` now needs `CLUSTER_HMAC_KEY` plus a full `NODE1/2/3_HOST` +
`NODE1/2/3_REPLAY_PORT` list — it previously needed almost no configuration.

#### Resolution Workflow
1. Add the missing variable(s) — see `CLAUDE.md` §6's env var table or `.env.example` in the
   relevant crate for defaults.
2. `order-receiver`'s `CLUSTER_HMAC_KEY` must match the same key used by `order-sending` and
   every `order-process` node.

---

### 6. Issue: Leader keeps flapping (`[role]` cycles between all 3 names continuously), throughput never progresses under load

#### Root Cause
The leader's Raft heartbeat thread is missing the `ELECTION_TIMEOUT_MIN_MS`/`MAX_MS` deadline
purely from CPU scheduling delay, not a real failure. This shows up specifically when
simulating all 3 `order-process` nodes plus `order-sending` and `order-receiver` as competing
processes on one shared, contended machine — a real 3-machine deployment gives each node
dedicated CPU and shouldn't need this. This was the confirmed root cause of a total pipeline
stall found while load-testing (see `docs/BENCHMARK.md` §0).

#### Resolution Workflow
1. Check for CPU contention: `uptime` (load average relative to core count), `ps -eo pcpu,comm --sort=-pcpu | head`.
2. For single-machine testing under load, widen the timeouts — `scripts/run_benchmark.sh` already
   does this (`HEARTBEAT_INTERVAL_MS=100`, `ELECTION_TIMEOUT_MIN_MS=800`,
   `ELECTION_TIMEOUT_MAX_MS=1500`).
3. On real dedicated hardware, leader flapping is a more concerning signal than a benchmark
   artifact — investigate network, clock skew, or actual CPU starvation on that machine instead
   of just widening timeouts.

---

### 7. Issue: A gap in `order-receiver`'s log never closes no matter how long you wait

#### Root Cause
Either (a) the sustained input rate exceeds this deployment's real processing ceiling — the
backlog grows rather than the system losing data, since the replay protocol converges given
enough time *within* capacity, but there's no adaptive rate control yet to slow the sender past
capacity — or (b) the leader-flapping issue above, which looks similar (no progress) but has a
different fix.

#### Resolution Workflow
1. Check `[role]` lines for flapping first (see Scenario 6) — rule that out before concluding
   it's a capacity issue.
2. Compare against `docs/BENCHMARK.md` §0's measured ceilings for this kind of environment.
3. Lower `TARGET_TPS` or add capacity (more nodes doesn't help throughput here — replication
   overhead lowers the ceiling further, not higher, per the same benchmark).
