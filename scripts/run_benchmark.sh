#!/usr/bin/env bash
set -e

# OMS Performance Benchmark Script — single-machine simulation.
#
# Usage: ./scripts/run_benchmark.sh [nodes: 1|3] [threads: N] [duration_sec: S] [target_tps: T]
#
# Restored+updated from git history (commit b538a48, deleted in d141253) to
# match the current code:
#   - order-sending's orders-sent.log is now a binary WAL (orders-sent.wal) —
#     can't `wc -l` it. Sent count is now parsed from order-sending's own
#     stdout throughput counter instead.
#   - order-receiver now requires CLUSTER_HMAC_KEY (it verifies the result
#     channel's HMAC) and a full S2 node list (NODE*_HOST/NODE*_REPLAY_PORT)
#     to broadcast REPLAY_REQUEST.
#   - order-process now requires S1_HOST (to send REPLAY_REQUEST to
#     order-sending) and, for NODES=1, REQUIRE_MONITORING_FOR_SINGLE_NODE_LEADER=false
#     (no order-monitoring service runs in this harness, so the default
#     monitoring-corroborated single-node promotion path would otherwise
#     never let a lone node become leader).
#   - Scoring now checks not just sent==received counts but sequence
#     continuity (missing/duplicate order_ids) in order-receiver's log, per
#     this system's zero-loss requirement.
#
# NOTE ON SCOPE: this is a single-machine simulation (all nodes on
# 127.0.0.1, differentiated by port). It validates pipeline correctness and
# this machine's throughput ceiling — it does NOT validate real 3-machine
# network throughput (see scripts/run_lab_benchmark.md for that).

NODES=${1:-1}
THREADS=${2:-4}
DURATION=${3:-10}
TARGET_TPS=${4:-5000}
export TARGET_TPS="$TARGET_TPS"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="$ROOT_DIR/target/benchmark_tmp"
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR/receiver_logs" "$BENCH_DIR/sending_logs" "$BENCH_DIR/wal_data"

# ── Aeron Media Driver ───────────────────────────────────────────────────
# Reuse an already-running driver at the default AERON_DIR if present
# (matches order-process/starter.sh's own live-process detection); only
# start a new one otherwise. Never overrides an existing dev-loop driver.
export AERON_DIR="${AERON_DIR:-/dev/shm/aeron-$(id -u)}"
DRIVER_STARTED_BY_THIS_SCRIPT=false
if pgrep -f "Daeron\.dir=${AERON_DIR}[^[:space:]]*.*MediaDriver" >/dev/null 2>&1; then
    echo "[bench] reusing already-running Aeron Media Driver (AERON_DIR=$AERON_DIR)"
else
    echo "[bench] no Media Driver found at $AERON_DIR — starting one"
    "$ROOT_DIR/scripts/start-media-driver.sh"
    DRIVER_STARTED_BY_THIS_SCRIPT=true
fi

# ── Common localhost configuration ───────────────────────────────────────
export BIND_HOST="127.0.0.1"
export NODE1_HOST="127.0.0.1"
export NODE1_NAME="Nitin"
export NODE1_RAFT_PORT="16001"
export NODE1_ORDER_PORT="17001"
export NODE1_REPLAY_PORT="16201"

export NODE2_HOST="127.0.0.1"
export NODE2_NAME="Amit"
export NODE2_RAFT_PORT="16002"
export NODE2_ORDER_PORT="17002"
export NODE2_REPLAY_PORT="16202"

export NODE3_HOST="127.0.0.1"
export NODE3_NAME="Yousuf"
export NODE3_RAFT_PORT="16003"
export NODE3_ORDER_PORT="17003"
export NODE3_REPLAY_PORT="16203"

export S3_HOST="127.0.0.1"
export S3_PORT="18001"

# Wider than the .env defaults (50/150/300ms): this benchmark runs all 3
# Raft nodes + sender + receiver as competing processes on one shared
# machine (not 3 dedicated physical hosts, which is what those tighter
# defaults assume). Under CPU contention here, a leader's heartbeat thread
# can miss a 150-300ms deadline purely from scheduling delay, not a real
# failure — observed directly as continuous leader churn at higher TPS that
# prevented the cluster from ever making sustained progress. This is a
# single-machine-simulation accommodation, not a production tuning change.
export HEARTBEAT_INTERVAL_MS="100"
export ELECTION_TIMEOUT_MIN_MS="800"
export ELECTION_TIMEOUT_MAX_MS="1500"
export ALLOW_SINGLE_NODE_LEADER="true"
export PEER_SILENT_MS="1000"
export VERBOSE_RAFT="false"
export SENDER_THREADS="$THREADS"

# No order-monitoring service runs in this harness — fall back to the
# legacy blind-timeout single-node promotion path instead of waiting
# forever for a monitoring corroboration that will never arrive.
# NOTE: env var name casing here is not a typo — it must match
# order-process/src/config.rs's actual `env_bool("REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER", ...)`
# lookup exactly (env var names are case-sensitive). That mixed casing is a
# leftover artifact of the order-witness->order-monitoring rename that
# didn't normalize case when "witness" became "monitoring" — same root
# cause as the monitoringClient/monitoring_KEY clippy naming warnings.
export REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER="false"

# order-sending's replay listener (S1<->S2 hop) — order-process needs
# S1_HOST to send REPLAY_REQUEST; S1_REPLAY_PORT matches order-sending's own
# SENDER_BIND_PORT (they're the same listener).
export SENDER_BIND_PORT="19001"
export S1_HOST="127.0.0.1"
export S1_REPLAY_PORT="19001"

# Ephemeral per-run key so nothing benchmark-only is committed anywhere.
export CLUSTER_HMAC_KEY="$(openssl rand -hex 32)"

# ── Reset per-run durable state ──────────────────────────────────────────
# Fresh WAL/checkpoint state each run so scoring reflects this run only,
# not accumulated history from a previous one.
rm -f "$ROOT_DIR/order-sending/logs/orders-sent.wal"
rm -f "$ROOT_DIR/order-receiver/logs/orders-received.log"
rm -f "$ROOT_DIR/order-receiver/logs/receiver-checkpoint.dat"

PIDS=()

cleanup() {
    echo "Stopping benchmark processes..."
    for pid in "${PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    if [ "$DRIVER_STARTED_BY_THIS_SCRIPT" = true ] && [ -f "$ROOT_DIR/scripts/media-driver.pid" ]; then
        kill "$(cat "$ROOT_DIR/scripts/media-driver.pid")" 2>/dev/null || true
    fi
}
trap cleanup EXIT

echo "=========================================================="
echo " Starting Benchmark Run:"
echo " Cluster Nodes : $NODES"
echo " Sender Threads: $THREADS"
echo " Duration      : ${DURATION}s"
echo " Target TPS    : ${TARGET_TPS}"
echo "=========================================================="

# 1. Start order-receiver (S3)
(
    cd "$ROOT_DIR/order-receiver"
    exec "$ROOT_DIR/order-receiver/target/release/order-receiver" > "$BENCH_DIR/s3.out" 2>&1
) &
PIDS+=($!)
sleep 0.5

# 2. Start order-process nodes (S2)
if [ "$NODES" -eq 1 ]; then
    (
        export NODE_ID=1
        export ORDER_PROCESS_DATA_DIR="$BENCH_DIR/wal_data"
        cd "$ROOT_DIR/order-process"
        exec "$ROOT_DIR/order-process/target/release/order-process" > "$BENCH_DIR/s2-node1.out" 2>&1
    ) &
    PIDS+=($!)
else
    for id in 1 2 3; do
        (
            export NODE_ID=$id
            export ORDER_PROCESS_DATA_DIR="$BENCH_DIR/wal_data"
            cd "$ROOT_DIR/order-process"
            exec "$ROOT_DIR/order-process/target/release/order-process" > "$BENCH_DIR/s2-node$id.out" 2>&1
        ) &
        PIDS+=($!)
    done
fi

echo "Waiting 2.5s for leader election to settle..."
sleep 2.5

# 3. Start order-sending (S1) for specified duration
(
    cd "$ROOT_DIR/order-sending"
    exec "$ROOT_DIR/order-sending/target/release/order-sending" > "$BENCH_DIR/s1.out" 2>&1
) &
SENDER_PID=$!
PIDS+=($SENDER_PID)

echo "Sending orders for ${DURATION}s..."
sleep "$DURATION"

# Stop sender
kill -9 "$SENDER_PID" 2>/dev/null || true
sleep 1

# SENT_COUNT: parse order-sending's binary WAL directly (length-prefixed
# bincode records — see order-sending/src/wal.rs) rather than its stdout
# throughput counter, which only prints once/sec and can under-report by up
# to ~1s of orders relative to the instant `kill -9` actually lands.
SENT_WAL="$ROOT_DIR/order-sending/logs/orders-sent.wal"
SENT_COUNT=0
if [ -f "$SENT_WAL" ]; then
    SENT_COUNT=$(python3 - "$SENT_WAL" <<'PYEOF'
import struct, sys
data = open(sys.argv[1], "rb").read()
pos = 0
count = 0
while pos + 4 <= len(data):
    (length,) = struct.unpack_from("<I", data, pos)
    pos += 4
    if pos + length > len(data):
        break
    pos += length
    count += 1
print(count)
PYEOF
)
fi

# ── Wait for convergence, not a fixed guess ─────────────────────────────
# order-process and order-receiver stay running (not killed yet) so the
# replay protocol has a real chance to close any gap before we score —
# scoring immediately after stopping the sender would misreport
# still-in-flight/still-converging orders as permanent loss.
RECEIVED_LOG="$ROOT_DIR/order-receiver/logs/orders-received.log"
CONVERGENCE_TIMEOUT="${CONVERGENCE_TIMEOUT:-15}"
echo "Waiting up to ${CONVERGENCE_TIMEOUT}s for received count to reach $SENT_COUNT (replay convergence)..."
prev_count=-1
stable_ticks=0
for _ in $(seq 1 "$CONVERGENCE_TIMEOUT"); do
    sleep 1
    cur_count=0
    [ -f "$RECEIVED_LOG" ] && cur_count=$(wc -l < "$RECEIVED_LOG" | tr -d ' ')
    if [ "$cur_count" -ge "$SENT_COUNT" ]; then
        echo "  converged: received $cur_count/$SENT_COUNT"
        break
    fi
    if [ "$cur_count" -eq "$prev_count" ]; then
        stable_ticks=$((stable_ticks + 1))
    else
        stable_ticks=0
    fi
    prev_count=$cur_count
    # Stop early only once the count has stopped moving for 3s straight —
    # otherwise keep waiting out the full timeout for a slow replay round.
    if [ "$stable_ticks" -ge 3 ]; then
        echo "  received count stable at $cur_count/$SENT_COUNT for 3s — stopping wait early"
        break
    fi
done

# Stop every remaining process now, BEFORE scoring — not in the EXIT trap —
# so the received log is quiescent while we read it. Reading it twice
# (once for line count, once for the sequence/duplicate check) while
# order-receiver is still actively appending is a race that can produce
# nonsensical results (e.g. a negative duplicate count).
for pid in "${PIDS[@]}"; do
    kill -9 "$pid" 2>/dev/null || true
done
sleep 0.3

# ── Score the run ─────────────────────────────────────────────────────────

# RECEIVED_COUNT + sequence check: order-receiver's log is still one
# space-separated text line per order (order_id is the first field), so it
# remains directly inspectable.
RECEIVED_COUNT=0
DUPLICATE_COUNT=0
MISSING_RANGES=""
if [ -f "$RECEIVED_LOG" ]; then
    RECEIVED_COUNT=$(wc -l < "$RECEIVED_LOG" | tr -d ' ')
    UNIQUE_COUNT=$(awk '{print $1}' "$RECEIVED_LOG" | sort -n -u | wc -l | tr -d ' ')
    DUPLICATE_COUNT=$((RECEIVED_COUNT - UNIQUE_COUNT))
    MISSING_RANGES=$(awk '{print $1}' "$RECEIVED_LOG" | sort -n -u | awk '
        NR==1 { prev=$1; first=$1; next }
        { if ($1 != prev+1) { print first"-"prev; first=$1 } prev=$1 }
        END { if (NR>0) print first"-"prev }
    ' | awk -v max="$SENT_COUNT" '
        BEGIN { last_end=0 }
        {
            split($0, r, "-"); start=r[1]; end=r[2];
            if (start > last_end+1) print (last_end+1)"-"(start-1);
            last_end=end;
        }
        END { if (max > last_end) print (last_end+1)"-"max }
    ')
fi

LOSS_COUNT=$((SENT_COUNT - RECEIVED_COUNT))
if [ "$SENT_COUNT" -gt 0 ]; then
    LOSS_PERCENT=$(awk "BEGIN {printf \"%.4f\", ($LOSS_COUNT / $SENT_COUNT) * 100}")
else
    LOSS_PERCENT="0.0000"
fi

SENT_TPS=$((SENT_COUNT / DURATION))
RECEIVED_TPS=$((RECEIVED_COUNT / DURATION))

echo "=========================================================="
echo " RESULTS SUMMARY:"
echo " Total Orders Sent      : $SENT_COUNT  (~${SENT_TPS} orders/sec)"
echo " Total Results Received : $RECEIVED_COUNT  (~${RECEIVED_TPS} orders/sec)"
echo " Sent - Received        : $LOSS_COUNT  (${LOSS_PERCENT}%) — at end of drain window; see note below"
echo " Duplicate order_ids in received log : $DUPLICATE_COUNT"
if [ -n "$MISSING_RANGES" ]; then
    echo " Missing order_id ranges (still outstanding at scoring time):"
    echo "$MISSING_RANGES" | sed 's/^/   /'
else
    echo " Missing order_id ranges : none"
fi
echo "=========================================================="
if [ -n "$MISSING_RANGES" ]; then
    echo " NOTE: convergence wait (${CONVERGENCE_TIMEOUT}s cap) ended with gaps still"
    echo " outstanding above — either the replay protocol needs longer than this"
    echo " harness waited, or there's a real bug. Re-run with a longer"
    echo " CONVERGENCE_TIMEOUT / inspect $BENCH_DIR/*.out before concluding loss."
fi
echo "=========================================================="
