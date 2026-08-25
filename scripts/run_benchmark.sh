#!/usr/bin/env bash
set -e

# OMS Performance Benchmark Script
# Usage: ./scripts/run_benchmark.sh [nodes: 1|3] [threads: N] [duration_sec: S]

NODES=${1:-1}
THREADS=${2:-4}
DURATION=${3:-10}
TARGET_TPS=${4:-5000}
export TARGET_TPS="$TARGET_TPS"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="$ROOT_DIR/target/benchmark_tmp"
rm -rf "$BENCH_DIR"
mkdir -p "$BENCH_DIR/receiver_logs" "$BENCH_DIR/sending_logs" "$BENCH_DIR/wal_data"

# Common localhost configuration
export BIND_HOST="127.0.0.1"
export NODE1_HOST="127.0.0.1"
export NODE1_NAME="Vivek"
export NODE1_RAFT_PORT="16001"
export NODE1_ORDER_PORT="17001"

export NODE2_HOST="127.0.0.1"
export NODE2_NAME="Amit"
export NODE2_RAFT_PORT="16002"
export NODE2_ORDER_PORT="17002"

export NODE3_HOST="127.0.0.1"
export NODE3_NAME="Yousuf"
export NODE3_RAFT_PORT="16003"
export NODE3_ORDER_PORT="17003"

export S3_HOST="127.0.0.1"
export S3_PORT="18001"

export HEARTBEAT_INTERVAL_MS="50"
export ELECTION_TIMEOUT_MIN_MS="150"
export ELECTION_TIMEOUT_MAX_MS="300"
export ALLOW_SINGLE_NODE_LEADER="true"
export PEER_SILENT_MS="1000"
export VERBOSE_RAFT="false"
export SENDER_THREADS="$THREADS"

PIDS=()

cleanup() {
    echo "Stopping benchmark processes..."
    for pid in "${PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

echo "=========================================================="
echo " Starting Benchmark Run:"
echo " Cluster Nodes : $NODES"
echo " Sender Threads: $THREADS"
echo " Duration      : ${DURATION}s"
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

# Give receiver / processor 1s to drain remaining buffers
sleep 1

# Gather metrics
SENT_COUNT=0
PROCESSED_COUNT=0
RECEIVED_COUNT=0

if [ -f "$ROOT_DIR/order-sending/logs/orders-sent.log" ]; then
    SENT_COUNT=$(wc -l < "$ROOT_DIR/order-sending/logs/orders-sent.log" | tr -d ' ')
fi

if [ -f "$BENCH_DIR/wal_data/orders-processed-s2-1.log" ]; then
    PROCESSED_COUNT=$(wc -l < "$BENCH_DIR/wal_data/orders-processed-s2-1.log" | tr -d ' ')
elif [ -f "$ROOT_DIR/order-process/logs/orders-processed.log" ]; then
    PROCESSED_COUNT=$(wc -l < "$ROOT_DIR/order-process/logs/orders-processed.log" | tr -d ' ')
fi

if [ -f "$ROOT_DIR/order-receiver/logs/orders-received.log" ]; then
    RECEIVED_COUNT=$(wc -l < "$ROOT_DIR/order-receiver/logs/orders-received.log" | tr -d ' ')
fi

SENT_TPS=$((SENT_COUNT / DURATION))
PROCESSED_TPS=$((PROCESSED_COUNT / DURATION))

LOSS_COUNT=$((SENT_COUNT - PROCESSED_COUNT))
if [ "$SENT_COUNT" -gt 0 ]; then
    LOSS_PERCENT=$(awk "BEGIN {printf \"%.2f\", ($LOSS_COUNT / $SENT_COUNT) * 100}")
else
    LOSS_PERCENT="0.00"
fi

WAL_SIZE_KB=0
if [ -f "$BENCH_DIR/wal_data/orders-processed-s2-1.log" ]; then
    WAL_SIZE_KB=$(du -k "$BENCH_DIR/wal_data/orders-processed-s2-1.log" | cut -f1)
elif [ -f "$ROOT_DIR/order-process/logs/orders-processed.log" ]; then
    WAL_SIZE_KB=$(du -k "$ROOT_DIR/order-process/logs/orders-processed.log" | cut -f1)
fi

echo "=========================================================="
echo " RESULTS SUMMARY:"
echo " Total Orders Sent      : $SENT_COUNT  (${SENT_TPS} orders/sec)"
echo " Total Orders Processed : $PROCESSED_COUNT  (${PROCESSED_TPS} orders/sec)"
echo " Total Results Received : $RECEIVED_COUNT"
echo " Dropped Orders         : $LOSS_COUNT  (${LOSS_PERCENT}% packet loss)"
echo " WAL 1 File Size        : ${WAL_SIZE_KB} KB"
echo "=========================================================="

# Clean up logs created in working dirs for next benchmark
rm -f "$ROOT_DIR/order-sending/logs/orders-sent.log"
rm -f "$ROOT_DIR/order-process/logs/orders-processed.log"
rm -f "$ROOT_DIR/order-receiver/logs/orders-received.log"
