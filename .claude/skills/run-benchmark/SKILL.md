---
name: run-benchmark
description: Run automated performance benchmarks, measure TPS, and verify packet loss limits
---

# Skill: Run Benchmarks

Use this skill when benchmarking order processing capacity, TPS throughput, packet loss, or latency metrics across the pipeline.

## Workflow Steps

### Step 1: Run Automated Benchmark Script

```bash
# Benchmark Syntax: ./scripts/run_benchmark.sh <num_nodes> <num_sender_threads> <duration_seconds>

# Run 1-node baseline (1 S2 node, 4 sender threads, 10s duration)
/data/Antier-project/Exchange/oms/scripts/run_benchmark.sh 1 4 10

# Run 3-node Raft consensus cluster benchmark (3 S2 nodes, 8 sender threads, 10s duration)
/data/Antier-project/Exchange/oms/scripts/run_benchmark.sh 3 8 10
```

### Step 2: Rate-Paced Zero Packet Loss Verification
To achieve **0.00% packet loss**, ensure `TARGET_TPS` in `order-sending/.env` matches processor throughput capacity (~5,000 orders/sec default):

1. Set `TARGET_TPS=5000` in `order-sending/.env`.
2. Run S3 receiver: `cd order-receiver && cargo run --release`.
3. Run 3 S2 nodes via `./starter.sh 1|2|3`.
4. Run S1 sender: `cd order-sending && cargo run --release`.
5. Observe throughput logs:
```text
[order-sending] throughput:     5000 orders/sec  total: 50000
[order-receiver] throughput:    5000 results/sec total: 50000
```

### Step 3: Inspect Benchmark History & Limits
Review baseline numbers and bottleneck analysis in [`docs/BENCHMARK.md`](file:///data/Antier-project/Exchange/oms/docs/BENCHMARK.md).
