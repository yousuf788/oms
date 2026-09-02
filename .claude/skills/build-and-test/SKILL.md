---
name: build-and-test
description: Build, compile check, lint, and test all microservices in the Rust OMS codebase
---

# Skill: Build & Test Microservices

Use this skill whenever building, running syntax checks, or linting code across `order-process`, `order-sending`, `order-receiver`, and `order-witness`.

## Workflow Steps

### Step 1: Run Syntax & Type Checks
Execute `cargo check` across all crates to ensure there are no compilation errors:

```bash
cd /data/Antier-project/Exchange/oms/order-process && cargo check
cd /data/Antier-project/Exchange/oms/order-sending && cargo check
cd /data/Antier-project/Exchange/oms/order-receiver && cargo check
cd /data/Antier-project/Exchange/oms/order-witness && cargo check
```

### Step 2: Run Lints with Clippy
Enforce zero warnings across all crates:

```bash
cd /data/Antier-project/Exchange/oms/order-process && cargo clippy --all-targets -- -D warnings
cd /data/Antier-project/Exchange/oms/order-sending && cargo clippy --all-targets -- -D warnings
cd /data/Antier-project/Exchange/oms/order-receiver && cargo clippy --all-targets -- -D warnings
cd /data/Antier-project/Exchange/oms/order-witness && cargo clippy --all-targets -- -D warnings
```

### Step 3: Run Unit & Integration Tests
Execute cargo unit test suites:

```bash
cd /data/Antier-project/Exchange/oms/order-process && cargo test
cd /data/Antier-project/Exchange/oms/order-sending && cargo test
cd /data/Antier-project/Exchange/oms/order-receiver && cargo test
cd /data/Antier-project/Exchange/oms/order-witness && cargo test
```

### Step 4: Build Release Binaries
Compile optimized release binaries:

```bash
cd /data/Antier-project/Exchange/oms/order-process && cargo build --release
cd /data/Antier-project/Exchange/oms/order-sending && cargo build --release
cd /data/Antier-project/Exchange/oms/order-receiver && cargo build --release
cd /data/Antier-project/Exchange/oms/order-witness && cargo build --release
```
