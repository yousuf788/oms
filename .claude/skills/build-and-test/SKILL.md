---
name: build-and-test
description: Build, compile check, lint, and test all microservices in the Rust OMS codebase
---

# Skill: Build & Test Microservices

Use this skill whenever building, running syntax checks, or linting code across `order-process`, `order-sending`, `order-receiver`, and `order-monitoring`.

## Workflow Steps

### Step 1: Run Syntax & Type Checks
Execute `cargo check` across all crates to ensure there are no compilation errors:

```bash
cd /data/Antier-project/Exchange/oms/order-process && cargo check
cd /data/Antier-project/Exchange/oms/order-sending && cargo check
cd /data/Antier-project/Exchange/oms/order-receiver && cargo check
cd /data/Antier-project/Exchange/oms/order-monitoring && cargo check
```

### Step 2: Run Lints with Clippy
Enforce zero warnings across all crates:

```bash
cd /data/Antier-project/Exchange/oms/order-process && cargo clippy --all-targets -- -D warnings
cd /data/Antier-project/Exchange/oms/order-sending && cargo clippy --all-targets -- -D warnings
cd /data/Antier-project/Exchange/oms/order-receiver && cargo clippy --all-targets -- -D warnings
cd /data/Antier-project/Exchange/oms/order-monitoring && cargo clippy --all-targets -- -D warnings
```

> [!NOTE]
> As of this writing, `order-process`, `order-sending`, and `order-monitoring` each have a
> handful of **pre-existing** `-D warnings` violations (naming-convention issues from the
> order-witness→order-monitoring rename's inconsistent casing, plus a couple of unrelated style
> lints in `order-process/src/wal.rs`) that predate the sequencing/replay work. `order-receiver`
> is clean. Don't assume a clippy failure in those three crates is something you just broke —
> check `git blame`/diff the specific flagged lines before treating it as a regression.

### Step 3: Run Unit & Integration Tests
Execute cargo unit test suites:

```bash
cd /data/Antier-project/Exchange/oms/order-process && cargo test
cd /data/Antier-project/Exchange/oms/order-sending && cargo test
cd /data/Antier-project/Exchange/oms/order-receiver && cargo test
cd /data/Antier-project/Exchange/oms/order-monitoring && cargo test
```

### Step 4: Build Release Binaries
Compile optimized release binaries:

```bash
cd /data/Antier-project/Exchange/oms/order-process && cargo build --release
cd /data/Antier-project/Exchange/oms/order-sending && cargo build --release
cd /data/Antier-project/Exchange/oms/order-receiver && cargo build --release
cd /data/Antier-project/Exchange/oms/order-monitoring && cargo build --release
```
