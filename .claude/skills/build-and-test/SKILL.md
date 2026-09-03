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
> All four crates are warning-free under plain `cargo build` and clean under
> `cargo clippy --all-targets -- -D warnings` as of this writing. This includes the
> order-witness→order-monitoring rename's naming-convention warnings (`monitoringConfig`,
> `monitoring_KEY`, `monitoringClient`, `monitoringUnreachable`) and a batch of unrelated
> pre-existing clippy-only lints (`div_ceil`, `len_without_is_empty`, `explicit_counter_loop`,
> `while_let_loop`, `default_constructed_unit_structs`, `new_without_default`, `identical_blocks`,
> `format_in_format_args`) that predated this — all fixed as plain refactors with no behavior
> change (verified: the `div_ceil` rewrite in particular was checked against the original
> integer-division formula for all n before being applied, since it's Raft quorum math). If
> clippy ever fails again, it's a genuine regression to investigate, not expected pre-existing
> debt — this note used to say otherwise; it doesn't anymore.
>
> This is unrelated to the env var casing quirk documented in `CLAUDE.md` §6
> (`REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER` etc.), which is still real, still exact-cased, and
> was deliberately left alone — it's a load-bearing external config contract, not a cosmetic
> Rust identifier.

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
