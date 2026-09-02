# Rule: Mandatory Development Workflow & Code Change Policy

This rule establishes the mandatory three-phase engineering protocol required before making any code modifications in this high-throughput Order Management System (OMS).

---

## 1. High Throughput Core Mandate (200,000 to 300,000 TPS)

This codebase is engineered to scale to **minimum 200,000 to 300,000 orders per second (200k–300k TPS)** under strict latency SLA constraints.

Every feature, refactoring, or bug fix MUST be designed with:
- Zero-copy data passing where possible.
- Lock-free or lock-minimized concurrent pipelines (`crossbeam-channel`).
- Zero-allocation hot paths (reusing pre-allocated vectors/buffers).
- Non-blocking asynchronous disk and logging streams.
- Amortized network RPC batching via Aeron.

---

## 2. Mandatory 3-Phase Pre-Change Protocol

> [!CAUTION]
> **DO NOT DIRECTLY MODIFY CODE FILES WITHOUT USER REVIEW & APPROVAL.**
> Claude Code / AI assistants MUST execute the following workflow for all non-trivial code modifications:

```
┌────────────────────────────────────────────────────────────────────────┐
│                        3-PHASE DEVELOPMENT WORKFLOW                    │
│                                                                        │
│  [PHASE 1: RESEARCH] ──► [PHASE 2: PROPOSAL & USER REVIEW] ──► [PHASE 3: EXECUTION] │
│  - Inspect codebase      - Formulate technical proposal       - Edit code files │
│  - Profile locks & I/O   - Present trade-offs to user         - Run cargo check │
│  - Analyze benchmarks    - Request manual verification &      - Run benchmarks  │
│                            suggestions                                 │
└────────────────────────────────────────────────────────────────────────┘
```

### Phase 1: Research & Bottleneck Analysis
- Thoroughly inspect relevant files across all affected crates (`order-process`, `order-sending`, `order-receiver`, `order-witness`).
- Analyze potential performance impacts on 200k–300k TPS target (lock contention, heap allocations, Aeron ring buffer pressure, bincode wire serialization).
- Review historical benchmarks (`docs/BENCHMARK.md`) and high-level design specs (`docs/HLD.md`).
- **NO SOURCE CODE CHANGES ARE PERMITTED DURING THIS PHASE.**

### Phase 2: Technical Proposal & User Review Request
- Present a clear, structured technical proposal to the user containing:
  1. Problem statement / optimization goal.
  2. Detailed proposed changes (struct modifications, channel buffers, lock strategies).
  3. Expected impact on 200k–300k TPS target and memory footprint.
  4. Specific open questions or manual verification steps for the user.
- **WAIT FOR EXPLICIT USER APPROVAL, FEEDBACK, OR SUGGESTIONS BEFORE PROCEEDING.**

### Phase 3: Approved Code Execution & Verification
- Modify code files strictly according to the approved plan.
- Maintain positional field alignment for `bincode` wire structs.
- Run `cargo check` and `cargo clippy` across all crates.
- Execute benchmarks (`./scripts/run_benchmark.sh`) to verify performance.
