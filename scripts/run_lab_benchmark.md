# 300k orders/sec lab benchmark runbook

Run on the real 3-machine deployment (Nitin / Amit / Yousuf, per docs/HLD.md).
Each machine already has its own `order-process/.env` (real IPs) from prior
setup — do not use `.env.example` or `run_benchmark.sh`'s isolated ports for
this run.

**Before starting, confirm every machine's `.env` has the newer required vars**
(added for the sequencing/gap-detection/replay protocol — see `docs/HLD.md` §7):
- `order-process/.env` on every node: `S1_HOST` / `S1_REPLAY_PORT` (where
  order-sending's replay listener is) and `NODE1/2/3_REPLAY_PORT`
  (default 6201/6202/6203 — only needs setting if you're not using the defaults).
- `order-receiver/.env`: now needs `CLUSTER_HMAC_KEY` (it verifies the result
  channel and signs its own replay requests — previously needed no HMAC key at
  all) and the full `NODE1/2/3_HOST` + `NODE1/2/3_REPLAY_PORT` list, to
  broadcast REPLAY_REQUEST to the S2 cluster.
- Casing on `REQUIRE_monitoring_FOR_SINGLE_NODE_LEADER` is exact
  (`monitoring`, not `MONITORING`) — see `CLAUDE.md` §6 if this node should be
  allowed to self-promote alone and isn't.

Missing any of the above causes a startup panic (`order-process`/`order-receiver`)
naming the exact variable, not silent misbehavior — but confirm before a long run.

## 1. Rebuild release binaries on every machine

On **each** of Nitin, Amit, Yousuf:

    cd order-process && cargo build --release
    cd ../order-sending && cargo build --release   # Yousuf only (S1 lives here)
    cd ../order-receiver && cargo build --release  # Yousuf only (S3 lives here)

## 2. Start the Aeron Media Driver on every machine

On **each** of Nitin, Amit, Yousuf:

    ./scripts/start-media-driver.sh

Confirm each one is actually up before proceeding:

    ls "$AERON_DIR"/cnc.dat   # AERON_DIR defaults to /dev/shm/aeron-$(id -u)

## 3. Start order-process on all 3 nodes

On Nitin:    `cd order-process && ./starter.sh 1`
On Amit:     `cd order-process && ./starter.sh 2`
On Yousuf:   `cd order-process && ./starter.sh 3`

Wait for a `[role] ... is LEADER` line to appear on all three consoles
before continuing (confirms the cluster has elected a leader).

## 4. Start order-receiver on Yousuf

    cd order-receiver && ./target/release/order-receiver

## 5. Start order-sending on Yousuf, driving 300k/sec

    cd order-sending
    TARGET_TPS=300000 SENDER_THREADS=32 ./target/release/order-sending

(`order-sending/.env` already sets `TARGET_TPS=300000`; the env vars above
are only needed to override it for a different run.)

Let it run for at least 30 seconds before stopping (Ctrl-C on order-sending
first, then wait a few seconds for order-process/order-receiver to drain
in-flight orders before stopping them).

## 6. Score the run

`orders-sent.wal` and `orders-processed*.log` are binary (length-prefixed
bincode) now — `wc -l` no longer works on them. Options:

    # Sent count: parse the binary WAL directly (see scripts/run_benchmark.sh's
    # SENT_COUNT logic for the exact parser), or read order-sending's own
    # periodic stdout throughput line (approximate — see its caveat below).

    # Received count + sequence check (still text, one line per order,
    # order_id is the first field):
    wc -l order-receiver/logs/orders-received.log
    awk '{print $1}' order-receiver/logs/orders-received.log | sort -n -u | wc -l   # unique count — compare to line count for duplicates

Success: sent count == received count, zero duplicates, and no gaps in the
sorted `order_id` sequence in `orders-received.log`. Give the replay protocol
time to converge before declaring loss — a gap right at the tail, sized
roughly like the last flush interval, is expected from `order-sending`'s
async WAL batching and a hard process stop, not real loss; a gap that
doesn't shrink over 30-60s of waiting is real and worth investigating.

## 7. If short of 300k/sec

Expected on the first run. Things to check, in order of likely impact:
- `order-sending`'s per-second throughput printout (stats thread) — is the
  bottleneck at the sender, or is send throughput fine but processed/received
  falling behind?
- CPU on Yousuf specifically — it runs order-sending + order-process (if it's
  leader) + order-receiver simultaneously (see
  `docs/superpowers/specs/2026-09-01-oms-300k-throughput-design.md`'s flagged
  co-location constraint). `top`/`htop` during a run will show if this
  single machine is the ceiling.
- Raise `SENDER_THREADS` if generator threads (not the fan-out/publisher
  threads) are the bottleneck.
- Actual LAN bandwidth between the 3 machines — `iperf3` between
  Nitin/Amit/Yousuf if throughput plateaus well under 300k/sec despite low
  CPU usage everywhere.

## Notes

- Existing on-disk WAL files from before this change are not compatible —
  the WAL format switched from JSON-lines to length-prefixed `bincode`
  records. Delete any `logs/*.log`, `logs/*.wal`, `logs/receiver-checkpoint.dat`,
  or benchmark WAL dirs left over from earlier runs before starting a clean
  measurement (a stale `receiver-checkpoint.dat` in particular will make
  order-receiver think it's already seen everything up to that watermark).
- All three legs (order-sending, order-process, order-receiver) now use
  `bincode` on the wire; every channel is HMAC-signed with `CLUSTER_HMAC_KEY`
  — the order channel and Raft control messages (present before this work),
  plus the result channel and both REPLAY_REQUEST control channels (added
  with the sequencing/replay protocol — order-receiver did not carry any
  HMAC key before this).
- Unlike `scripts/run_benchmark.sh` (which widens Raft heartbeat/election
  timeouts to cope with 3 nodes + sender + receiver sharing one machine's
  CPU), the real 3-machine deployment should NOT need that — each node gets
  dedicated hardware, so the tighter `.env` defaults (50ms/150-300ms) should
  hold. If you see leadership flapping (`[role]` cycling between all 3 names
  repeatedly) on real hardware, that's a different, more concerning signal
  than the single-machine CPU-contention explanation — investigate network/
  clock/CPU issues on the affected machine rather than just widening timeouts.
- If throughput plateaus below target with a gap that keeps growing (not
  shrinking) in `orders-received.log`, you've found this deployment's real
  ceiling for now — see `docs/BENCHMARK.md` §0 for what that looked like on
  a single shared machine, for comparison.
