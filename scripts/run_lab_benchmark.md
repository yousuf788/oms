# 300k orders/sec lab benchmark runbook

Run on the real 3-machine deployment (Nitin / Amit / Yousuf, per docs/HLD.md).
Each machine already has its own `order-process/.env` (real IPs) from prior
setup — do not use `.env.example` or `run_benchmark.sh`'s isolated ports for
this run.

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

    wc -l order-sending/logs/orders-sent.log
    wc -l order-process/logs/orders-processed*.log   # sum across all 3 nodes' WAL-adjacent logs if present
    wc -l order-receiver/logs/orders-received.log

Success: sent count == received count (processed count on the leader's own
WAL should also match — a follower's replicated WAL is not a duplicate
count, it's the same logical entries).

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
  records. Delete any `logs/*.log` / benchmark WAL dirs left over from
  earlier runs before starting.
- All three legs (order-sending, order-process, order-receiver) now use
  `bincode` on the wire; order-sending and order-process's inbound order
  channel additionally carry an HMAC-SHA256 signature (`CLUSTER_HMAC_KEY`)
  — unrelated to the binary encoding, already present before this work.
