#!/usr/bin/env bash
# install-aeron-deps.sh
# Creates local symlinks for libuuid and libbsd that rusteron-client needs at link time.
# Does NOT require sudo. Run once on each machine before building.

set -e
mkdir -p /tmp/oms-libs

UUID_SO=$(find /usr/lib/x86_64-linux-gnu -name "libuuid.so.*" | sort | tail -1)
BSD_SO=$(find /usr/lib/x86_64-linux-gnu  -name "libbsd.so.*"  | sort | tail -1)

[ -z "$UUID_SO" ] && { echo "ERROR: libuuid not found. Install: sudo apt-get install libuuid1"; exit 1; }
[ -z "$BSD_SO"  ] && { echo "ERROR: libbsd not found.  Install: sudo apt-get install libbsd0"; exit 1; }

ln -sf "$UUID_SO" /tmp/oms-libs/libuuid.so
ln -sf "$BSD_SO"  /tmp/oms-libs/libbsd.so

echo "[deps] Symlinks created in /tmp/oms-libs:"
ls -la /tmp/oms-libs/
echo "[deps] Ready — you can now run: cargo build --release"
