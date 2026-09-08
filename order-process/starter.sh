#!/usr/bin/env bash
# starter.sh — one-command bootstrap + run for order-process (S2 replica).
#
# Fresh clone → just run `./starter.sh` (or `./starter.sh 1|2|3`):
#   1. Bootstraps .env from .env.example if missing
#   2. Installs system packages needed to build rusteron-client (libuuid, libbsd, JRE)
#   3. Creates the /tmp/oms-libs symlinks cargo needs at link time
#   4. Installs Rust via rustup if missing
#   5. Starts the Aeron Media Driver (if not already running)
#   6. Builds and runs the current source (`cargo run --release` always rebuilds
#      on changes, so this always starts the latest code — never a stale binary)
set -euo pipefail

SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SERVICE_DIR/.." && pwd)"
cd "$SERVICE_DIR"

# ── 1. .env bootstrap ────────────────────────────────────────────────────────
if [[ ! -f ".env" ]]; then
  if [[ -f ".env.example" ]]; then
    echo "[starter] No .env found — copying .env.example -> .env (single-machine/localhost defaults)."
    echo "[starter] Edit .env with real NODE*_HOST values before deploying across machines."
    cp ".env.example" ".env"
  else
    echo "[starter] No .env or .env.example found in $SERVICE_DIR. Cannot continue."
    exit 1
  fi
fi
set -a
# shellcheck disable=SC1091
source ".env"
set +a

# ── 2. System packages (libuuid, libbsd, JRE for Aeron) ─────────────────────
missing_pkgs=()
command -v java >/dev/null 2>&1 || missing_pkgs+=(default-jre-headless)
# NOTE: use command substitution, not `| grep -q`, to check these — under
# `pipefail`, `grep -q` exits as soon as it finds a match and closes its end
# of the pipe, which can make the still-writing `ldconfig` process die from
# SIGPIPE and report a false failure even though the library is present.
[[ -n "$(ldconfig -p 2>/dev/null | grep libuuid.so)" ]] || missing_pkgs+=(libuuid1)
[[ -n "$(ldconfig -p 2>/dev/null | grep libbsd.so)"  ]] || missing_pkgs+=(libbsd0)

if [[ ${#missing_pkgs[@]} -gt 0 ]]; then
  if command -v apt-get >/dev/null 2>&1; then
    echo "[starter] Installing missing packages: ${missing_pkgs[*]}"
    if [[ $EUID -eq 0 ]]; then
      apt-get update -qq && apt-get install -y -qq "${missing_pkgs[@]}"
    elif command -v sudo >/dev/null 2>&1; then
      sudo apt-get update -qq && sudo apt-get install -y -qq "${missing_pkgs[@]}"
    else
      echo "[starter] Need root/sudo to install: ${missing_pkgs[*]}. Install manually and re-run."
      exit 1
    fi
  else
    echo "[starter] Missing packages (${missing_pkgs[*]}) and apt-get not found. Install manually and re-run."
    exit 1
  fi
fi

# ── 3. /tmp/oms-libs symlinks (rusteron-client link-time deps) ──────────────
if [[ ! -e /tmp/oms-libs/libuuid.so || ! -e /tmp/oms-libs/libbsd.so ]]; then
  echo "[starter] Creating /tmp/oms-libs symlinks via install-aeron-deps.sh"
  "$REPO_ROOT/scripts/install-aeron-deps.sh"
fi

# ── 4. Rust toolchain ─────────────────────────────────────────────────────
if ! command -v cargo >/dev/null 2>&1; then
  echo "[starter] Rust/Cargo not found. Installing via rustup..."
  if ! command -v curl >/dev/null 2>&1; then
    echo "[starter] curl is required to install Rust. Please install curl first."
    exit 1
  fi
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  # shellcheck disable=SC1090
  source "$HOME/.cargo/env"
fi

# ── 5. Aeron Media Driver ────────────────────────────────────────────────────
export AERON_DIR="${AERON_DIR:-/dev/shm/aeron-$(id -u)}"
# Check for a LIVE MediaDriver process bound to this AERON_DIR — not just the
# presence of cnc.dat, which is a memory-mapped file that survives on disk
# after the driver that created it has been killed (stale state otherwise
# makes this think a dead driver is still running, and the app then times out
# trying to connect to it).
driver_running=false
if pgrep -f "Daeron\.dir=${AERON_DIR}[^[:space:]]*.*MediaDriver" >/dev/null 2>&1; then
  driver_running=true
fi

if [[ "$driver_running" == true ]]; then
  echo "[starter] Aeron Media Driver already running (AERON_DIR=$AERON_DIR)."
else
  echo "[starter] Starting Aeron Media Driver..."
  "$REPO_ROOT/scripts/start-media-driver.sh"
fi

# ── 6. NODE_ID (optional override: ./starter.sh 1|2|3) ──────────────────────
if [[ $# -ge 1 ]]; then
  NODE_ID="$1"
  if [[ ! "$NODE_ID" =~ ^[123]$ ]]; then
    echo "[starter] Invalid NODE_ID: $NODE_ID. Use 1, 2, or 3 (or omit for auto-detect)."
    exit 1
  fi
  export NODE_ID
  echo "[starter] Starting order-process with NODE_ID=$NODE_ID"
else
  # Auto-detect: if all 3 NODE*_HOST values are the same (single-machine/localhost
  # demo), the Rust IP-match logic finds all 3 and panics. In that case, pick the
  # first node whose RAFT port is NOT already bound — that's the next free slot.
  H1="${NODE1_HOST:-127.0.0.1}"
  H2="${NODE2_HOST:-127.0.0.1}"
  H3="${NODE3_HOST:-127.0.0.1}"

  if [[ "$H1" == "$H2" && "$H2" == "$H3" ]]; then
    NODE_ID=""
    for try_id in 1 2 3; do
      port_var="NODE${try_id}_RAFT_PORT"
      port="${!port_var:-$((6000 + try_id))}"
      if ! ss -tulnp 2>/dev/null | grep -q ":${port} "; then
        NODE_ID="$try_id"
        break
      fi
    done
    if [[ -z "$NODE_ID" ]]; then
      echo "[starter] All 3 raft ports already in use. Are all 3 nodes already running?"
      exit 1
    fi
    export NODE_ID
    echo "[starter] Auto-assigned NODE_ID=$NODE_ID (first free raft port on localhost)"
  else
    # Multi-machine mode: let Rust auto-detect from local IP vs NODE*_HOST
    unset NODE_ID || true
    echo "[starter] Starting order-process (NODE_ID auto-detect from local IP)"
  fi
fi

# ── 7. Build + run latest code ───────────────────────────────────────────────
# `cargo run` always recompiles changed sources first, so this never launches
# a stale binary — it always starts whatever is currently checked out.
cargo run --release --bin order-process
