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
ldconfig -p 2>/dev/null | grep -q libuuid.so || missing_pkgs+=(libuuid1)
ldconfig -p 2>/dev/null | grep -q libbsd.so  || missing_pkgs+=(libbsd0)

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
driver_running=false
if [[ -f "$REPO_ROOT/scripts/media-driver.pid" ]]; then
  pid="$(cat "$REPO_ROOT/scripts/media-driver.pid" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    driver_running=true
  fi
fi
if [[ "$driver_running" == false ]] && [[ -f "$AERON_DIR/cnc.dat" ]]; then
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
  unset NODE_ID || true
  echo "[starter] Starting order-process (NODE_ID auto-detect from local IP)"
fi

# ── 7. Build + run latest code ───────────────────────────────────────────────
# `cargo run` always recompiles changed sources first, so this never launches
# a stale binary — it always starts whatever is currently checked out.
cargo run --release --bin order-process
