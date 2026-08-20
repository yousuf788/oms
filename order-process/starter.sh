#!/usr/bin/env bash
set -euo pipefail

SERVICE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SERVICE_DIR"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

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

NODE_ID="${1:-${NODE_ID:-1}}"
if [[ ! "$NODE_ID" =~ ^[123]$ ]]; then
  echo "[starter] Invalid NODE_ID: $NODE_ID. Use 1, 2, or 3."
  exit 1
fi

echo "[starter] Starting order-process with NODE_ID=$NODE_ID"
cargo run --release --bin order-process
