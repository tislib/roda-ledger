#!/usr/bin/env bash
# Tests — matches CI (nextest, ci profile, all features).
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  echo "Usage: $(basename "$0") [extra nextest args]" >&2
  exit 0
fi

command -v cargo-nextest >/dev/null 2>&1 || {
  echo "error: cargo-nextest not found. Install: cargo install cargo-nextest --locked" >&2
  exit 1
}

echo "==> cargo nextest run --workspace --cargo-profile ci --all-features" >&2
exec cargo nextest run --workspace --cargo-profile ci --all-features "$@"
