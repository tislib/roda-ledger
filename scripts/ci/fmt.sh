#!/usr/bin/env bash
# Check formatting — matches CI (cargo fmt --all --check).
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  echo "Usage: $(basename "$0") [extra cargo-fmt args]" >&2
  exit 0
fi

echo "==> cargo fmt --all --check" >&2
exec cargo fmt --all --check "$@"
