#!/usr/bin/env bash
# Lint — matches CI (clippy over the workspace, warnings as errors).
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  echo "Usage: $(basename "$0") [extra cargo args, inserted before --]" >&2
  exit 0
fi

echo "==> cargo clippy --workspace --all-targets --all-features -- -D warnings" >&2
exec cargo clippy --workspace --all-targets --all-features "$@" -- -D warnings
