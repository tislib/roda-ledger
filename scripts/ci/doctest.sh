#!/usr/bin/env bash
# Doctests — matches CI (ci profile).
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  echo "Usage: $(basename "$0") [extra cargo test args]" >&2
  exit 0
fi

echo "==> cargo test --doc --profile ci" >&2
exec cargo test --doc --profile ci "$@"
