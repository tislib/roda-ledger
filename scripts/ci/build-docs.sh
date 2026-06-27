#!/usr/bin/env bash
# Build the mdBook docs site into docs/book (matches the Deploy Docs workflow).
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  echo "Usage: $(basename "$0") [extra mdbook build args]" >&2
  exit 0
fi

command -v mdbook >/dev/null 2>&1 || {
  echo "error: mdbook not found. Install: cargo install mdbook --locked" >&2
  exit 1
}

echo "==> mdbook build" >&2
exec mdbook build "$@"
