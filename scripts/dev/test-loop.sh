#!/usr/bin/env bash
# Loop a test command until it fails (flake hunt). Default: cluster tests, single-threaded.
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0") [cargo test args...]   (loops until the first failure)" >&2; exit 0 ;;
esac

if [ $# -gt 0 ]; then
  while cargo test "$@"; do :; done
else
  while cargo test --release --features cluster cluster -- --test-threads=1; do :; done
fi
