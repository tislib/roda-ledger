#!/usr/bin/env bash
# Run the e2e scenario suite locally (no remote box).
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0") [extra scenario args]" >&2; exit 0 ;;
esac

exec cargo run --package control --bin scenario --release -- run-all --group e2e "$@"
