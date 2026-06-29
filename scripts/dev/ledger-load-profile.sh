#!/usr/bin/env bash
# Build the load binary with debug symbols, then run it locally (macOS profiling).
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0")   (rebuilds target/release/load with a dSYM, then runs it)" >&2; exit 0 ;;
esac

rm -rf target/release/load*
cargo build --package ledger --release --bin load
# dsymutil emits the .dSYM that samplers/Instruments need; macOS-only, skip elsewhere.
command -v dsymutil >/dev/null 2>&1 && dsymutil target/release/load

exec target/release/load
