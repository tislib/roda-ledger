#!/usr/bin/env bash
# Run the load_latency probe on the dev box (sync + cargo run).
set -euo pipefail
# shellcheck source=scripts/dev/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0") [load_latency args...]   (e.g. --wait-level commit)" >&2; exit 0 ;;
esac

dev_run cargo run -p ledger --release --bin load_latency -- "$@"
