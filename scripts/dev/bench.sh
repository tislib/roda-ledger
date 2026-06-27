#!/usr/bin/env bash
# Run a criterion bench on the dev box (sync + cargo bench -p ledger).
set -euo pipefail
# shellcheck source=scripts/dev/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

BENCH=${RODA_BENCH:-transaction_runner_bench}
case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0") [BENCH]   (default: $BENCH; '' runs all; env: RODA_BENCH)" >&2; exit 0 ;;
  ?*) BENCH=$1 ;;
esac

if [ -n "$BENCH" ]; then
  dev_run cargo bench -p ledger --bench "$BENCH"
else
  dev_run cargo bench -p ledger
fi
