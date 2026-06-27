#!/usr/bin/env bash
# Run the full check gate on the dev box (sync + scripts/check.sh).
set -euo pipefail
# shellcheck source=scripts/dev/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0")   (runs scripts/check.sh on \$RODA_DEV_HOST)" >&2; exit 0 ;;
esac

dev_run bash scripts/check.sh
