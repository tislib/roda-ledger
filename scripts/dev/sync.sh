#!/usr/bin/env bash
# Mirror the working tree to the remote dev box. See scripts/dev/README.md.
set -euo pipefail
# shellcheck source=scripts/dev/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0")   (config: RODA_DEV_HOST, RODA_DEV_PATH)" >&2; exit 0 ;;
esac

dev_sync
