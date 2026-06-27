#!/usr/bin/env bash
# Sync the working tree, then run any command on the dev box. See scripts/dev/README.md.
set -euo pipefail
# shellcheck source=scripts/dev/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

if [ $# -eq 0 ] || [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  echo "Usage: $(basename "$0") <command...>   (run in \$RODA_DEV_PATH on \$RODA_DEV_HOST)" >&2
  exit 0
fi

dev_run "$@"
