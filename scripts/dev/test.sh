#!/usr/bin/env bash
# Run the workspace test suite on the dev box (sync + nextest, ci profile).
set -euo pipefail
# shellcheck source=scripts/dev/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0") [extra nextest args]" >&2; exit 0 ;;
esac

dev_run cargo nextest run --workspace --cargo-profile ci "$@"
