#!/usr/bin/env bash
# Run load scenarios locally (no remote box). With a scenario name, run just that
# one; with no name, run the whole load group.
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0") [scenario-name] [extra scenario args]" >&2; exit 0 ;;
esac

cargo build -p cluster --bin roda-server --release --all-features

# A leading non-flag arg names a single scenario; otherwise run the whole group.
if [ -n "${1:-}" ] && [ "${1#-}" = "$1" ]; then
  name=$1; shift
  exec cargo run --package control --bin scenario --release -- run "$name" "$@"
fi

exec cargo run --package control --bin scenario --release -- run-all --group load "$@"
