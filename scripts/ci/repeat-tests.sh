#!/usr/bin/env bash
# Run the test suite COUNT times, stopping on the first failure (flake hunt).
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

COUNT=${REPEAT_COUNT:-10}
case ${1:-} in
  -h|--help) echo "Usage: $(basename "$0") [COUNT]   (default $COUNT; env: REPEAT_COUNT)" >&2; exit 0 ;;
  ?*) COUNT=$1 ;;
esac

command -v cargo-nextest >/dev/null 2>&1 || {
  echo "error: cargo-nextest not found. Install: cargo install cargo-nextest --locked" >&2
  exit 1
}

# Collapsible groups in CI; plain headers locally.
gh_group() { if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::group::$*"; else echo "--- $* ---"; fi; }
gh_end()   { if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::endgroup::"; fi; }

for i in $(seq 1 "$COUNT"); do
  gh_group "Iteration $i / $COUNT — nextest"
  cargo nextest run --workspace --cargo-profile ci
  gh_end
  gh_group "Iteration $i / $COUNT — doctests"
  cargo test --doc --profile ci
  gh_end
done
