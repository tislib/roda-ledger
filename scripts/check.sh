#!/usr/bin/env bash
# Full local gate (matches CI). Delegates to scripts/ci/*; run those individually
# for a single check. See scripts/ci/README.md.
set -euo pipefail
cd "$(dirname "$0")"

ci/fmt.sh
ci/clippy.sh
ci/test.sh
ci/doctest.sh

echo "All checks passed!"
