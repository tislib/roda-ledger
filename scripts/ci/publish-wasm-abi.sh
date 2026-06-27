#!/usr/bin/env bash
# Build, test, and publish the roda-wasm-abi guest crate (matches Publish workflow).
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../.."

DRY_RUN=0
usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") [--dry-run]

Test (host, with the 'tools' feature), build for wasm32, build the examples,
then publish roda-wasm-abi to crates.io. Needs the wasm32-unknown-unknown target
(rustup target add wasm32-unknown-unknown).

Options:
  --dry-run   cargo publish --dry-run; uploads nothing (no token needed).
  -h, --help  Show this help.

Env:
  CARGO_REGISTRY_TOKEN   crates.io API token (required unless --dry-run).
EOF
}
case ${1:-} in
  --dry-run) DRY_RUN=1 ;;
  -h|--help) usage; exit 0 ;;
  "") ;;
  *) usage; exit 1 ;;
esac

echo "==> cargo test -p roda-wasm-abi --features tools" >&2
cargo test -p roda-wasm-abi --features tools

echo "==> cargo build -p roda-wasm-abi --target wasm32-unknown-unknown" >&2
cargo build -p roda-wasm-abi --target wasm32-unknown-unknown

for ex in transfer counter; do
  echo "==> cargo build example ($ex) for wasm32" >&2
  cargo build --release --target wasm32-unknown-unknown \
    --manifest-path "crates/roda-wasm-abi/examples/$ex/Cargo.toml"
done

if [ "$DRY_RUN" = 1 ]; then
  echo "==> cargo publish -p roda-wasm-abi --dry-run" >&2
  exec cargo publish -p roda-wasm-abi --dry-run
fi

: "${CARGO_REGISTRY_TOKEN:?set CARGO_REGISTRY_TOKEN or pass --dry-run}"
echo "==> cargo publish -p roda-wasm-abi" >&2
exec cargo publish -p roda-wasm-abi
