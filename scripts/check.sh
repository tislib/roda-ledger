#!/usr/bin/env bash
set -eu

# Ensure we're in the project root
cd "$(dirname "$0")/.."

echo "Running rustfmt..."
cargo fmt --all --check

echo "Running clippy..."
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "Running tests..."
cargo test -p ledger --release
cargo test -p cluster --release -- --test-threads=1
cargo test -p raft --release
cargo test -p ctl --release
cargo test --doc

echo "All checks passed!"
