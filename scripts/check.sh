#!/usr/bin/env bash
set -euo pipefail

# Ensure we're in the project root
cd "$(dirname "$0")/.."

echo "Running rustfmt..."
cargo fmt --all --check

echo "Running clippy..."
cargo clippy --all-targets --all-features -- -D warnings

echo "Running tests..."
cargo test --workspace --all-targets --all-features
cargo test --doc

echo "All checks passed!"
