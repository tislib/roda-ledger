#!/usr/bin/env bash
set -eu

# Ensure we're in the project root
cd "$(dirname "$0")/.."

if ! command -v cargo-nextest >/dev/null 2>&1; then
    echo "error: cargo-nextest not found. Install with:" >&2
    echo "  cargo install cargo-nextest --locked" >&2
    exit 1
fi

echo "Running rustfmt..."
cargo fmt --all --check

echo "Running clippy..."
cargo clippy --workspace --all-targets --all-features -- -D warnings

echo "Running tests..."
cargo nextest run --workspace --cargo-profile ci

echo "Running doctests..."
cargo test --doc --profile ci

echo "All checks passed!"
