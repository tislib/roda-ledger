#!/usr/bin/env bash
# Runs ON a fresh Ubuntu server (streamed in via util/run-remote.sh). Builds the
# binaries and runs every e2e scenario against a fresh N-node localhost cluster.
# Required env: REPO, BRANCH, NODES.
set -euo pipefail

# Install dependencies
apt-get update -qq
apt-get install -y -qq build-essential pkg-config libssl-dev protobuf-compiler git curl

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y -q

# Clone repository
git clone --branch "$BRANCH" --depth 1 "https://github.com/$REPO.git" /root/roda-ledger
cd /root/roda-ledger

# Pre-build both bins so `scenario` can resolve its sibling
# `roda-server` via `current_exe()` on the first spawn —
# they land in the same target/release/ directory.
# `-p` is required: `--bin scenario` alone searches only the
# workspace's default-run packages, which doesn't include
# `control`.
/root/.cargo/bin/cargo build --release \
  -p cluster --bin roda-server \
  -p control --bin scenario

# Run every e2e scenario against a fresh N-node localhost
# cluster per scenario. Each scenario provisions its own
# cluster via the process provisioner, runs to completion,
# tears down. NODES defaults to 3 (set by the workflow
# input) — failover scenarios need ≥ 3 for quorum.
echo "::group::E2E Test Output"
/root/.cargo/bin/cargo run --release \
  -p control --bin scenario \
  -- --nodes "$NODES" run-all --group e2e 2>&1 \
  | tee /root/e2e-test-output.txt
echo "::endgroup::"
