#!/usr/bin/env bash
# Install the Hetzner Cloud CLI (hcloud), used to provision/destroy demo & test servers.
set -euo pipefail
# shellcheck source=scripts/util/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

BIN_DIR=/usr/local/bin
URL=https://github.com/hetznercloud/cli/releases/latest/download/hcloud-linux-amd64.tar.gz

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") [--bin-dir DIR]

Install the latest hcloud CLI (linux/amd64). No-op if hcloud is already present.

Options:
  --bin-dir DIR   Install location (default: $BIN_DIR). Uses sudo if not writable.
  -h, --help      Show this help.
EOF
}

while [ $# -gt 0 ]; do
  case $1 in
    --bin-dir) BIN_DIR=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; die "unknown argument: $1" ;;
  esac
done

if command -v hcloud >/dev/null 2>&1; then
  log "hcloud already installed: $(command -v hcloud)"
  exit 0
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

log "Installing hcloud CLI to $BIN_DIR ..."
# Download to a file first so curl can retry transient GitHub blips cleanly — piping
# a failed download into tar yields the cryptic "gzip: stdin: not in gzip format".
curl --fail --silent --show-error --location \
  --retry 5 --retry-delay 2 --retry-all-errors \
  -o "$tmp/hcloud.tar.gz" "$URL"
tar -xzf "$tmp/hcloud.tar.gz" -C "$tmp" hcloud

if [ -w "$BIN_DIR" ]; then
  mv "$tmp/hcloud" "$BIN_DIR/hcloud"
else
  sudo mv "$tmp/hcloud" "$BIN_DIR/hcloud"
fi
hcloud version
