#!/usr/bin/env bash
# Destroy a Hetzner server and its SSH key (both named NAME). Idempotent.
set -euo pipefail
# shellcheck source=scripts/util/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

NAME=${SERVER_NAME:-}

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") --name NAME

Delete the server and SSH key named NAME. Never fails if they are already gone.
Requires: HCLOUD_TOKEN, hcloud.

Options:
  --name NAME   Server + SSH-key name (env: SERVER_NAME).
  -h, --help    Show this help.
EOF
}

while [ $# -gt 0 ]; do
  case $1 in
    --name) NAME=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; die "unknown argument: $1" ;;
  esac
done

[ -n "$NAME" ] || { usage; die "--name is required"; }
require_env HCLOUD_TOKEN
require_cmd hcloud "install with scripts/util/hcloud-install.sh"
export HCLOUD_TOKEN

log "Destroying server $NAME ..."
hcloud server delete "$NAME" || true
log "Cleaning up SSH key $NAME ..."
hcloud ssh-key delete "$NAME" || true
log "Cleanup complete"
