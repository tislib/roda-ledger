#!/usr/bin/env bash
# Provision ("buy") a Hetzner server: create SSH key, upload it, create the server,
# wait until SSH is reachable. Prints the server's public IP to stdout (logs to stderr).
set -euo pipefail
# shellcheck source=scripts/util/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

NAME=${SERVER_NAME:-}
TYPE=${SERVER_TYPE:-cx22}
IMAGE=${SERVER_IMAGE:-ubuntu-24.04}
LOCATION=${SERVER_LOCATION:-fsn1}
KEY_PATH=
REPLACE=0

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") --name NAME [options]

Create a Hetzner server (and an SSH key of the same NAME) and wait for SSH.
Prints the public IPv4 to stdout. Requires: HCLOUD_TOKEN, hcloud, jq, ssh-keygen.

Options:
  --name NAME        Server + SSH-key name (env: SERVER_NAME).
  --type TYPE        Server type (default: $TYPE; env: SERVER_TYPE).
  --image IMAGE      OS image (default: $IMAGE; env: SERVER_IMAGE).
  --location LOC     Location (default: $LOCATION; env: SERVER_LOCATION).
  --key-path PATH    SSH private-key path (default: \$HOME/.ssh/roda_NAME).
  --replace          Delete any prior server/key of the same NAME first.
  -h, --help         Show this help.
EOF
}

while [ $# -gt 0 ]; do
  case $1 in
    --name) NAME=$2; shift 2 ;;
    --type) TYPE=$2; shift 2 ;;
    --image) IMAGE=$2; shift 2 ;;
    --location) LOCATION=$2; shift 2 ;;
    --key-path) KEY_PATH=$2; shift 2 ;;
    --replace) REPLACE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage; die "unknown argument: $1" ;;
  esac
done

[ -n "$NAME" ] || { usage; die "--name is required"; }
KEY_PATH=${KEY_PATH:-$HOME/.ssh/roda_$NAME}
require_env HCLOUD_TOKEN
require_cmd hcloud "install with scripts/util/hcloud-install.sh"
require_cmd jq
require_cmd ssh-keygen
export HCLOUD_TOKEN

if [ ! -f "$KEY_PATH" ]; then
  log "Generating SSH key at $KEY_PATH ..."
  mkdir -p "$(dirname "$KEY_PATH")"
  ssh-keygen -t ed25519 -f "$KEY_PATH" -N "" -q
fi

if [ "$REPLACE" = 1 ]; then
  log "Removing any prior server/key named $NAME ..."
  hcloud server delete "$NAME" >/dev/null 2>&1 || true
  hcloud ssh-key delete "$NAME" >/dev/null 2>&1 || true
fi

if ! hcloud ssh-key describe "$NAME" >/dev/null 2>&1; then
  hcloud ssh-key create --name "$NAME" --public-key "$(cat "$KEY_PATH.pub")" --output json >/dev/null
fi

log "Creating server $NAME ($TYPE, $IMAGE, $LOCATION) ..."
if ! resp=$(hcloud server create \
      --name "$NAME" --type "$TYPE" --image "$IMAGE" \
      --location "$LOCATION" --ssh-key "$NAME" --output json 2>&1); then
  log "::error::hcloud server create failed:"
  log "$resp"
  log "Server types available in $LOCATION:"
  hcloud server-type list -o columns=name,cores,cpu_type,memory,disk >&2 || true
  exit 1
fi
ip=$(echo "$resp" | jq -r '.server.public_net.ipv4.ip')
log "Server created at $ip"

log "Waiting for SSH to come up ..."
for i in $(seq 1 30); do
  if ssh "${SSH_OPTS[@]}" -o ConnectTimeout=5 -i "$KEY_PATH" "root@$ip" "echo ok" >/dev/null 2>&1; then
    log "Server is reachable"
    echo "$ip"
    exit 0
  fi
  log "Waiting for server... ($i/30)"
  sleep 10
done
die "Server not reachable after 5 minutes"
