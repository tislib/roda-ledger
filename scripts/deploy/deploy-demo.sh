#!/usr/bin/env bash
# Full demo deploy: provision a Hetzner server, bring up the control+ui demo stack,
# smoke-check the UI. The server is PERSISTENT (not destroyed). See deploy/README.md.
set -euo pipefail
DEPLOY_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
UTIL=$DEPLOY_DIR/../util
# shellcheck source=scripts/util/common.sh
source "$UTIL/common.sh"

NAME=${SERVER_NAME:-roda-ledger-demo}
TYPE=${SERVER_TYPE:-CX33}
IMAGE=${SERVER_IMAGE:-ubuntu-24.04}
LOCATION=${SERVER_LOCATION:-fsn1}
REPO=${REPO:-}
BRANCH=${BRANCH:-}
CONTROL_IMAGE=${CONTROL_IMAGE:-tislib/roda-ledger:control-latest}
UI_IMAGE=${UI_IMAGE:-tislib/roda-ledger:ui-latest}
KEY_PATH=
DEST=results

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") [options]

Provision a Hetzner server and deploy the demo stack (control + ui). Writes
\$DEST/demo-info.env. Requires HCLOUD_TOKEN and the hcloud CLI.

Options:
  --name NAME            Server/key name (default: $NAME; env: SERVER_NAME).
  --type TYPE            Server type (default: $TYPE; env: SERVER_TYPE).
  --image IMAGE          OS image (default: $IMAGE).
  --location LOC         Location (default: $LOCATION).
  --repo OWNER/NAME      Repo to pull docker-compose.control.yml from (default: git origin).
  --branch BRANCH        Branch to pull from (default: current branch).
  --control-image REF    Control image (default: $CONTROL_IMAGE; env: CONTROL_IMAGE).
  --ui-image REF         UI image (default: $UI_IMAGE; env: UI_IMAGE).
  --key-path PATH        SSH key path (default: \$HOME/.ssh/roda_NAME).
  --dest DIR             Output dir for demo-info.env (default: $DEST).
  -h, --help             Show this help.
EOF
}

while [ $# -gt 0 ]; do
  case $1 in
    --name) NAME=$2; shift 2 ;;
    --type) TYPE=$2; shift 2 ;;
    --image) IMAGE=$2; shift 2 ;;
    --location) LOCATION=$2; shift 2 ;;
    --repo) REPO=$2; shift 2 ;;
    --branch) BRANCH=$2; shift 2 ;;
    --control-image) CONTROL_IMAGE=$2; shift 2 ;;
    --ui-image) UI_IMAGE=$2; shift 2 ;;
    --key-path) KEY_PATH=$2; shift 2 ;;
    --dest) DEST=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; die "unknown argument: $1" ;;
  esac
done

REPO=${REPO:-$(repo_slug "$DEPLOY_DIR")}; REPO=${REPO:-tislib/roda-ledger}
BRANCH=${BRANCH:-$(git_branch "$DEPLOY_DIR")}; BRANCH=${BRANCH:-master}
KEY_PATH=${KEY_PATH:-$HOME/.ssh/roda_$NAME}
require_env HCLOUD_TOKEN
require_cmd hcloud "install with scripts/util/hcloud-install.sh"

# Wait up to ~30s for the UI to answer on :8080.
smoke_check() {
  local i
  for i in $(seq 1 6); do
    if ssh_exec "$KEY_PATH" "$SERVER_IP" "curl -fsS http://localhost:8080 >/dev/null"; then
      log "UI is responding"
      return 0
    fi
    log "UI not ready, retrying... ($i/6)"
    sleep 5
  done
  log "::warning::UI did not respond on port 8080 — check 'docker compose logs' on the server"
}

SERVER_IP=$("$UTIL/provision-server.sh" \
  --name "$NAME" --type "$TYPE" --image "$IMAGE" --location "$LOCATION" \
  --key-path "$KEY_PATH" --replace)

log "Deploying demo stack to $SERVER_IP ..."
"$UTIL/run-remote.sh" --key-path "$KEY_PATH" --host "$SERVER_IP" \
  --script "$DEPLOY_DIR/remote/demo-stack.sh" \
  --env REPO="$REPO" --env BRANCH="$BRANCH" --env SERVER_IP="$SERVER_IP" \
  --env CONTROL_IMAGE="$CONTROL_IMAGE" --env UI_IMAGE="$UI_IMAGE"

smoke_check

mkdir -p "$DEST"
cat > "$DEST/demo-info.env" <<EOF
SERVER_NAME=$NAME
SERVER_TYPE=$TYPE
SERVER_IP=$SERVER_IP
CONTROL_IMAGE=$CONTROL_IMAGE
UI_IMAGE=$UI_IMAGE
EOF

log "Demo deployed: http://$SERVER_IP:8080  (gRPC $SERVER_IP:50051)"
log "The server is persistent. Destroy with: scripts/util/destroy-server.sh --name $NAME"
