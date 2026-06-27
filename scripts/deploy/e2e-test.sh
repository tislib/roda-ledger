#!/usr/bin/env bash
# Full e2e run: provision a Hetzner server, build & run the e2e scenario suite, fetch
# the output, then destroy the server (even on failure). See deploy/README.md.
set -euo pipefail
DEPLOY_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
UTIL=$DEPLOY_DIR/../util
# shellcheck source=scripts/util/common.sh
source "$UTIL/common.sh"

NAME=${SERVER_NAME:-e2e-test-local}
TYPE=${SERVER_TYPE:-ccx23}
IMAGE=${SERVER_IMAGE:-ubuntu-24.04}
LOCATION=${SERVER_LOCATION:-fsn1}
NODES=${NODES:-3}
REPO=${REPO:-}
BRANCH=${BRANCH:-}
KEY_PATH=
DEST=results
KEEP=0

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") [options]

Provision a Hetzner server, run the e2e scenario suite, fetch results into \$DEST,
and destroy the server. Requires HCLOUD_TOKEN and the hcloud CLI.

Options:
  --name NAME        Server/key name (default: $NAME; env: SERVER_NAME).
  --type TYPE        Server type (default: $TYPE; env: SERVER_TYPE).
  --image IMAGE      OS image (default: $IMAGE).
  --location LOC     Location (default: $LOCATION).
  --nodes N          Nodes per scenario cluster (default: $NODES; env: NODES).
  --repo OWNER/NAME  Repo to clone on the server (default: git origin).
  --branch BRANCH    Branch to clone (default: current branch).
  --key-path PATH    SSH key path (default: \$HOME/.ssh/roda_NAME).
  --dest DIR         Local results dir (default: $DEST).
  --keep             Do not destroy the server on exit (debugging).
  -h, --help         Show this help.
EOF
}

while [ $# -gt 0 ]; do
  case $1 in
    --name) NAME=$2; shift 2 ;;
    --type) TYPE=$2; shift 2 ;;
    --image) IMAGE=$2; shift 2 ;;
    --location) LOCATION=$2; shift 2 ;;
    --nodes) NODES=$2; shift 2 ;;
    --repo) REPO=$2; shift 2 ;;
    --branch) BRANCH=$2; shift 2 ;;
    --key-path) KEY_PATH=$2; shift 2 ;;
    --dest) DEST=$2; shift 2 ;;
    --keep) KEEP=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage; die "unknown argument: $1" ;;
  esac
done

REPO=${REPO:-$(repo_slug "$DEPLOY_DIR")}; REPO=${REPO:-tislib/roda-ledger}
BRANCH=${BRANCH:-$(git_branch "$DEPLOY_DIR")}; BRANCH=${BRANCH:-master}
KEY_PATH=${KEY_PATH:-$HOME/.ssh/roda_$NAME}
require_env HCLOUD_TOKEN
require_cmd hcloud "install with scripts/util/hcloud-install.sh"

# shellcheck disable=SC2329  # invoked indirectly via trap
cleanup() { if [ "$KEEP" != 1 ]; then "$UTIL/destroy-server.sh" --name "$NAME" || true; fi; }
trap cleanup EXIT INT TERM

SERVER_IP=$("$UTIL/provision-server.sh" \
  --name "$NAME" --type "$TYPE" --image "$IMAGE" --location "$LOCATION" --key-path "$KEY_PATH")

run_status=0
"$UTIL/run-remote.sh" --key-path "$KEY_PATH" --host "$SERVER_IP" \
  --script "$DEPLOY_DIR/remote/e2e-run.sh" \
  --env REPO="$REPO" --env BRANCH="$BRANCH" --env NODES="$NODES" || run_status=$?

"$UTIL/fetch-results.sh" --key-path "$KEY_PATH" --host "$SERVER_IP" --dest "$DEST" /root/e2e-test-output.txt
[ -f "$DEST/e2e-test-output.txt" ] || echo "No output file to retrieve" > "$DEST/e2e-test-output.txt"

log "Results in $DEST/ (run status: $run_status)"
exit "$run_status"
