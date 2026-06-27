#!/usr/bin/env bash
# Full load run: provision a Hetzner server, run the load probes, fetch load.md plus the
# raw captures, then destroy the server (even on failure). See deploy/README.md.
set -euo pipefail
DEPLOY_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
UTIL=$DEPLOY_DIR/../util
# shellcheck source=scripts/util/common.sh
source "$UTIL/common.sh"

NAME=${SERVER_NAME:-load-test-local}
TYPE=${SERVER_TYPE:-ccx33}
IMAGE=${SERVER_IMAGE:-ubuntu-24.04}
LOCATION=${SERVER_LOCATION:-fsn1}
ARGS=${ARGS:-}
REPO=${REPO:-}
BRANCH=${BRANCH:-}
KEY_PATH=
DEST=results
KEEP=0

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") [options]

Provision a Hetzner server, run the load probes, fetch load.md + raw output into
\$DEST, and destroy the server. Requires HCLOUD_TOKEN and the hcloud CLI.

Options:
  --name NAME        Server/key name (default: $NAME; env: SERVER_NAME).
  --type TYPE        Server type (default: $TYPE; env: SERVER_TYPE).
  --image IMAGE      OS image (default: $IMAGE).
  --location LOC     Location (default: $LOCATION).
  --args "..."       Extra args forwarded to the load binary (currently unused; env: ARGS).
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
    --args) ARGS=$2; shift 2 ;;
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
  --script "$DEPLOY_DIR/remote/load-run.sh" \
  --env REPO="$REPO" --env BRANCH="$BRANCH" --env ARGS="$ARGS" || run_status=$?

# The assembled report (drop-in for docs/load.md) plus every raw per-run capture.
"$UTIL/fetch-results.sh" --key-path "$KEY_PATH" --host "$SERVER_IP" --dest "$DEST" /root/load.md "/root/out-*.txt"
[ -f "$DEST/load.md" ] || echo "No load.md to retrieve" > "$DEST/load.md"

log "Results in $DEST/ (run status: $run_status)"
exit "$run_status"
