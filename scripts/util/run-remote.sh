#!/usr/bin/env bash
# Run a local script ON a remote server over SSH (`ssh ... bash -s < SCRIPT`),
# forwarding chosen environment variables. Replaces inline heredocs in workflows.
set -euo pipefail
# shellcheck source=scripts/util/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

KEY_PATH=
HOST=${SERVER_IP:-}
SCRIPT=
ENVS=()

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") --key-path PATH --host IP --script FILE [--env K=V ...]

Stream FILE to root@IP and execute it with bash, exporting each --env K=V remotely.

Options:
  --key-path PATH   SSH private-key path.
  --host IP         Server IP/host (env: SERVER_IP).
  --script FILE     Local script to run on the server (fed on stdin).
  --env K=V         Environment variable to set remotely (repeatable).
  -h, --help        Show this help.
EOF
}

while [ $# -gt 0 ]; do
  case $1 in
    --key-path) KEY_PATH=$2; shift 2 ;;
    --host) HOST=$2; shift 2 ;;
    --script) SCRIPT=$2; shift 2 ;;
    --env) ENVS+=("$2"); shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; die "unknown argument: $1" ;;
  esac
done

[ -n "$KEY_PATH" ] || { usage; die "--key-path is required"; }
[ -n "$HOST" ] || { usage; die "--host is required"; }
[ -n "$SCRIPT" ] || { usage; die "--script is required"; }
[ -f "$SCRIPT" ] || die "script not found: $SCRIPT"

# Build a "K=v K=v bash -s" prefix; printf %q keeps values shell-safe remotely.
prefix=""
if [ "${#ENVS[@]}" -gt 0 ]; then
  for kv in "${ENVS[@]}"; do
    prefix+="${kv%%=*}=$(printf '%q' "${kv#*=}") "
  done
fi

ssh "${SSH_OPTS[@]}" -i "$KEY_PATH" "root@$HOST" "${prefix}bash -s" < "$SCRIPT"
