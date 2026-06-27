#!/usr/bin/env bash
# Copy result file(s)/glob(s) from a remote server into a local directory.
# Missing paths warn (and are skipped) rather than failing the run.
set -euo pipefail
# shellcheck source=scripts/util/common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

KEY_PATH=
HOST=${SERVER_IP:-}
DEST=results

usage() {
  cat >&2 <<EOF
Usage: $(basename "$0") --key-path PATH --host IP [--dest DIR] REMOTE [REMOTE ...]

scp each REMOTE path/glob from root@IP into DIR (created if needed).

Options:
  --key-path PATH   SSH private-key path.
  --host IP         Server IP/host (env: SERVER_IP).
  --dest DIR        Local destination directory (default: $DEST).
  -h, --help        Show this help.
EOF
}

REMOTES=()
while [ $# -gt 0 ]; do
  case $1 in
    --key-path) KEY_PATH=$2; shift 2 ;;
    --host) HOST=$2; shift 2 ;;
    --dest) DEST=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    --) shift; while [ $# -gt 0 ]; do REMOTES+=("$1"); shift; done ;;
    *) REMOTES+=("$1"); shift ;;
  esac
done

[ -n "$KEY_PATH" ] || { usage; die "--key-path is required"; }
[ -n "$HOST" ] || { usage; die "--host is required"; }
[ "${#REMOTES[@]}" -gt 0 ] || { usage; die "at least one REMOTE path is required"; }

mkdir -p "$DEST"
for remote in "${REMOTES[@]}"; do
  if scp "${SSH_OPTS[@]}" -i "$KEY_PATH" "root@$HOST:$remote" "$DEST/" 2>/dev/null; then
    log "Retrieved $remote -> $DEST/"
  else
    log "warn: could not retrieve $remote"
  fi
done
