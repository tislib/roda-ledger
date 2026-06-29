# shellcheck shell=bash
# Shared helpers for scripts/dev. Source this; do not execute.
# The remote dev box is configured by env only — never hardcode a host here:
#   RODA_DEV_HOST   user@host or an ~/.ssh/config alias of your build box (required)
#   RODA_DEV_PATH   repo path on the box (default: /root/roda-ledger)

log() { printf '%s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

DEV_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$DEV_DIR/../.." && pwd)

# Optional private config (gitignored): put RODA_DEV_HOST / RODA_DEV_PATH in scripts/dev/.env.
if [ -f "$DEV_DIR/.env" ]; then
  # shellcheck source=/dev/null
  . "$DEV_DIR/.env"
fi
RODA_DEV_PATH=${RODA_DEV_PATH:-/root/roda-ledger}

require_dev_host() {
  [ -n "${RODA_DEV_HOST:-}" ] || die "set RODA_DEV_HOST (e.g. root@my-box, or an ~/.ssh/config alias) — or put it in scripts/dev/.env; see scripts/dev/README.md"
}

# dev_sync — mirror the working tree to the dev box (build/scratch excluded).
dev_sync() {
  require_dev_host
  log "Syncing $REPO_ROOT/ -> $RODA_DEV_HOST:$RODA_DEV_PATH/ ..."
  rsync -a --delete \
    --exclude target --exclude "temp_*" --exclude node_modules \
    --exclude .claude --exclude data --exclude .git \
    "$REPO_ROOT/" "root@$RODA_DEV_HOST:$RODA_DEV_PATH/"
}

# dev_exec CMD... — run CMD on the dev box, in the repo, with cargo on PATH.
dev_exec() {
  require_dev_host
  # $RODA_DEV_PATH and the command expand client-side (intended); \$HOME expands remotely.
  # shellcheck disable=SC2029
  ssh "$RODA_DEV_HOST" "cd '$RODA_DEV_PATH'; . \"\$HOME/.cargo/env\" 2>/dev/null || true; $*"
}

# dev_run CMD... — sync, then run CMD on the dev box.
dev_run() {
  dev_sync
  log "Running on $RODA_DEV_HOST: $*"
  dev_exec "$@"
}
