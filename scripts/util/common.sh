# shellcheck shell=bash
# Shared helpers for scripts/util and scripts/deploy. Source this; do not execute.

# SSH options for ephemeral demo/test servers (no host-key prompts, quiet).
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR)

log() { printf '%s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

# require_env VAR [hint] — fail unless VAR is set and non-empty.
require_env() {
  [ -n "${!1:-}" ] || die "environment variable $1 is required${2:+ ($2)}"
}

# require_cmd CMD [hint] — fail unless CMD is on PATH.
require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 not found on PATH${2:+ — $2}"
}

# ssh_exec KEY HOST CMD... — run CMD on the server with standard options.
ssh_exec() {
  local key=$1 host=$2
  shift 2
  ssh "${SSH_OPTS[@]}" -i "$key" "root@$host" "$@"
}

# repo_slug DIR — best-effort "owner/name" from DIR's GitHub origin (empty on failure).
repo_slug() {
  local url
  url=$(git -C "${1:-.}" remote get-url origin 2>/dev/null) || return 0
  url=${url%.git}
  printf '%s' "${url#*github.com[:/]}"
}

# git_branch DIR — current branch name (empty on failure).
git_branch() { git -C "${1:-.}" rev-parse --abbrev-ref HEAD 2>/dev/null || true; }
