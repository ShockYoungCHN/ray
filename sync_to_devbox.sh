#!/usr/bin/env bash
# One-shot: rsync local tree to devbox, overwriting remote.
# Excludes VCS metadata, bazel symlinks, Python caches, IDE state, and large
# build artifacts so each push stays under a few hundred MB.
#
# REMOTE_PATH selection (first match wins):
#   1. $REMOTE_PATH env var if set (explicit override)
#   2. /home/ubuntu/ray   when current branch is data/base-shuffle-operator
#                         (the legacy primary path — keeps existing flows working)
#   3. /home/ubuntu/ray-<sanitized-branch>   otherwise
#                         (each worktree / branch gets its own isolated tree
#                          on devbox, so deploys from different branches don't
#                          clobber each other)
#
# Branch sanitization: lowercase + replace any non-[a-z0-9.] with '-'.
# Examples:
#   data/base-shuffle-operator           -> /home/ubuntu/ray             (legacy)
#   core/restore-objects-bulk-api        -> /home/ubuntu/ray-core-restore-objects-bulk-api
#   trace/youcheng-base-shuffle          -> /home/ubuntu/ray-trace-youcheng-base-shuffle
#
# Usage:
#   ./sync_to_devbox.sh                 # sync only
#   ./sync_to_devbox.sh --rebuild       # sync + rebuild raylet on devbox
#   ./sync_to_devbox.sh --rebuild-wheel # sync + rebuild python wheel on devbox
#
# Override path explicitly:
#   REMOTE_PATH=/home/ubuntu/ray-experiment ./sync_to_devbox.sh

set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-devbox}"
LOCAL_PATH="$(cd "$(dirname "$0")" && pwd)"

# Resolve REMOTE_PATH. Env var wins; otherwise derive from current git branch
# so each worktree / branch gets an isolated devbox directory.
if [[ -z "${REMOTE_PATH:-}" ]]; then
  branch="$(git -C "$LOCAL_PATH" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "")"
  if [[ -z "$branch" || "$branch" == "HEAD" ]]; then
    echo "ERROR: could not resolve git branch in $LOCAL_PATH" >&2
    echo "  (detached HEAD?) — set REMOTE_PATH explicitly to proceed" >&2
    exit 2
  fi
  if [[ "$branch" == "data/base-shuffle-operator" ]]; then
    REMOTE_PATH="/home/ubuntu/ray"
  else
    # Sanitize: lowercase, replace any non [a-z0-9.] with '-'.
    sanitized="$(printf '%s' "$branch" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9.' '-')"
    # Collapse repeated dashes + strip leading/trailing dashes.
    sanitized="$(printf '%s' "$sanitized" | sed -e 's/--*/-/g' -e 's/^-//' -e 's/-$//')"
    REMOTE_PATH="/home/ubuntu/ray-${sanitized}"
  fi
fi
echo "== Branch: $(git -C "$LOCAL_PATH" rev-parse --abbrev-ref HEAD)  →  $REMOTE_HOST:$REMOTE_PATH"

ACTION="${1:-sync}"

echo "== Syncing $LOCAL_PATH -> $REMOTE_HOST:$REMOTE_PATH"
rsync -avz --delete \
  --exclude='.git/' \
  --exclude='.claude/worktrees/' \
  --exclude='.claude/plans/' \
  --exclude='bazel-bin' \
  --exclude='bazel-out' \
  --exclude='bazel-ray*' \
  --exclude='bazel-testlogs' \
  --exclude='.bazel_cache/' \
  --exclude='build/' \
  --exclude='dist/' \
  --exclude='*.egg-info/' \
  --exclude='__pycache__/' \
  --exclude='*.pyc' \
  --exclude='.pytest_cache/' \
  --exclude='.mypy_cache/' \
  --exclude='.ruff_cache/' \
  --exclude='.idea/' \
  --exclude='.vscode/' \
  --exclude='*.jsonl' \
  --exclude='node_modules/' \
  --exclude='python/ray/dashboard/client/build/' \
  --exclude='python/ray/dashboard/client/node_modules/' \
  --exclude='python/ray/core/' \
  --exclude='python/ray/_raylet*.so' \
  --exclude='python/ray/thirdparty_files/' \
  --exclude='python/ray/_private/runtime_env/agent/thirdparty_files/' \
  "$LOCAL_PATH/" "$REMOTE_HOST:$REMOTE_PATH/"

echo "== Sync done."

case "$ACTION" in
  --rebuild)
    echo "== Rebuilding raylet on $REMOTE_HOST"
    ssh "$REMOTE_HOST" bash -lc "'
      source ~/venv-ray-310/bin/activate &&
      cd $REMOTE_PATH &&
      bazel build //src/ray/raylet:raylet 2>&1 | tail -20
    '"
    ;;
  --rebuild-wheel)
    echo "== Rebuilding wheel on $REMOTE_HOST"
    ssh "$REMOTE_HOST" bash -lc "'
      source ~/venv-ray-310/bin/activate &&
      cd $REMOTE_PATH/python &&
      pip wheel -v -w dist . --no-deps 2>&1 | tail -30
    '"
    ;;
  sync)
    ;;
  *)
    echo "Unknown action: $ACTION" >&2
    exit 1
    ;;
esac
