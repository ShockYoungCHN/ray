#!/usr/bin/env bash
# One-shot: rsync local tree to devbox:/home/ubuntu/ray, overwriting remote.
# Excludes VCS metadata, bazel symlinks, Python caches, IDE state, and large
# build artifacts so each push stays under a few hundred MB.
#
# Usage:
#   ./sync_to_devbox.sh                 # sync only
#   ./sync_to_devbox.sh --rebuild       # sync + rebuild raylet on devbox
#   ./sync_to_devbox.sh --rebuild-wheel # sync + rebuild python wheel on devbox

set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-devbox}"
REMOTE_PATH="${REMOTE_PATH:-/home/ubuntu/ray}"
LOCAL_PATH="$(cd "$(dirname "$0")" && pwd)"

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
