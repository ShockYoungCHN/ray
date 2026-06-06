#!/usr/bin/env bash
# SSH to devbox and run its checked-in deploy.sh (wheel → docker → ECR push).
#
# Forwards the three AWS env vars deploy.sh needs. They must already be
# exported in YOUR local shell before running this. If any is missing, we
# fail fast rather than letting deploy.sh error halfway through the build.
#
# All trailing args are forwarded verbatim to remote deploy.sh, so you can
# pass an image name and/or --clean:
#   ./deploy_via_devbox.sh                       # default name (yuanzhuo_ray_test)
#   ./deploy_via_devbox.sh restore-batch         # build/push restore-batch:latest
#   ./deploy_via_devbox.sh restore-batch --clean # also bazel clean --expunge
#
# Assumes you've already run ./sync_to_devbox.sh — but we invoke it again
# below to guarantee the latest deploy.sh / source tree lands on devbox.
#
# Required env (exported locally before running):
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN

set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-devbox}"
LOCAL_PATH="$(cd "$(dirname "$0")" && pwd)"

# Resolve REMOTE_PATH from current git branch — same logic as
# sync_to_devbox.sh — so the wheel build on devbox happens in the
# branch's own directory, not the legacy /home/ubuntu/ray. Export it
# so the child sync_to_devbox.sh below inherits the same path instead
# of doing its own (independent, hopefully matching) derivation.
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
    sanitized="$(printf '%s' "$branch" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9.' '-')"
    sanitized="$(printf '%s' "$sanitized" | sed -e 's/--*/-/g' -e 's/^-//' -e 's/-$//')"
    REMOTE_PATH="/home/ubuntu/ray-${sanitized}"
  fi
fi
export REMOTE_PATH

: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID must be exported locally}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY must be exported locally}"
: "${AWS_SESSION_TOKEN:?AWS_SESSION_TOKEN must be exported locally}"

echo "== Syncing local code to $REMOTE_HOST"
./sync_to_devbox.sh

# Quote each arg individually so names with spaces / shell-special chars
# survive the ssh round trip intact.
REMOTE_ARGS=""
for a in "$@"; do
  REMOTE_ARGS+=" $(printf '%q' "$a")"
done

echo "== Running deploy.sh${REMOTE_ARGS:+ ${REMOTE_ARGS# }} on $REMOTE_HOST:$REMOTE_PATH"

# Pass creds inline (ssh doesn't forward env vars by default, and we don't
# want to depend on the devbox sshd's AcceptEnv config). -t allocates a tty
# so docker login output streams back live.
ssh -t "$REMOTE_HOST" "
  export AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID'
  export AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY'
  export AWS_SESSION_TOKEN='$AWS_SESSION_TOKEN'
  cd '$REMOTE_PATH' && bash ./deploy.sh ${REMOTE_ARGS}
"
