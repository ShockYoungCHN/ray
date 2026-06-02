#!/usr/bin/env bash
# SSH to devbox and run its checked-in deploy.sh (wheel → docker → ECR push).
#
# Forwards the three AWS env vars deploy.sh needs. They must already be
# exported in YOUR local shell before running this. If any is missing, we
# fail fast rather than letting deploy.sh error halfway through the build.
#
# Assumes you've already run ./sync_to_devbox.sh so the devbox tree is fresh.
#
# Usage:
#   export AWS_ACCESS_KEY_ID=...
#   export AWS_SECRET_ACCESS_KEY=...
#   export AWS_SESSION_TOKEN=...
#   ./deploy_via_devbox.sh

set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-devbox}"
REMOTE_PATH="${REMOTE_PATH:-/home/ubuntu/ray}"

: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID must be exported locally}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY must be exported locally}"
: "${AWS_SESSION_TOKEN:?AWS_SESSION_TOKEN must be exported locally}"

echo "== Syncing local code to $REMOTE_HOST"
./sync_to_devbox.sh

echo "== Running deploy.sh on $REMOTE_HOST:$REMOTE_PATH"

# Pass creds inline (ssh doesn't forward env vars by default, and we don't
# want to depend on the devbox sshd's AcceptEnv config). -t allocates a tty
# so docker login output streams back live.
ssh -t "$REMOTE_HOST" "
  export AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID'
  export AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY'
  export AWS_SESSION_TOKEN='$AWS_SESSION_TOKEN'
  cd '$REMOTE_PATH' && bash ./deploy.sh ${1:-}
"
