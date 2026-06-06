#!/usr/bin/env bash
# Build a Ray wheel, bake it into a docker image, and push to ECR.
#
# Usage:
#   ./deploy.sh                # builds yuanzhuo_ray_test:latest
#   ./deploy.sh restore-batch  # builds restore-batch:latest, pushes restore-batch_<sha>_fix
#   ./deploy.sh restore-batch --clean   # also bazel clean --expunge first
#   ./deploy.sh --clean                 # name defaults to yuanzhuo_ray_test
#
# Args (order-independent):
#   --clean   wipe bazel cache before building
#   <name>    local docker image name AND ECR tag prefix; defaults to
#             yuanzhuo_ray_test for backward compat.
set -euo pipefail

# Parse args: --clean is a flag, anything else overrides the image name.
# Defaults:
#   local docker image -> yuanzhuo_ray_test:latest
#   ECR tag            -> yuanzhuo_fix              (rolling — overwrites each push)
# When a name is passed, both become <name>:latest and <name>_fix.
IMAGE_NAME="yuanzhuo_ray_test"
TAG_PREFIX="yuanzhuo"
CLEAN=0
for arg in "$@"; do
  case "$arg" in
    --clean) CLEAN=1 ;;
    -*)
      echo "Unknown flag: $arg" >&2
      exit 2
      ;;
    *)
      IMAGE_NAME="$arg"
      TAG_PREFIX="$arg"
      ;;
  esac
done

# fnm (npm) — only auto-loaded in interactive shells
export PATH="$HOME/.fnm:$PATH"
eval "$(fnm env)"

# Python venv — absolute path; ~/venv-ray-310/, NOT ~/ray/venv-ray-310/
# shellcheck disable=SC1090
source "$HOME/venv-ray-310/bin/activate"

# AWS creds + ECR login (reuse caller env; if expired, error out clearly)
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID must be set (export it before running)}"
aws ecr get-login-password --region us-west-2 | \
    docker login --username AWS --password-stdin \
    830883877497.dkr.ecr.us-west-2.amazonaws.com

# Sanity check the cwd is a Ray source tree. We rely on the caller
# (deploy_via_devbox.sh -> ssh "cd $REMOTE_PATH && bash ./deploy.sh")
# to chdir us into the right branch directory; the previous hard-coded
# `cd "$HOME/ray"` silently masked branch-specific REMOTE_PATH choices.
if [[ ! -f WORKSPACE && ! -f MODULE.bazel ]] || [[ ! -d python/ray ]]; then
  echo "ERROR: deploy.sh must be run from a Ray source tree (cwd: $PWD)" >&2
  echo "  Usually invoked via deploy_via_devbox.sh which cds into the" >&2
  echo "  branch-specific REMOTE_PATH on devbox." >&2
  exit 2
fi

# Optional: Clean build artifacts
if [[ "$CLEAN" == "1" ]]; then
    echo "=== bazel clean (expunge) ==="
    bazel clean --expunge
fi

# 0. Build dashboard frontend
# Skip if build output already exists (frontend rarely changes). Force with
# FORCE_NPM=1 when you do touch python/ray/dashboard/client/.
if [[ -f python/ray/dashboard/client/build/index.html && "${FORCE_NPM:-0}" != "1" ]]; then
    echo "=== npm build (skipped, build/index.html exists; set FORCE_NPM=1 to rebuild) ==="
else
    echo "=== npm build ==="
    (cd python/ray/dashboard/client && npm ci && npm run build)
    test -f python/ray/dashboard/client/build/index.html
fi

# 1. Build wheel from python/ (root pyproject.toml is ruff-only, not a build spec)
echo "=== pip wheel ==="
rm -f dist/ray-*.whl
mkdir -p dist
(cd python && python -m pip wheel -v -w ../dist . --no-deps)
NEW_WHL=$(ls -t dist/ray-*.whl | head -1)
echo "built: $NEW_WHL"
# Capture listing first; piping `unzip | grep -q` triggers SIGPIPE on unzip
# (grep exits early on match) which `set -o pipefail` then reports as failure.
wheel_listing=$(unzip -l "$NEW_WHL")
grep -q "dashboard/client/build/index.html" <<<"$wheel_listing" \
    || { echo "ERROR: wheel missing dashboard build/" >&2; exit 1; }

# 2. Build docker image
echo "=== docker build (image: ${IMAGE_NAME}:latest) ==="
rm -f "$HOME/workspace-testing/"*.whl
cp "$NEW_WHL" "$HOME/workspace-testing/"
docker build -t "${IMAGE_NAME}:latest" "$HOME/workspace-testing"

# 3. Push
echo "=== docker push ==="
ECR_REPO=830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/ray
TAG="${TAG_PREFIX}_fix"
docker tag "${IMAGE_NAME}:latest" "${ECR_REPO}:${TAG}"
docker push "${ECR_REPO}:${TAG}"

echo "DONE: ${ECR_REPO}:${TAG}"
