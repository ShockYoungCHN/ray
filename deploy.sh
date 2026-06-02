#!/usr/bin/env bash
set -euo pipefail

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

cd "$HOME/ray"

# Optional: Clean build artifacts
if [[ "${1:-}" == "--clean" ]]; then
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
echo "=== docker build ==="
rm -f "$HOME/workspace-testing/"*.whl
cp "$NEW_WHL" "$HOME/workspace-testing/"
docker build -t yuanzhuo_ray_test:latest "$HOME/workspace-testing"

# 3. Push
echo "=== docker push ==="
ECR_REPO=830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/ray
TAG="yuanzhuo_$(git rev-parse --short HEAD)_fix"
docker tag yuanzhuo_ray_test:latest "${ECR_REPO}:${TAG}"
docker push "${ECR_REPO}:${TAG}"

echo "DONE: ${ECR_REPO}:${TAG}"
