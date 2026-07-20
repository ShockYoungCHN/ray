#!/usr/bin/env bash
# Spin up a 2-node Ray cluster in Docker on devbox, run the node-death
# probe on the head, kill the worker container mid-test, observe.
#
# Runs on the devbox host. Requires `docker` and an image with Ray installed
# (defaults to yuanzhuo_ray_test:latest — the one deploy.sh builds).
#
# Usage:
#   ./run_node_death_test.sh
#   IMAGE=my/ray:tag ./run_node_death_test.sh
set -euo pipefail

IMAGE="${IMAGE:-yuanzhuo_ray_test:latest}"
NET="ray-node-death-net"
HEAD="rnd-head"
WORKER="rnd-worker"
TEST_SCRIPT="$(cd "$(dirname "$0")" && pwd)/test_node_death.py"

cleanup() {
    docker rm -f "$HEAD" "$WORKER" 2>/dev/null || true
    docker network rm "$NET" 2>/dev/null || true
}
trap cleanup EXIT

cleanup   # in case a prior run left containers around

echo ">>> creating docker network"
docker network create "$NET" >/dev/null

echo ">>> starting head node"
docker run -d --name "$HEAD" --hostname "$HEAD" --network "$NET" \
    "$IMAGE" \
    bash -c 'ray start --head --port=6379 --num-cpus=2 --dashboard-host=0.0.0.0 --block'

sleep 5

echo ">>> starting worker node"
docker run -d --name "$WORKER" --hostname "$WORKER" --network "$NET" \
    "$IMAGE" \
    bash -c "ray start --address=${HEAD}:6379 --num-cpus=2 --block"

echo ">>> waiting 15s for cluster to form"
sleep 15

echo ">>> ray status:"
docker exec "$HEAD" ray status || true

echo ">>> copying test script into head"
docker cp "$TEST_SCRIPT" "$HEAD:/tmp/test_node_death.py"

echo ">>> starting test on head (background)"
docker exec -d "$HEAD" bash -c 'python /tmp/test_node_death.py > /tmp/test_output.log 2>&1'

echo ">>> waiting 15s for actor creation + baseline probes"
sleep 15

echo ">>> killing worker container"
docker kill "$WORKER"
echo "    worker killed at $(date -u +%H:%M:%S)"

echo ">>> tailing head test log until it finishes (up to 12 min)"
# The test loops 60 iterations at 10s each = 600s + startup. Give some slack.
timeout 720 docker exec "$HEAD" bash -c 'tail -f /tmp/test_output.log & pid=$!; while kill -0 $(pgrep -f test_node_death.py) 2>/dev/null; do sleep 2; done; kill $pid 2>/dev/null' || true

echo ""
echo ">>> final test log:"
docker exec "$HEAD" cat /tmp/test_output.log
