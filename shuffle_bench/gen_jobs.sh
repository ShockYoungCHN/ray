#!/usr/bin/env bash
# Generate Anyscale job yamls for the shuffle sweep (file vs in-memory hash shuffle).
#
# Usage:  bash gen_jobs.sh [ARM]        ARM = disk | v2 | both   (default: both)
#   disk = external file-transport shuffle    (use_external_hash_shuffle=True)
#   v2   = in-memory / object-store shuffle    (use_external_hash_shuffle=False)
#   both = run v2 then disk in one job (emits a SPEEDUP line)
#
# Generated job_<arm>_<size>.yaml files are artifacts (git-ignored) -- regenerate,
# don't commit. Edit the config block below to change the sweep.
set -euo pipefail
cd "$(dirname "$0")"

ARM="${1:-both}"
case "$ARM" in disk|v2|both) ;; *) echo "ARM must be disk|v2|both" >&2; exit 2 ;; esac

IMAGE="830883877497.dkr.ecr.us-west-2.amazonaws.com/anyscale/ray:yuanzhuo_v3clean_fix"
COMPUTE="yy-shuffle-32"            # 32 x m5.2xlarge, gp3
TARGET_CPU=256
PARTITION_SIZE_GB=1
declare -A SIZES=( [128gb]=128 [256gb]=256 [512gb]=512 [1tb]=1024 [2tb]=2048 [4tb]=4096 [8tb]=8192 )
ORDER="8tb 4tb 2tb 1tb 512gb 256gb 128gb"

for sz in $ORDER; do
  gb=${SIZES[$sz]}
  parts=$(( gb / PARTITION_SIZE_GB ))
  out="job_${ARM}_${sz}.yaml"
  cat > "$out" <<EOF
name: yy-${ARM}-${sz}
image_uri: ${IMAGE}
compute_config: ${COMPUTE}
working_dir: .
excludes: ["__pycache__", "*.log"]
max_retries: 0
env_vars:
  RAY_DEDUP_LOGS: "0"
  RAY_object_manager_rpc_threads_num: "8"
  # REQUIRED: routes keyed HASH_SHUFFLE repartition through the v2 planner,
  # the only one with the external(disk) branch. Without it the disk arm
  # silently runs the in-memory path.
  RAY_DATA_USE_HASH_SHUFFLE_V2: "1"
entrypoint: |
  set -x
  echo "===== ${ARM} | ${sz} (${gb}GB) / ${PARTITION_SIZE_GB}GB-per-partition = ${parts} parts ====="
  python bench_shuffle.py --data-size-gb ${gb} --gb-per-partition ${PARTITION_SIZE_GB} --shuffle ${ARM} --target-cpu ${TARGET_CPU} --result-json /tmp/res_${ARM}_${sz}.json 2>&1 | tee /tmp/run_${ARM}_${sz}.log
  grep -E "RESULT|SPEEDUP" /tmp/run_${ARM}_${sz}.log || true
EOF
  echo "wrote $out  (${gb}GB -> ${parts} parts, arm=${ARM})"
done
echo "done: $(echo $ORDER | wc -w) job yamls for arm=${ARM}."
