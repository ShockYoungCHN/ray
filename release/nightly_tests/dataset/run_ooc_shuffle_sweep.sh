#!/usr/bin/env bash
#
# Reproduction driver for the out-of-core hash-shuffle spill study.
#
# Runs benchmark_ooc_shuffle.py across a sweep of data sizes at a fixed
# partition count, so spill behaviour can be compared from the in-core
# regime into heavy spill. The /tmp throwaway drivers used during the
# original investigation are folded into this single documented script.
#
# Per run it writes (under $OUTDIR):
#   - result_<sz>gb.json   : timing + spill metrics summary
#   - <experiment_ts>/      : per-node raw spill logs (*_spill_log.txt)
#   - sweep.log             : combined stdout (incl. ds.stats when profiling on)
# Raw logs/results live under benchmark_logs/, which .rayignore keeps out of
# ray.init uploads (and they are intentionally NOT git-committed).
#
# Usage:
#   ./run_ooc_shuffle_sweep.sh                       # default sweep, 500 partitions
#   SIZES="128 256 384 512" PARTITIONS=500 ./run_ooc_shuffle_sweep.sh
#   PROFILE=1 ./run_ooc_shuffle_sweep.sh             # also print per-stage ds.stats()
#   TIMELINE=1 SIZES="512 64" ./run_ooc_shuffle_sweep.sh   # dump ray.timeline() per run
#
# Analyze afterwards:
#   python analyze_spill_events.py <OUTDIR>/<experiment_ts> --gap-split 60
#   python analyze_task_store_phase.py <OUTDIR>/timeline_<sz>.json --since <BENCHMARK_START_TS>
set -euo pipefail

SIZES="${SIZES:-128 256 384 512}"
PARTITIONS="${PARTITIONS:-500}"
OUTDIR="${OUTDIR:-/home/ray/default/benchmark_logs/sweep_${PARTITIONS}p}"
PROFILE="${PROFILE:-0}"
TIMELINE="${TIMELINE:-0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$OUTDIR"

if [[ "$PROFILE" == "1" ]]; then
  export RAY_DATA_SHUFFLE_PROFILE=1
fi

echo "Sweep: sizes=[$SIZES] partitions=$PARTITIONS profile=$PROFILE timeline=$TIMELINE -> $OUTDIR"

for sz in $SIZES; do
  echo "==================== RUN ${sz}GB / ${PARTITIONS}p $(date -u +%H:%M:%S) ===================="
  extra=()
  if [[ "$TIMELINE" == "1" ]]; then
    extra+=(--timeline-out "${OUTDIR}/timeline_${sz}.json")
  fi
  python "${SCRIPT_DIR}/benchmark_ooc_shuffle.py" \
    --data-size-gb "${sz}" \
    --num-partitions "${PARTITIONS}" \
    --output "${OUTDIR}/result_${sz}gb.json" \
    "${extra[@]}"
  echo "==================== DONE ${sz}GB rc=$? $(date -u +%H:%M:%S) ===================="
done

echo "ALL_RUNS_COMPLETE"
