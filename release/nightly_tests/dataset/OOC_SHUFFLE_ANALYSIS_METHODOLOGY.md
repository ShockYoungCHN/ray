# OOC Shuffle Spill Analysis — Methodology & Number Provenance

Records *what was measured*, *which system each number comes from*, and *how
to reproduce it*. Companion to the scripts in this directory. Raw data dumps
live under `/home/ray/default/benchmark_logs/` (git-ignored from ray uploads
via `.rayignore`, NOT committed).

## Timing sources — which layer each number comes from

Numbers came from **4 different systems at 3 layers**, measuring different
scopes. They are not interchangeable; combining them is what caught the
map-store blind spot.

| number | source | layer | scope |
|---|---|---|---|
| `elapsed_s`, GB/s | driver `time.perf_counter()` (`benchmark_ooc_shuffle.py`) | wall-clock | whole read→shuffle→write |
| per-stage wall ("blocks produced in Xs"), "Remote wall/cpu time", shuffle sub-timers (`map.ipc_encode_s`, `reduce.ray_get_s`, `map.partition_fn_s`, …) | `ds.stats()` → `BlockExecStats` + `_StageTimings` (`data/_internal/stats.py:1539,1550`) | **Ray Data** | "Remote wall time" = UDF body only (`task:execute`); sub-timers = custom instrumentation inside shuffle tasks |
| per-task total durations (the ×1.1 / ×3.3 table) | `ray.util.state.list_tasks` `end_time_ms − start_time_ms` | **Ray Core** (GCS task state) | whole task (deserialize + execute + store_outputs) |
| phase breakdown (`task:execute` / `store_outputs` / `deserialize_arguments`) — create-stall + read decomposition | `ray.timeline()` profile events (`_raylet.pyx:1836,1985`) | **Ray Core** (profiling) | per-phase per-task |
| spilled GB, throughput, EvictionOnCreate/ThresholdMonitor, size dist, burstiness | `raylet_spill_events.out` (C++ `LocalObjectManager`) + Prometheus | **raylet / Core** | spill events |
| per-op concurrency / effective parallelism + cluster CPU saturation | `ray.timeline` execute spans, sweep-line (`analyze_scheduling.py`) | **Ray Core** (profiling) | how many tasks of an op run simultaneously vs total CPUs |
| op output-queue (backpressure) + running tasks + CPU over time | streaming-executor progress log (`sweep.log`) | **Ray Data** (executor) | is an op throttled by a full output queue / starved of CPU |

Key consequence: `ds.stats()` "Remote wall time" wraps only `task:execute`, so
it is **blind to the `task:store_outputs` epilogue** (where spill-create
would land). `list_tasks` end−start covers the whole task; `ray.timeline`
isolates each phase. The three cross-check each other.

## Clock-skew validity

Every reported figure is a **duration** (a single-worker start→end delta), and
the timeline→task attribution join is **within one worker_id (one node, one
clock)**. Cross-node NTP skew only corrupts comparing absolute timestamps
*across* nodes, which we never do. The only absolute-ts use is the
`--since/--until` run window, which has minutes of margin vs typical <100 ms
skew (and skew there would only change which events are counted, not any
duration). So "read store time flat / read execute ×3.9" is skew-immune.

## Reproduce each analysis

```bash
# 1. The sweep itself (per-stage ds.stats + spill metrics)
PROFILE=1 SIZES="64 128 256 384 512" PARTITIONS=500 ./run_ooc_shuffle_sweep.sh
#   -> benchmark_logs/sweep_500p/{result_*gb.json, sweep.log}
#   per-stage + shuffle sub-timer table parsed into summary_table.{md,csv}

# 2. Spill object size distribution + burstiness + plots  (raylet spill log)
python analyze_spill_events.py benchmark_logs/<experiment_ts>/ --gap-split 60
#   -> spill_{size_distribution,timeline,interarrival}.png + stdout report

# 3. Timeline-instrumented runs (for phase analysis)
TIMELINE=1 SIZES="512 64" PARTITIONS=500 ./run_ooc_shuffle_sweep.sh
#   -> benchmark_logs/timeline/timeline_{512,64}.json  (+ BENCHMARK_START_TS in log)

# 4a. Snapshot task-state records (worker_id + windows) BEFORE GCS evicts them
#     (inline; see task_records.json — list_tasks per name, dumped to json)

# 4b. Per-task-type PHASE breakdown = create-stall + read decomposition
python analyze_task_phases.py benchmark_logs/timeline/timeline_512.json \
    --tasks benchmark_logs/timeline/task_records.json \
    --since <BENCHMARK_START_TS_512> --until <+elapsed> --label 512_spill \
    --out-prefix benchmark_logs/timeline/tl512
#   -> tl512_phase_breakdown.{csv,md}   (n/sum/mean/p50/p99/max per task×phase)

# 5. Scheduling: per-op concurrency + cluster saturation, and read backpressure
python analyze_scheduling.py --timeline benchmark_logs/timeline/timeline_512.json \
    --tasks benchmark_logs/timeline/task_records.json \
    --since <BENCHMARK_START_TS_512> --until <+elapsed> --total-cpus 256 \
    --label 512_spill --out-prefix benchmark_logs/timeline/sched512
python analyze_scheduling.py --progress-log benchmark_logs/sweep_500p/sweep.log \
    --run-size 512 --op ReadFilesParquetV2 --out-prefix benchmark_logs/timeline/sched512
#   -> sched512_concurrency.{csv,md} (eff parallelism + cluster max/p90 conc)
#   -> sched512_backpressure.{csv,md} (read output-queue + CPU over progress ticks)
```

## Headline numbers (this run, 500 partitions)

- spill is NOT end-to-end bottleneck through 512 GB (GB/s holds ~2.1 to 295 GB spill).
- reduce restore is the dominant spill cost: `_shuffle_reduce_task` execute
  1164 ms → 8272 ms (64→512 GB).
- map produce-side create-stall is small: `store_outputs` 319 → 355 ms/task
  (+35 ms; tail max 387 → 998 ms). Map total ×1.1.
- read is NOT plasma/spill-affected: `store_outputs` flat 0.08 ms; its growth
  is entirely in `execute` (4882 → 19078 ms = S3 fetch + parquet decode, and
  the two runs read different datasets sf100 vs sf1000 — a dirty A/B for read).
- read is NOT scheduling-blocked: cluster never CPU-saturated (512 GB execute
  conc max 195/256, 64 GB max 203/256); read runs the WIDEST of all ops
  (eff_parallel 134 at 512) and its output queue DRAINS (839→0 blocks, not
  backpressured); read also finishes early (~half the run), so it is not on the
  critical path. The persistent idle CPU (cluster tops ~193-195/256) is a
  packing inefficiency (read tasks are IO-bound on S3 yet reserve 1 CPU each),
  not a block — and widening read wouldn't help end-to-end since reduce+write
  dominate the back half. Net: the only large spill cost is reduce restore.

## File index (under benchmark_logs/)

- `sweep_500p/summary_table.{md,csv}` — per-stage wall + shuffle sub-timers, all sizes (Ray Data `ds.stats`)
- `sweep_500p/{sweep.log, run_64gb.log, result_*gb.json}` — raw sweep output + spill metrics
- `20260604_0948*/` — per-node raw `*_spill_log.txt` (256/384/512 GB); `094821/plots*/` spill PNGs
- `timeline/timeline_{512,64}.json` — ray.timeline Chrome traces
- `timeline/task_records.json` — list_tasks snapshot (worker_id, name, start/end) for the join
- `timeline/tl{512,64}_phase_breakdown.{csv,md}` — per-task-type phase breakdown (create-stall + read decomposition)
- `timeline/sched{512,64}_concurrency.{csv,md}` — per-op effective parallelism + cluster CPU saturation
- `timeline/sched512_backpressure.{csv,md}` — read output-queue + CPU over progress ticks (not backpressured)
