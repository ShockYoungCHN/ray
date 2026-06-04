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

## Throughput model (corrected)

The job is NOT a single fully-overlapped pipeline. The hash-shuffle **barrier**
(a reducer needs every mapper's shard for its partition) splits it into two
largely-sequential phases:

    total_time  ≈  T(read+map)  +  T(reduce+write)        # barrier between them
    GB/s        =  data / total_time

So the ceiling is **the current binding stage's rate, across ALL stages**
— read (S3 + decode), map (partition + encode), reduce (restore + decode +
reduce_fn), AND **write** (encode + local-disk write). It is NOT a hard wall:
speeding up any stage shortens its phase and **raises GB/s by ~that stage's
share of total time** (whack-a-mole — the next stage then binds). Data size
alone does not raise the ceiling; optimizing a binding stage does.

Measured shares at 512 GB (from stage spans; phases overlap internally):

| phase | stages | span | ~share | binding factor |
|---|---|---|---|---|
| 1 | read (~124s) ‖ map (~112s) | ~124 s | **~51%** | read (S3 + parquet decode) |
| 2 | reduce (~108s) ‖ write (~113s) | ~113 s | **~47%** | reduce restore + write |

Within phase 2, reduce restore (`ray_get` wall ~47 s, spill-portion ~25-40 s)
≈ **10-16% of total**. So at 512 GB: **read is the biggest single lever
(~51%)**, write is a non-trivial phase-2 cost (easy to overlook), and efficient
spill buys ~10-16% now — a share that **grows super-linearly with data** as
restore degrades, eventually becoming the dominant lever (and bending the GB/s
plateau downward if left unfixed).

## Reduce restore path — inefficiencies & proposed fixes

Where the dominant spill cost lives and how to cut it (highest-value first):

1. **No fetch/decode overlap** (`_shuffle_tasks.py:423-435`): strictly serial
   `ray.get(batch)` → decode → next `ray.get`; no prefetch / `ray.wait`. While
   decoding, the raylet isn't restoring the next batch. *Fix:* prefetch the next
   batch / consume via `ray.wait` in completion order → hide restore behind decode.
2. **In-flight restore window capped at batch=16** (`_REDUCE_BATCH_SIZE`, line 47):
   only 16 refs ever requested at once. *Fix:* decouple the pull window (large,
   to saturate disk) from the decoded-accumulator memory cap (small).
3. **Head-of-line blocking within a batch**: `ray.get(16)` waits for the slowest
   of 16 (one spilled straggler stalls 15 in-memory). *Fix:* `ray.wait`.
4. **Restore concurrency vs the spill BACKEND** (`max_io_workers=4`,
   `ray_config_def.h:719`, shared by spill AND restore). The right fix depends
   entirely on what spill lands on — "raise io_workers" is only correct for
   local NVMe:
   - **Local NVMe (instance store: m5d/i3/i4i):** concurrency-limited → raise
     `max_io_workers` to the device's parallelism. Best case for spill perf.
   - **EBS (THIS cluster — m5.2xlarge, root `nvme0n1` = "Amazon Elastic Block
     Store", NO instance store):** bandwidth-CAPPED. Measured spill ≈ **600 MB/s
     per node**, which matches the m5.2xlarge **instance-level EBS bandwidth cap
     (~593 MB/s / 4750 Mbps)** — i.e. the bottleneck is the instance's EBS pipe,
     not the volume and definitely not NVMe (which would be multi-GB/s). 4
     io_workers already saturate this pipe → **raising `max_io_workers` does
     nothing here.** *Fix:* use instance-store NVMe nodes (m5d/i3/i4i) to lift
     the cap to GB/s, OR reduce spill VOLUME so less crosses the capped pipe
     (bigger object store / better overlap), OR a larger instance with more EBS
     bandwidth. ⚠ the page-cache `spill_throughput_mb` metric (>1000 MB/s) HIDES
     this — the real rate is ~600 MB/s (`iostat -dx` under load confirms).
   - **S3 object spilling:** latency-bound (~tens of ms/request) → raise
     concurrency a LOT to hide latency, AND fix #5 first: per-object GETs of the
     0.7 MiB median shard are catastrophic on S3.
5. **Write/read fusing asymmetry**: spill fuses ≤2000 objs / 100 MB into one file
   (`min_spilling_size`, `max_fused_object_count`), but `AsyncRestoreSpilledObject`
   restores **1 object per RPC** (`local_object_manager.cc:574`) → many small reads
   of the median ~0.7 MiB shard. *Fix:* batch-restore by fused file (core change).
6. **Tiny shards** (median 0.7 MiB) amplify per-object RPC/io-worker overhead.
   *Fix:* coarser partitioning / fewer-larger shards (trade vs parallelism/skew).

Cheapest high-impact combo: **#1 + #4** (overlap restore with decode + raise
restore concurrency). Verify with the no-spill counterfactual (bump object store,
rerun 512 GB, watch `reduce.ray_get_s` collapse).

## File index (under benchmark_logs/)

- `sweep_500p/summary_table.{md,csv}` — per-stage wall + shuffle sub-timers, all sizes (Ray Data `ds.stats`)
- `sweep_500p/{sweep.log, run_64gb.log, result_*gb.json}` — raw sweep output + spill metrics
- `20260604_0948*/` — per-node raw `*_spill_log.txt` (256/384/512 GB); `094821/plots*/` spill PNGs
- `timeline/timeline_{512,64}.json` — ray.timeline Chrome traces
- `timeline/task_records.json` — list_tasks snapshot (worker_id, name, start/end) for the join
- `timeline/tl{512,64}_phase_breakdown.{csv,md}` — per-task-type phase breakdown (create-stall + read decomposition)
- `timeline/sched{512,64}_concurrency.{csv,md}` — per-op effective parallelism + cluster CPU saturation
- `timeline/sched512_backpressure.{csv,md}` — read output-queue + CPU over progress ticks (not backpressured)
