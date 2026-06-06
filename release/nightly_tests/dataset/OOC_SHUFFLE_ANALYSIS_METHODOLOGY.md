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

> **CORRECTION (2026-06-05), see `benchmark_logs/reducer_get_512/ANALYSIS_reducer_get_decomposition.md`.**
> Two earlier claims in this doc were wrong and are fixed below:
> 1. The dominant reduce cost is **not a plasma "restore"**. A reducer pulling a shard
>    spilled on a remote node is served by `ObjectManager::PushFromFilesystem` →
>    `SpilledObjectReader` (reads the spill file directly, chunks it, sends with
>    `from_disk=true`) — it never restores to the remote node's plasma and never uses
>    a restore io_worker. `AsyncRestoreSpilledObject` restore-to-plasma is the *local*-get
>    path only (~3% of fetches). So old fixes #4/#5 below pointed at the wrong code.
> 2. EBS is **not** the binding constraint in reduce. Measured (O_DIRECT, this cluster):
>    write ~316 MB/s, read ~598 MB/s per node. Reduce restore-read demand is only ~8–17%
>    of the aggregate read ceiling — the reducer was **CPU-bound in `SpilledObjectReader`**
>    (byte-by-byte read, ~199 MB/s, *below* the EBS read cap), not disk-bound.

- spill is NOT end-to-end bottleneck through 512 GB (GB/s holds ~2.1 to 295 GB spill).
- the dominant reduce spill cost is the reducer waiting on **remote spilled-object fetch**
  (served by `PushFromFilesystem`), not a plasma restore: `_shuffle_reduce_task` execute
  1164 ms → 8272 ms (64→512 GB); `reduce.ray_get_s` mean 4.65 s/reducer at 512 GB.
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
  dominate the back half. Net: the only large spill cost is the reduce-side
  remote spilled-object fetch (served by `ObjectManager::PushFromFilesystem`).

## Throughput model (corrected)

The job is NOT a single fully-overlapped pipeline. The hash-shuffle **barrier**
(a reducer needs every mapper's shard for its partition) splits it into two
largely-sequential phases:

    total_time  ≈  T(read+map)  +  T(reduce+write)        # barrier between them
    GB/s        =  data / total_time

So the ceiling is **the current binding stage's rate, across ALL stages**
— read (S3 + decode), map (partition + encode), reduce (remote-spilled fetch +
decode + reduce_fn), AND **write** (encode + local-disk write). It is NOT a hard wall:
speeding up any stage shortens its phase and **raises GB/s by ~that stage's
share of total time** (whack-a-mole — the next stage then binds). Data size
alone does not raise the ceiling; optimizing a binding stage does.

Measured shares at 512 GB (from stage spans; phases overlap internally):

| phase | stages | span | ~share | binding factor |
|---|---|---|---|---|
| 1 | read (~124s) ‖ map (~112s) | ~124 s | **~51%** | read (S3 + parquet decode) |
| 2 | reduce (~108s) ‖ write (~113s) | ~113 s | **~47%** | remote-spilled fetch + write |

Within phase 2, the reduce remote-spilled fetch (`ray_get` wall ~47 s, spill-portion
~25-40 s) ≈ **10-16% of total**. So at 512 GB: **read is the biggest single lever
(~51%)**, write is a non-trivial phase-2 cost (easy to overlook), and efficient
spilled-object serving buys ~10-16% now — a share that **grows super-linearly with
data** as the fetch path degrades, eventually becoming the dominant lever (and bending
the GB/s plateau downward if left unfixed).

## Reduce remote-spilled-fetch path — inefficiencies & proposed fixes

Two distinct serving paths (verified in source); the dominant one is the remote serve:

- **Remote shard spilled on node B** (~97% of a reducer's shards; ~95% of those spilled →
  the dominant cost): A pulls → B serves via `ObjectManager::Push` → `PushFromFilesystem`
  (`object_manager.cc:410`) → `SpilledObjectReader` reads the spill file directly, chunks
  it (`object_chunk_size`=5 MiB), sends `from_disk=true`. **No plasma restore, no io_worker.**
- **Local shard spilled on A** (~3%): A's own raylet restores to A's plasma via
  `AsyncRestoreSpilledObject` (`local_object_manager.cc:574`, the io_worker / 1-obj-per-RPC
  path). Cheap and rare — **not worth optimizing.**

Highest-value first:

1. **`SpilledObjectReader` is CPU-bound on a byte-by-byte read (core, biggest).**
   `ReadFromDataSection`/`ReadFromMetadataSection` (`spilled_object_reader.cc:171-197`,
   current HEAD) copy the payload one byte at a time via `istreambuf_iterator`+`push_back`,
   capping the reader at **~199 MB/s — below the EBS read cap (598)**, so the reader, not
   the disk, binds. *Fix:* bulk read; read straight into the buffer `ChunkObjectReader::GetChunk`
   already `reserve()`d (don't `resize()` — that re-zeros what `read` overwrites, undoing
   #51330) via `absl STLStringResizeUninitialized`; open the file once (cached fd) + `pread`
   instead of an `ifstream` per call. Measured (warm, 0.68 MiB, -O2): 3589 → 85 → ~51 µs/obj
   (~42× from the bulk read, ~32% more from dropping the zero-init). Candidate +
   review: `/tmp/spilled_object_reader.improved.cc`. Note: warm-cache CPU deltas; on a cold
   read the disk dominates and these shrink to ~1-2%.
2. **No fetch/decode overlap, reducer side** (`_shuffle_tasks.py:423-435`): strictly serial
   `ray.get(batch)` → decode → next `ray.get`; no prefetch / `ray.wait`. *Fix:* prefetch the
   next batch / consume via `ray.wait` in completion order → hide fetch behind the 2.74 s
   `ipc_decode`. Also fixes head-of-line blocking within a `ray.get(16)` (one slow shard
   stalls 15).
3. **Pull window capped at batch=16** (`_REDUCE_BATCH_SIZE`, line 47): only 16 refs in
   flight. *Fix:* decouple the pull window (large) from the decoded-accumulator memory cap.
4. **No coalescing across pushes by fused file** (core, follow-up): spill fuses ≤2000 objs /
   100 MB into one file (`min_spilling_size`, `max_fused_object_count`), but each object is
   served by an independent `PushFromFilesystem` → the same fused file is opened/read once
   per object across pushes. Under OOC memory pressure page cache thrashes and the file gets
   cold-re-read. *Fix:* a per-`file_path` reader/buffer cache outside the per-push reader.
5. **Tiny shards** (median 0.68 MiB) amplify per-object open/serve overhead on every path.
   *Fix:* coarser partitioning / fewer-larger shards (trade vs parallelism/skew).

Scope note — these are likely **not** the shuffle-reduce lever, but the path attribution
below is from a microbench + topology model and the end-to-end impact is **still pending
re-measurement** (task #2), so treat the magnitudes as provisional:
- `max_io_workers`: the remote-serve path (`PushFromFilesystem`) doesn't use restore io_workers.
- **Batched restore at `AsyncRestoreSpilledObject` + `external_storage.py` file-IO batching is
  now MERGED (commit `4c021408f5`).** It speeds the **local** restore-to-plasma path — which is
  reached only via `PullManager` (a local consumer restoring a locally-spilled object). In the
  512 GB/500p hash-shuffle reduce that's ~3% of a reducer's fetches by topology, but it can
  dominate locality-heavy / non-shuffle workloads (the commit targets `q5 sf=10000`'s restore
  RPC-storm). The dominant remote+spilled fetch still needs the same batching idea in
  `SpilledObjectReader`/`PushFromFilesystem` (tasks #1/#4) — *that* part is unverified end-to-end.
- "use NVMe / raise EBS bandwidth": reduce appears not disk-bound here (~8-17% of the read
  ceiling), but this is an aggregate estimate, not a per-second iostat trace.
⚠ the page-cache `spill_throughput_mb` metric (>1000 MB/s) hides the real device rate — measured
O_DIRECT: write ~316 MB/s, read ~598 MB/s per node.

Cheapest high-impact combo: **#1 + #2** (fast reader + overlap fetch/decode). Verify by
rebuilding the raylet with #1 and re-running `probe_reducer_get_paths.py` (expect remote+spilled
~287 → ~600-1000 MB/s), and the no-spill counterfactual (bump object store, rerun 512 GB,
watch `reduce.ray_get_s` collapse).

## File index (under benchmark_logs/)

- `reducer_get_512/ANALYSIS_reducer_get_decomposition.md` — 4-path decomposition of `reduce.ray_get_s` (local/remote × mem/spill), the `PushFromFilesystem` correction, EBS measurement, and the `SpilledObjectReader` fix + micro numbers
- `reducer_get_512/{result_512gb.json, run.log}` — the 512 GB/500p profiling run behind this analysis
- `../probe_reducer_get_paths.py` — cluster microbench: per-path `ray.get` cost (NodeAffinity + forced spill); `/tmp/spill_read_micro.cc`, `/tmp/spill_zeroinit_micro.cc` — reader byte-by-byte vs bulk vs zero-init isolation
- `sweep_500p/summary_table.{md,csv}` — per-stage wall + shuffle sub-timers, all sizes (Ray Data `ds.stats`)
- `sweep_500p/{sweep.log, run_64gb.log, result_*gb.json}` — raw sweep output + spill metrics
- `20260604_0948*/` — per-node raw `*_spill_log.txt` (256/384/512 GB); `094821/plots*/` spill PNGs
- `timeline/timeline_{512,64}.json` — ray.timeline Chrome traces
- `timeline/task_records.json` — list_tasks snapshot (worker_id, name, start/end) for the join
- `timeline/tl{512,64}_phase_breakdown.{csv,md}` — per-task-type phase breakdown (create-stall + read decomposition)
- `timeline/sched{512,64}_concurrency.{csv,md}` — per-op effective parallelism + cluster CPU saturation
- `timeline/sched512_backpressure.{csv,md}` — read output-queue + CPU over progress ticks (not backpressured)
