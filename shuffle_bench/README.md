# shuffle_bench — file vs in-memory hash-shuffle sweep

Self-contained Anyscale sweep of `repartition` across data size (and partition size), comparing the two
hash-shuffle arms. The only per-arm difference is `DataContext.use_external_hash_shuffle`:

| arm | `use_external_hash_shuffle` | what it does |
|-----|------------------------------|--------------|
| `disk` | `True`  | external **file-transport** shuffle (local disk + socket; object store carries small handles) |
| `v2`   | `False` | **in-memory / object-store** shuffle (map outputs via plasma; spills under pressure) |

Both do `read_parquet(TPC-H lineitem).limit(rows).repartition(N, keys=[l_orderkey]).write_parquet`, with
`num_partitions = round(data_GB / gb_per_partition)`. (`v2`/`disk` are the codebase's own terms — kept as
arm labels so RESULT lines stay comparable across runs.)

## Files (only these three are tracked — yamls are generated)
- **`bench_shuffle.py`** — the harness. `--data-size-gb`, `--gb-per-partition` (→ num_partitions),
  `--shuffle {v2,disk,both}`, `--target-cpu`, `--result-json`, `--stats`. Emits one
  `RESULT arm=… size=…GB gbpp=… parts=… wall=…s throughput=…GB/s ok=…` per arm (+ a `SPEEDUP` line for `both`).
  `v2` OOM/OwnerDied at large cells is caught and reported as `ok=False` — a data point, not a crash.
- **`gen_jobs.sh [ARM]`** — generates the job yamls for `ARM ∈ {disk, v2, both}` (default `both`).
  Edit `IMAGE` / `COMPUTE` / `TARGET_CPU` / `SIZES` / `ORDER` / `PARTITION_SIZE_GB` at the top.
- `job_<arm>_<size>.yaml` — **generated artifacts, git-ignored.** Regenerate; don't commit.

## Run
```bash
cd shuffle_bench
bash gen_jobs.sh disk           # generate the 7 disk-only job yamls (or: v2 / both)
for f in job_disk_*.yaml; do anyscale job submit -f "$f"; done
```

## Notes
- **`RAY_DATA_USE_HASH_SHUFFLE_V2: "1"` is required** (set in every job) — it routes the keyed
  `repartition` through the v2 planner, the only one with the external(disk) branch. Without it the `disk`
  arm silently runs the in-memory path.
- Runs on `yy-shuffle-32` (32 × m5.2xlarge, gp3). Disk capacity: an 8 TB shuffle writes ~8 TB of shard files
  (+ reduce staging); 32 × 500 GB gp3 = 16 TB is tight at 8 TB (it fit — 7.9 TiB written).
- `v2` is expected to struggle / OOM at the larger, higher-partition cells — that contrast is the point;
  the harness records `ok=False` and continues.

## Sweep results so far (arm=disk, 1 GB/partition, image `yuanzhuo_v3clean_fix`)
| data | parts | wall | throughput |
|------|-------|------|------------|
| 128 GB | 128 | 48.7s | 2.63 GB/s |
| 256 GB | 256 | 77.7s | 3.30 GB/s |
| 512 GB | 512 | 135.0s | 3.79 GB/s |
| 1 TB | 1024 | 259.9s | 3.94 GB/s |
| 2 TB | 2048 | 551.1s | 3.72 GB/s |
| 4 TB | 4096 | 1235.6s | 3.31 GB/s |
| 8 TB | 8192 | 3413.2s | 2.40 GB/s |
