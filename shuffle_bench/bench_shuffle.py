"""Sweep harness: disk (file-transport, v3) vs v2 (in-memory/object-store) hash
shuffle, parameterized by DATA SIZE and PARTITION SIZE (GB per partition).

num_partitions = round(data_size_gb / gb_per_partition), so:
  1GB/partition -> 1TB=1024, 2TB=2048, 4TB=4096, 8TB=8192 partitions
  2GB/partition -> 1TB=512,  2TB=1024, 4TB=2048, 8TB=4096 partitions

The only knob that distinguishes the two arms is DataContext.use_external_hash_shuffle:
  v2   -> False : map outputs via the Ray object store; spills to disk under pressure
  disk -> True  : file-transport; local disk + socket; object store carries small handles only
Both keep ShuffleStrategy.HASH_SHUFFLE. ("v2"/"disk" are kept as arm labels because they
are the codebase's own terms and match the RESULT lines already collected.)

Usage:
  python bench_shuffle.py --data-size-gb 1024 --gb-per-partition 1 --shuffle both
  python bench_shuffle.py --data-size-gb 8192 --gb-per-partition 2 --shuffle disk --target-cpu 256
"""

import argparse
import gc
import json
import os
import shutil
import sys
import time
from datetime import datetime

import ray
from ray.data.context import DataContext, ShuffleStrategy

KEY_COLUMNS = ["column00"]  # l_orderkey
APPROX_BYTES_PER_ROW = 145

# v2 (in-memory) reads RAY_DATA_SHUFFLE_PROFILE at worker import; forward if set.
_WORKER_ENV_VARS_TO_FORWARD = ("RAY_DATA_SHUFFLE_PROFILE",)

# arm -> use_external_hash_shuffle
_ARM_EXTERNAL = {"v2": False, "disk": True}


def pick_sf(data_size_gb: int) -> int:
    if data_size_gb <= 70:
        return 100
    if data_size_gb <= 700:
        return 1000
    return 10000


def partitions_for(data_size_gb: int, gb_per_partition: float) -> int:
    return max(1, round(data_size_gb / gb_per_partition))


def wait_for_object_store_to_drain(threshold_pct=20, timeout_s=180, poll_s=5):
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        mem = ray.cluster_resources().get("object_store_memory", 1)
        avail = ray.available_resources().get("object_store_memory", 0)
        used_pct = (1 - avail / mem) * 100 if mem > 0 else 0
        if used_pct < threshold_pct:
            return
        print(f"    draining object store ({used_pct:.0f}% used)...", flush=True)
        time.sleep(poll_s)
    print(f"    object store drain timed out after {timeout_s}s", flush=True)


def configure_shuffle(ctx, arm: str) -> None:
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = _ARM_EXTERNAL[arm]


def run_one(*, data_size_gb, num_partitions, gb_per_partition, arm, output_path, dump_stats):
    sf = pick_sf(data_size_gb)
    target_rows = int(data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    shutil.rmtree(output_path, ignore_errors=True)

    ds = ray.data.read_parquet(path).limit(target_rows)
    configure_shuffle(ds.context, arm)
    repartitioned = ds.repartition(num_partitions, keys=KEY_COLUMNS)

    print(
        f"  [{arm}] read(sf{sf}, limit {target_rows:,}) + shuffle -> "
        f"{num_partitions} parts ({gb_per_partition}GB/part) + write ... ",
        end="",
        flush=True,
    )
    start = time.perf_counter()
    ok, err = True, None
    try:
        repartitioned.write_parquet(output_path)
    except Exception as e:  # v2 can OOM/OwnerDied at large data/high partitions
        ok, err = False, repr(e)
    elapsed = time.perf_counter() - start
    print(f"{elapsed:.1f}s ({target_rows:,} rows) ok={ok}", flush=True)

    stats_str = None
    if dump_stats and ok:
        stats_str = repartitioned.stats()
        print("\n===== ds.stats() =====\n" + stats_str + "\n", flush=True)

    del repartitioned, ds
    gc.collect()
    shutil.rmtree(output_path, ignore_errors=True)
    wait_for_object_store_to_drain()

    gbps = data_size_gb / elapsed if (elapsed > 0 and ok) else 0.0
    return {
        "arm": arm,
        "data_size_gb": data_size_gb,
        "gb_per_partition": gb_per_partition,
        "num_partitions": num_partitions,
        "sf": sf,
        "rows": target_rows,
        "elapsed_s": elapsed,
        "throughput_gbps": gbps,
        "ok": ok,
        "error": err,
        "stats": stats_str,
    }


def _wait_for_fleet(target_cpu, timeout_s=1800):
    print(f"Waiting for {target_cpu} CPU ...", flush=True)
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        if ray.cluster_resources().get("CPU", 0) >= target_cpu:
            break
        time.sleep(10)
    print(f"Fleet ready: {ray.cluster_resources().get('CPU', 0):.0f} CPU", flush=True)


def _print_cluster_summary(data_size_gb):
    c = ray.cluster_resources()
    obj_gb = c.get("object_store_memory", 0) / 1e9
    nodes = len([n for n in ray.nodes() if n.get("Alive")])
    in_core = obj_gb / 3 if obj_gb > 0 else 0
    ratio = data_size_gb / in_core if in_core > 0 else float("inf")
    print(
        f"Cluster: {nodes} nodes, {c.get('CPU',0):.0f} CPU, "
        f"{c.get('memory',0)/1e9:.0f} GB mem, {obj_gb:.0f} GB obj store | "
        f"in-core(obj/3)~={in_core:.0f}GB, {data_size_gb}GB => {ratio:.1f}x",
        flush=True,
    )


def _result_line(r):
    return (
        f"RESULT arm={r['arm']} size={r['data_size_gb']}GB "
        f"gbpp={r['gb_per_partition']} parts={r['num_partitions']} "
        f"rows={r['rows']:,} wall={r['elapsed_s']:.1f}s "
        f"throughput={r['throughput_gbps']:.2f}GB/s ok={r['ok']}"
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data-size-gb", type=int, required=True)
    p.add_argument("--gb-per-partition", type=float, required=True,
                   help="GB per partition; num_partitions = data_size_gb / this.")
    p.add_argument("--shuffle", choices=["v2", "disk", "both"], default="both")
    p.add_argument("--target-cpu", type=int, default=256)
    p.add_argument("--output-path", default="/tmp/shuffle_output")
    p.add_argument("--result-json", default=None)
    p.add_argument("--stats", action="store_true")
    args = p.parse_args()

    forwarded = {k: os.environ[k] for k in _WORKER_ENV_VARS_TO_FORWARD if k in os.environ}
    ray.init(address="auto", runtime_env={"env_vars": forwarded} if forwarded else None)
    DataContext.get_current().use_datasource_v2 = True

    _wait_for_fleet(args.target_cpu)
    _print_cluster_summary(args.data_size_gb)

    nparts = partitions_for(args.data_size_gb, args.gb_per_partition)
    arms = ["v2", "disk"] if args.shuffle == "both" else [args.shuffle]
    print(f"Running: {', '.join(arms)} | {args.data_size_gb}GB / "
          f"{args.gb_per_partition}GB-per-part = {nparts} partitions\n", flush=True)

    results = []
    for arm in arms:
        t0 = time.time()
        r = run_one(
            data_size_gb=args.data_size_gb, num_partitions=nparts,
            gb_per_partition=args.gb_per_partition, arm=arm,
            output_path=args.output_path, dump_stats=args.stats,
        )
        print("\n" + _result_line(r), flush=True)
        print(f"(total incl. setup: {time.time()-t0:.1f}s)  {datetime.now().isoformat()}\n",
              flush=True)
        results.append(r)

    if args.shuffle == "both" and len(results) == 2 and all(x["ok"] for x in results):
        v2_t, disk_t = results[0]["elapsed_s"], results[1]["elapsed_s"]
        sp = v2_t / disk_t if disk_t > 0 else float("inf")
        print(f"SPEEDUP disk vs v2: {sp:.2f}x  (v2={v2_t:.1f}s, disk={disk_t:.1f}s)", flush=True)

    if args.result_json:
        slim = [{k: v for k, v in r.items() if k != "stats"} for r in results]
        with open(args.result_json, "w") as f:
            json.dump(slim, f, indent=2)
        print(f"RESULTS_JSON {args.result_json}", flush=True)

    print("DONE", flush=True)


if __name__ == "__main__":
    sys.exit(main())
