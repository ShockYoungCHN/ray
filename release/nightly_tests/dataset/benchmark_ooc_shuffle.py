"""Benchmark: Out-of-core shuffle capacity.

Reads TPC-H lineitem from S3 (limited to ``--data-size-gb``), hash-repartitions
by ``column00``, and writes the result to local parquet.  Times the full
read -> shuffle -> write pipeline so we can compare V2 hash shuffle against
prior baselines.

Usage:
    python benchmark_ooc_shuffle.py --data-size-gb 100 --num-partitions 100
"""

import argparse
import gc
import json
import os
import shutil
import time
from datetime import datetime

import ray
from ray.data.context import ShuffleStrategy

# Env vars that must be visible inside Ray worker processes (driver-side
# `os.environ` does NOT propagate to workers on an Anyscale workspace,
# because `ray.init()` attaches to a cluster that is already running).
# Anything read at module-import time on a worker — e.g. the shuffle-
# profile flag in `_shuffle_tasks.py` — must be forwarded via
# `runtime_env={"env_vars": ...}`.
_WORKER_ENV_VARS_TO_FORWARD = (
    "RAY_DATA_SHUFFLE_PROFILE",
)

STRATEGY_MAP = {
    "actorless": ShuffleStrategy.HASH_SHUFFLE,
    "actor": ShuffleStrategy.HASH_SHUFFLE,
}

KEY_COLUMNS = ["column00"]  # l_orderkey
APPROX_BYTES_PER_ROW = 145


def pick_sf(data_size_gb):
    if data_size_gb <= 70:
        return 100
    if data_size_gb <= 700:
        return 1000
    return 10000


def wait_for_object_store_to_drain(threshold_pct=20, timeout_s=180, poll_s=5):
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        mem = ray.cluster_resources().get("object_store_memory", 1)
        avail = ray.available_resources().get("object_store_memory", 0)
        used_pct = (1 - avail / mem) * 100 if mem > 0 else 0
        if used_pct < threshold_pct:
            return
        print(
            f"    waiting for object store to drain ({used_pct:.0f}% used)...",
            flush=True,
        )
        time.sleep(poll_s)
    print(f"    object store drain timed out after {timeout_s}s", flush=True)


def run_one(data_size_gb, num_partitions, strategy_name="actorless", uncap_reduce=False):
    sf = pick_sf(data_size_gb)
    target_rows = int(data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"

    ds = ray.data.read_parquet(path)
    ds = ds.limit(target_rows)

    ds.context.shuffle_strategy = STRATEGY_MAP[strategy_name]
    if uncap_reduce:
        ds.context.set_config("map_reduce_max_concurrent_reducers", 100000)

    repartitioned = ds.repartition(num_partitions, keys=KEY_COLUMNS)

    output_path = "/tmp/shuffle_output"
    shutil.rmtree(output_path, ignore_errors=True)

    print(
        f"  [ray] read(sf{sf}, limit {target_rows:,}) + shuffle -> "
        f"{num_partitions} partitions ({strategy_name}) ... ",
        end="",
        flush=True,
    )

    start = time.perf_counter()
    repartitioned.write_parquet(output_path )
    elapsed = time.perf_counter() - start
    print(f"{elapsed:.1f}s ({target_rows:,} rows, {data_size_gb:.1f} GB)")

    del repartitioned, ds
    gc.collect()
    shutil.rmtree(output_path, ignore_errors=True)
    wait_for_object_store_to_drain()

    return {
        "elapsed_s": elapsed,
        "num_rows": target_rows,
        "actual_gb": round(data_size_gb, 2),
        "status": "ok",
    }


def main():
    parser = argparse.ArgumentParser(description="Out-of-core shuffle benchmark")
    parser.add_argument("--output", type=str, default="benchmark_ooc_shuffle_results.json")
    parser.add_argument("--data-size-gb", type=int, required=True)
    parser.add_argument("--num-partitions", type=int, required=True)
    parser.add_argument(
        "--strategy", type=str, choices=["actorless", "actor"], default="actorless"
    )
    parser.add_argument(
        "--uncap-reduce", action="store_true",
        help="Set map_reduce_max_concurrent_reducers to a large value so the"
        " scheduler is the only authority on reducer concurrency.",
    )
    args = parser.parse_args()

    forwarded_env_vars = {
        k: os.environ[k] for k in _WORKER_ENV_VARS_TO_FORWARD if k in os.environ
    }
    runtime_env = {"env_vars": forwarded_env_vars} if forwarded_env_vars else None
    if forwarded_env_vars:
        print(f"Forwarding env vars to workers: {forwarded_env_vars}")
    ray.init(runtime_env=runtime_env)

    cluster = ray.cluster_resources()
    total_cpu = cluster.get("CPU", 0)
    total_mem_gb = cluster.get("memory", 0) / 1e9
    total_obj_gb = cluster.get("object_store_memory", 0) / 1e9
    in_core_limit_gb = total_obj_gb / 3

    data_size_gb = args.data_size_gb
    num_partitions = args.num_partitions
    strategy_name = args.strategy

    print(
        f"Cluster: {total_cpu:.0f} CPUs, {total_mem_gb:.0f} GB memory, "
        f"{total_obj_gb:.0f} GB object store"
    )
    print(f"Estimated in-core shuffle limit (obj_store/3): {in_core_limit_gb:.0f} GB")
    print(f"Test: {data_size_gb} GB, {num_partitions} partitions, strategy={strategy_name}")
    print()

    ratio = data_size_gb / in_core_limit_gb if in_core_limit_gb > 0 else float("inf")
    zone = "IN-CORE" if ratio <= 1.0 else "SPILL"
    print(
        f"--- {data_size_gb} GB, {num_partitions} partitions, {strategy_name} "
        f"({ratio:.1f}x of in-core limit, {zone}) ---"
    )

    info = run_one(data_size_gb, num_partitions, strategy_name=strategy_name, uncap_reduce=args.uncap_reduce)

    result = {
        "timestamp": datetime.now().isoformat(),
        "cluster": {
            "num_cpus": total_cpu,
            "total_memory_gb": round(total_mem_gb, 1),
            "object_store_gb": round(total_obj_gb, 1),
            "in_core_limit_gb": round(in_core_limit_gb, 1),
        },
        "config": {
            "data_size_gb": data_size_gb,
            "num_partitions": num_partitions,
            "strategy": strategy_name,
            "ratio_to_in_core_limit": round(ratio, 2),
        },
        **info,
    }
    with open(args.output, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\nResults written to {args.output}")
    t = info["elapsed_s"]
    tp = f"{info['actual_gb'] / t:.1f} GB/s" if t and t > 0 else "N/A"
    print(f"  {data_size_gb} GB, {num_partitions} partitions: {t:.1f}s, {tp}")


if __name__ == "__main__":
    main()
