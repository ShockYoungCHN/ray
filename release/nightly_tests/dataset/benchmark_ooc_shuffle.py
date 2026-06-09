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
from typing import List
from datetime import datetime

import ray
from ray.data.context import ShuffleStrategy

from spill_metrics_dump import (
    collect_spill_metrics,
    default_output_dir,
    snapshot_spill_metrics,
    summarize,
    write_benchmark_summary_to_raylet_logs,
)

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


def _truncate_event_logs_on_all_nodes(paths: List[str]) -> None:
    """Truncate each path in ``paths`` on every alive node so each benchmark
    run starts with clean event log files instead of appending to whatever
    previous runs in this raylet session left behind.

    Safe to call while the raylet has the files open: spdlog opens them with
    O_APPEND, so subsequent writes seek to end-of-file (now offset 0) and
    just continue from there. Used for both raylet_spill_events.out and
    raylet_pull_events.out — same semantics, same truncation policy.
    """
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    @ray.remote(num_cpus=0)
    def _truncate(target_paths: List[str]) -> List[str]:
        ip = ray.util.get_node_ip_address()
        out = []
        for path in target_paths:
            try:
                with open(path, "w"):
                    pass
                out.append(f"{ip}: truncated {path}")
            except OSError as e:
                out.append(f"{ip}: FAILED to truncate {path}: {e}")
        return out

    nodes = [n for n in ray.nodes() if n.get("Alive")]
    futures = []
    for n in nodes:
        sched = NodeAffinitySchedulingStrategy(node_id=n["NodeID"], soft=False)
        futures.append(_truncate.options(scheduling_strategy=sched).remote(paths))
    for lines in ray.get(futures):
        for line in lines:
            print(f"  [event-log] {line}", flush=True)


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

    if os.environ.get("RAY_DATA_SHUFFLE_PROFILE") == "1":
        print("\n========== ds.stats() ==========")
        print(repartitioned.stats())
        print("================================\n")

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
    parser.add_argument("--output", type=str, default=None,
        help="Output JSON path. Defaults to "
             "<persistent_log_dir>/<ts>/benchmark_ooc_shuffle.json.")
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
    parser.add_argument(
        "--timeline-out", type=str, default=None,
        help="If set, dump ray.timeline() (Chrome trace) to this path after the"
        " workload, for task:store_outputs / task:execute phase analysis.",
    )
    parser.add_argument(
        "--no-truncate-spill-log", action="store_true",
        help="By default each run truncates BOTH raylet_spill_events.out and"
        " raylet_pull_events.out on every node so post-run analysis sees only"
        " this run's events. Pass this flag to keep spdlog's default"
        " append-across-sessions behavior for both files. (Flag name kept for"
        " backward compatibility; it now covers all raylet event logs.)",
    )
    args = parser.parse_args()

    # Define unique experiment output directory
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    experiment_dir = os.path.join(default_output_dir(), ts_str)
    os.makedirs(experiment_dir, exist_ok=True)

    # Fixed event log paths for all nodes (raylet picks them up via env vars
    # set in main.cc; defaults to /tmp/raylet_*_events.out if unset).
    spill_log_path = "/tmp/raylet_spill_events.out"
    pull_log_path = "/tmp/raylet_pull_events.out"

    forwarded_env_vars = {
        k: os.environ[k] for k in _WORKER_ENV_VARS_TO_FORWARD if k in os.environ
    }
    # Mandate the event log paths for all workers and nodes.
    forwarded_env_vars["RAY_SPILL_EVENTS_LOG_PATH"] = spill_log_path
    forwarded_env_vars["RAY_PULL_EVENTS_LOG_PATH"] = pull_log_path

    runtime_env = {"env_vars": forwarded_env_vars}
    print(f"Forwarding env vars to workers: {forwarded_env_vars}")
    ray.init(address="auto", runtime_env=runtime_env)

    if not args.no_truncate_spill_log:
        event_log_paths = [spill_log_path, pull_log_path]
        print(f"Truncating event logs {event_log_paths} on all alive nodes ...", flush=True)
        _truncate_event_logs_on_all_nodes(event_log_paths)

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

    # Baseline snapshot before workload so the post-run delta is just this
    # benchmark, not cluster-lifetime cumulative.
    print("Snapshotting spill metrics baseline ...", flush=True)
    snapshot_before = snapshot_spill_metrics()

    benchmark_start_ts = time.time()
    print(f"BENCHMARK_START_TS {benchmark_start_ts:.6f}", flush=True)
    info = run_one(data_size_gb, num_partitions, strategy_name=strategy_name, uncap_reduce=args.uncap_reduce)

    if args.timeline_out:
        # Dump the cluster timeline from this same Ray session.  Includes
        # task:execute and task:store_outputs profile events; filter by
        # BENCHMARK_START_TS downstream to scope to this run.
        ray.timeline(filename=args.timeline_out)
        print(f"TIMELINE written to {args.timeline_out}", flush=True)

    # Collect metrics and pull raw log files from all nodes into experiment_dir.
    # Delta against snapshot_before; raw cumulative preserved under raw_after.
    spill_metrics = collect_spill_metrics(
        start_ts=benchmark_start_ts,
        output_dir=experiment_dir,
        snapshot_before=snapshot_before,
    )

    # Append a BENCHMARK_FINALIZE line to every node's spill events log so
    # the per-run delta lives next to the per-spill events.
    try:
        finalize_writes = write_benchmark_summary_to_raylet_logs(
            benchmark_tag=ts_str,
            snapshot_before=snapshot_before,
            snapshot_after=spill_metrics.get("raw_after", {}),
            delta_per_node=spill_metrics["per_node"],
        )
        ok = sum(1 for r in finalize_writes if r.get("ok"))
        print(
            f"BENCHMARK_FINALIZE written to {ok}/{len(finalize_writes)} "
            f"raylet spill logs",
            flush=True,
        )
    except Exception as e:
        print(
            f"WARN: failed to write BENCHMARK_FINALIZE to raylet logs: "
            f"{type(e).__name__}: {e}",
            flush=True,
        )

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
        "spill_metrics": spill_metrics,
    }

    output_path = args.output
    if output_path is None:
        output_path = os.path.join(experiment_dir, "benchmark_ooc_shuffle.json")

    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print()
    print(summarize(spill_metrics))
    print(f"\nResults and raw logs written to {experiment_dir}")
    t = info["elapsed_s"]
    tp = f"{info['actual_gb'] / t:.1f} GB/s" if t and t > 0 else "N/A"
    print(f"  {data_size_gb} GB, {num_partitions} partitions: {t:.1f}s, {tp}")


if __name__ == "__main__":
    main()
