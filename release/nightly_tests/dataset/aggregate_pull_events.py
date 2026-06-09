"""Scan every node's raylet_pull_events.out and aggregate bundle-pull latencies.

This file is written by the dedicated spdlog logger initialized in
``PullManager`` (see ``InitPullEventLogger`` in
``src/ray/object_manager/pull_manager.cc``).  Every line is one event; no
JSON envelope.  See ``PULL_EVENTS_LOG_FORMAT.md`` for the schema.

Three event phases per bundle:
  - phase=bundle_locate   (T0 -> T1): subscribe to all-locations-resolved
  - phase=bundle_active   (T1 -> T2): plasma-quota wait before activation
  - phase=bundle_complete (T0 -> T3): full end-to-end fetch wall time

This script:
  1. fans out one remote task per node (NodeAffinity-pinned) to parse the
     local file — beats fetching over the network for big runs;
  2. joins the three phases per bundle by ``(node_ip, req_id)`` (req_id is
     only unique within a single raylet process);
  3. emits per-task and per-phase percentile summaries, separating warm
     (already-local-at-construct) bundles from cold ones.

Usage:
    python aggregate_pull_events.py [--output path.json]
"""

import argparse
import glob
import json
import math
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# Same grammar as raylet_spill_events.out: `<ts> key=val key=val ...`.
LINE_RE = re.compile(r"^(\d+\.\d+)\s+(.*)$")
KV_RE = re.compile(r"(\w+)=(\S+)")

DEFAULT_LOG_DIR = "/home/ray/default/benchmark_logs"
DEFAULT_LOG_GLOB = "/tmp/raylet_pull_events.out*"


def _parse_kvs(s: str) -> Dict[str, str]:
    # Pull events never use trailing commas (unlike SpillObjectsInternal in
    # the spill log) — but strip defensively in case the format drifts.
    return {k: v.rstrip(",") for k, v in KV_RE.findall(s)}


@ray.remote(num_cpus=0)
def _scan_node(log_glob: str) -> Dict[str, Any]:
    """Run on a worker node — parse the local pull-events file."""
    events: List[Dict[str, Any]] = []
    paths = sorted(glob.glob(log_glob))
    for p in paths:
        try:
            with open(p, "r", errors="replace") as f:
                for line in f:
                    m = LINE_RE.match(line.strip())
                    if not m:
                        continue
                    ts = float(m.group(1))
                    kv = _parse_kvs(m.group(2))
                    if "phase" not in kv:
                        continue
                    kv["ts"] = ts
                    events.append(kv)
        except FileNotFoundError:
            continue
    return {
        "node_ip": ray.util.get_node_ip_address(),
        "files_scanned": paths,
        "num_events": len(events),
        "events": events,
    }


def _percentile(sorted_values: List[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return sorted_values[lo]
    frac = rank - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def _summarize_distribution(values: List[float]) -> Dict[str, Any]:
    """min / p50 / p90 / p99 / max / mean / count of a list of floats."""
    if not values:
        return {"count": 0}
    s = sorted(values)
    n = len(s)
    return {
        "count": n,
        "min": s[0],
        "p50": _percentile(s, 50),
        "p90": _percentile(s, 90),
        "p99": _percentile(s, 99),
        "max": s[-1],
        "mean": sum(s) / n,
        "sum": sum(s),
    }


def _summarize(per_node: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Group events per (node_ip, req_id) so we can join the three phases of
    # one bundle. req_id alone is not unique across nodes (each raylet
    # process numbers from 0).
    per_bundle: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = defaultdict(dict)

    total_events = 0
    phase_counts: Dict[str, int] = defaultdict(int)
    for n in per_node:
        node_ip = n["node_ip"]
        for ev in n["events"]:
            total_events += 1
            phase = ev["phase"]
            phase_counts[phase] += 1
            req_id = ev.get("req_id")
            if req_id is None:
                continue
            per_bundle[(node_ip, req_id)][phase] = ev

    # Per-task latency buckets. Each value is a per-bundle scalar (e.g. one
    # bundle_locate.latency_ms reading), grouped by (task_name, warm flag).
    per_task_locate: Dict[Tuple[str, str], List[float]] = defaultdict(list)
    per_task_active: Dict[str, List[float]] = defaultdict(list)
    per_task_complete_transfer: Dict[str, List[float]] = defaultdict(list)
    per_task_complete_total: Dict[str, List[float]] = defaultdict(list)
    per_task_bundle_size: Dict[str, List[int]] = defaultdict(list)
    warm_counter = {"warm": 0, "cold": 0}

    # Bundles missing the bundle_complete line — failed / cancelled / still
    # in flight when we dumped.  Tracked separately so the cold-completion
    # ratio is recoverable.
    incomplete_by_task: Dict[str, int] = defaultdict(int)

    for (_node_ip, _req_id), phases in per_bundle.items():
        locate = phases.get("bundle_locate")
        active = phases.get("bundle_active")
        complete = phases.get("bundle_complete")

        if locate is not None:
            task = locate.get("task", "-")
            warm = locate.get("warm", "0")
            warm_counter["warm" if warm == "1" else "cold"] += 1
            try:
                latency = float(locate["latency_ms"])
                per_task_locate[(task, "warm" if warm == "1" else "cold")].append(
                    latency
                )
            except (KeyError, ValueError):
                pass
            try:
                per_task_bundle_size[task].append(int(locate["bundle_size"]))
            except (KeyError, ValueError):
                pass

        if active is not None:
            task = active.get("task", "-")
            try:
                per_task_active[task].append(float(active["wait_ms"]))
            except (KeyError, ValueError):
                pass

        if complete is not None:
            task = complete.get("task", "-")
            try:
                per_task_complete_transfer[task].append(
                    float(complete["transfer_ms"])
                )
                per_task_complete_total[task].append(float(complete["total_ms"]))
            except (KeyError, ValueError):
                pass
        elif locate is not None:
            incomplete_by_task[locate.get("task", "-")] += 1

    # Per-task summary block. Keep `task` keys stable so downstream dashboards
    # don't drift when task names change order.
    tasks = sorted(
        {task for (task, _warm) in per_task_locate}
        | set(per_task_active.keys())
        | set(per_task_complete_transfer.keys())
    )
    per_task_summary = {}
    for task in tasks:
        per_task_summary[task] = {
            "bundle_size": _summarize_distribution(
                [float(x) for x in per_task_bundle_size.get(task, [])]
            ),
            "bundle_locate_ms_cold": _summarize_distribution(
                per_task_locate.get((task, "cold"), [])
            ),
            "bundle_locate_ms_warm": _summarize_distribution(
                per_task_locate.get((task, "warm"), [])
            ),
            "bundle_active_wait_ms": _summarize_distribution(
                per_task_active.get(task, [])
            ),
            "bundle_complete_transfer_ms": _summarize_distribution(
                per_task_complete_transfer.get(task, [])
            ),
            "bundle_complete_total_ms": _summarize_distribution(
                per_task_complete_total.get(task, [])
            ),
            "incomplete_bundles": incomplete_by_task.get(task, 0),
        }

    total_bundles = len(per_bundle)
    warm_total = warm_counter["warm"]
    cold_total = warm_counter["cold"]

    return {
        "total_events": total_events,
        "total_bundles": total_bundles,
        "warm_bundles": warm_total,
        "cold_bundles": cold_total,
        "warm_ratio": warm_total / max(1, warm_total + cold_total),
        "phase_counts": dict(phase_counts),
        "incomplete_bundles_total": sum(incomplete_by_task.values()),
        "num_nodes_with_events": sum(1 for n in per_node if n["num_events"] > 0),
        "num_nodes_scanned": len(per_node),
        "per_task": per_task_summary,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-glob",
        default=DEFAULT_LOG_GLOB,
        help="Glob (evaluated on every node) for the dedicated pull-event log files.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=f"Output JSON. Defaults to <persistent>/pull_events_<ts>.json. "
        f"Persistent dir: {DEFAULT_LOG_DIR}.",
    )
    args = parser.parse_args()

    ray.init(address="auto")

    nodes = [n for n in ray.nodes() if n.get("Alive")]
    print(f"Scanning {len(nodes)} live nodes for pull events...")

    futures = []
    for n in nodes:
        node_id = n["NodeID"]
        sched = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
        futures.append(_scan_node.options(scheduling_strategy=sched).remote(args.log_glob))

    per_node = ray.get(futures)
    summary = _summarize(per_node)

    output_path: Optional[str] = args.output
    if output_path is None:
        out_dir = DEFAULT_LOG_DIR if os.path.isdir(DEFAULT_LOG_DIR) else os.getcwd()
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(out_dir, f"pull_events_{ts}.json")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    result = {
        "timestamp": datetime.now().isoformat(),
        "summary": summary,
        "per_node": [
            {
                "node_ip": n["node_ip"],
                "files_scanned": n["files_scanned"],
                "num_events": n["num_events"],
            }
            for n in per_node
        ],
    }
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(summary, indent=2))
    print(f"\nWrote {output_path}")


if __name__ == "__main__":
    main()
