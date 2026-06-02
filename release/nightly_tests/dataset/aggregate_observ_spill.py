"""Scan every node's raylet_spill_events.out and aggregate by (trigger, creator).

This file is written by the dedicated spdlog logger initialized in
``LocalObjectManager`` (see ``InitSpillEventLogger`` in
``src/ray/raylet/local_object_manager.cc``). Every line is one event; no JSON
envelope. Format:

  <unix_seconds>.<frac> key=val key=val ...

Sample lines:
  1717369123.456789012 phase=spilled object_id=ABC size=12345 trigger=ThresholdMonitor creator=TaskReturn
  1717369130.012345678 phase=deleted object_id=ABC duration_ms=42.1 size=12345 trigger=... creator=...
  1717369140.000000000 fn=TryToSpillObjects trigger=ThresholdMonitor pinned=128 pending=4

We launch one remote task per node (pinned via node-affinity scheduling
strategy) so each node parses its own file locally; this beats fetching the
file over the network for big runs.

Usage:
    python aggregate_observ_spill.py [--output path.json]
"""

import argparse
import glob
import json
import os
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

# Each line in the dedicated spill log starts with `<unix_seconds>.<frac>` then
# space-separated `key=val` pairs. We just split off the timestamp and parse
# the rest as kvs — no outer prefix anymore.
LINE_RE = re.compile(r"^\d+\.\d+\s+(.*)$")
KV_RE = re.compile(r"(\w+)=(\S+)")

DEFAULT_LOG_DIR = "/home/ray/default/benchmark_logs"


def _parse_kvs(s: str) -> Dict[str, str]:
    return dict(KV_RE.findall(s))


@ray.remote(num_cpus=0)
def _scan_node(log_glob: str) -> Dict[str, Any]:
    """Run on a worker node — parse local raylet.out and return parsed events."""
    events: List[Dict[str, Any]] = []
    paths = sorted(glob.glob(log_glob))
    for p in paths:
        try:
            with open(p, "r", errors="replace") as f:
                for line in f:
                    m = LINE_RE.search(line)
                    if not m:
                        continue
                    events.append(_parse_kvs(m.group(1)))
        except FileNotFoundError:
            continue
    return {
        "node_ip": ray.util.get_node_ip_address(),
        "files_scanned": paths,
        "num_events": len(events),
        "events": events,
    }


def _summarize(per_node: List[Dict[str, Any]]) -> Dict[str, Any]:
    spilled: Dict[tuple, Dict[str, float]] = defaultdict(
        lambda: {"count": 0, "bytes_sum": 0.0}
    )
    deleted: Dict[tuple, Dict[str, float]] = defaultdict(
        lambda: {
            "count": 0,
            "duration_ms_sum": 0.0,
            "duration_ms_max": 0.0,
            "bytes_sum": 0.0,
        }
    )

    total_events = 0
    fn_calls: Dict[str, int] = defaultdict(int)
    for n in per_node:
        for ev in n["events"]:
            total_events += 1
            # Entry-point breadcrumbs (LOM_ENTRY): `fn=SpillObjectUptoMaxThroughput ...`
            fn = ev.get("fn")
            if fn is not None:
                fn_calls[fn] += 1
                continue
            trigger = ev.get("trigger", "?")
            creator = ev.get("creator", "?")
            key = (trigger, creator)
            phase = ev.get("phase")
            if phase == "spilled":
                spilled[key]["count"] += 1
                spilled[key]["bytes_sum"] += float(ev.get("size", 0))
            elif phase == "deleted":
                d = deleted[key]
                d["count"] += 1
                d["duration_ms_sum"] += float(ev.get("duration_ms", 0))
                d["duration_ms_max"] = max(
                    d["duration_ms_max"], float(ev.get("duration_ms", 0))
                )
                d["bytes_sum"] += float(ev.get("size", 0))

    out: Dict[str, Any] = {
        "total_events": total_events,
        "num_nodes_with_events": sum(1 for n in per_node if n["num_events"] > 0),
        "num_nodes_scanned": len(per_node),
        "lom_entry_calls": dict(fn_calls),
        "by_trigger_creator": {
            "spilled": {
                f"{t}|{c}": {
                    "count": v["count"],
                    "bytes_sum": v["bytes_sum"],
                    "bytes_mean": v["bytes_sum"] / v["count"] if v["count"] else 0,
                }
                for (t, c), v in spilled.items()
            },
            "deleted": {
                f"{t}|{c}": {
                    "count": v["count"],
                    "bytes_sum": v["bytes_sum"],
                    "duration_ms_mean": v["duration_ms_sum"] / v["count"]
                    if v["count"]
                    else 0,
                    "duration_ms_max": v["duration_ms_max"],
                }
                for (t, c), v in deleted.items()
            },
        },
    }
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log-glob",
        default="/home/ray/default/raylet_spill_events/raylet_spill_events.out*",
        help="Glob (evaluated on every node) for the dedicated spill-event log files. "
        "Default matches Anyscale workspaces (persistent home dir); override with "
        "`/tmp/ray/session_latest/logs/raylet_spill_events.out*` for non-Anyscale runs.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=f"Output JSON. Defaults to <persistent>/observ_spill_<ts>.json. "
        f"Persistent dir: {DEFAULT_LOG_DIR}.",
    )
    args = parser.parse_args()

    ray.init(address="auto")

    nodes = [n for n in ray.nodes() if n.get("Alive")]
    print(f"Scanning {len(nodes)} live nodes for OBSERV_SPILL...")

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
        output_path = os.path.join(out_dir, f"observ_spill_{ts}.json")

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
