"""Scrape per-node spill metrics from raylets at end of experiment.

Each raylet exposes Prometheus text on its `MetricsExportPort`. We fetch
that endpoint from every alive node, filter to spill-related series,
aggregate across the cluster, and return a JSON-friendly dict.

Call `collect_spill_metrics()` after Ray is initialized and the workload
is done.
"""

from collections import defaultdict
from typing import Any, Dict, List, Optional
import json
import os
import urllib.request
import glob
import re
from datetime import datetime

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


# Per-object spill/delete telemetry (size + spill→delete latency, broken
# down by Trigger/CreatorType) was previously exposed as two Prometheus
# histograms here. Those were removed in favor of the dedicated
# `raylet_spill_events.out` event log — see
# `release/nightly_tests/dataset/aggregate_observ_spill.py`. Only
# pre-existing cluster-state gauges remain in this scraper.
SPILL_METRIC_PREFIXES = (
    "ray_spill_manager_objects",
    "ray_spill_manager_objects_bytes",
    "ray_spill_manager_request_total",
    "ray_spill_manager_throughput_mb",
)

LINE_RE = re.compile(r"^\d+\.\d+\s+(.*)$")
KV_RE = re.compile(r"(\w+)=(\S+)")


def _parse_kvs(s: str) -> Dict[str, str]:
    return dict(KV_RE.findall(s))


@ray.remote(num_cpus=0)
def _fetch_node_spill_log() -> Dict[str, Any]:
    """Run on a worker node — read the fixed spill log file and return its content."""
    # Use the environment variable if set, otherwise default to the /tmp path.
    path = os.environ.get("RAY_SPILL_EVENTS_LOG_PATH", "/tmp/raylet_spill_events.out")
    
    if not os.path.exists(path):
        return {
            "node_id": ray.get_runtime_context().get_node_id(),
            "node_ip": ray.util.get_node_ip_address(),
            "content": "",
            "path": path,
            "error": "Log file not found"
        }
    
    try:
        with open(path, "r", errors="replace") as f:
            content = f.read()
    except Exception as e:
        content = ""
        error = str(e)
    else:
        error = None

    return {
        "node_id": ray.get_runtime_context().get_node_id(),
        "node_ip": ray.util.get_node_ip_address(),
        "content": content,
        "path": path,
        "error": error
    }


def _aggregate_events(per_node_events: List[Dict[str, Any]]) -> Dict[str, Any]:
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
    for n in per_node_events:
        for ev in n["events"]:
            total_events += 1
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

    return {
        "total_events": total_events,
        "lom_entry_calls": dict(fn_calls),
        "by_trigger_creator": {
            "spilled": {
                f"{t}|{c}": {
                    "count": v["count"],
                    "bytes_sum": v["bytes_sum"],
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


def _parse_prom_line(line: str):
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if "{" in line:
        name, rest = line.split("{", 1)
        label_str, value_str = rest.rsplit("}", 1)
        labels: Dict[str, str] = {}
        for pair in label_str.split(","):
            if "=" not in pair:
                continue
            k, v = pair.split("=", 1)
            labels[k.strip()] = v.strip().strip('"')
        value_str = value_str.strip().split()[0]
    else:
        parts = line.split()
        if len(parts) < 2:
            return None
        name = parts[0]
        labels = {}
        value_str = parts[1]
    try:
        return name, labels, float(value_str)
    except ValueError:
        return None


def _fetch_metrics(endpoint: str, timeout: float = 5.0) -> str:
    with urllib.request.urlopen(f"http://{endpoint}/metrics", timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def _aggregate(per_node: Dict[str, Any]) -> Dict[str, Any]:
    """Sum gauge values across nodes, grouped by label (e.g. State, Type)."""
    agg: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(
        lambda: defaultdict(lambda: {"value": 0.0})
    )
    for data in per_node.values():
        for s in data.get("samples", []):
            # All remaining spill metrics are gauges, so label by the most
            # informative tag the metric happens to use (State for objects,
            # Type for throughput/request counters).
            labels = s["labels"]
            key = labels.get("State") or labels.get("Type") or "_no_label"
            agg[s["name"]][key]["value"] += s["value"]
    return {n: {k: dict(v) for k, v in by_label.items()} for n, by_label in agg.items()}


def snapshot_spill_metrics() -> Dict[str, Any]:
    """Take a snapshot of every alive node's spill Prometheus gauges.

    Returned shape: ``{endpoint -> {"samples": [{name, labels, value}, ...]}}``
    matching the ``per_node`` payload of :func:`collect_spill_metrics`.

    Use this at benchmark start and end to compute deltas — Prometheus
    spill gauges (``throughput_mb``, ``request_total``, ``objects_bytes
    [Spilled]``) are **cluster-lifetime cumulative**, so any single
    scrape reflects everything the cluster has ever spilled, not the
    current workload.  Delta = after - before recovers per-benchmark
    activity.  Pinned/PendingSpill/PendingRestore object counts are
    instantaneous gauges and have no meaningful "delta"; they're
    carried through as the after-value.
    """
    per_node: Dict[str, Any] = {}
    nodes = [n for n in ray.nodes() if n.get("Alive")]
    for node in nodes:
        ip = node.get("NodeManagerAddress")
        port = node.get("MetricsExportPort")
        if not ip or not port:
            continue
        endpoint = f"{ip}:{port}"
        try:
            text = _fetch_metrics(endpoint)
        except Exception as e:
            per_node[endpoint] = {"error": f"{type(e).__name__}: {e}"}
            continue
        samples = []
        for line in text.splitlines():
            if not any(line.startswith(p) for p in SPILL_METRIC_PREFIXES):
                continue
            parsed = _parse_prom_line(line)
            if parsed is None:
                continue
            name, labels, value = parsed
            samples.append({"name": name, "labels": labels, "value": value})
        per_node[endpoint] = {"samples": samples}
    return per_node


# Names whose value at any instant is meaningful as a snapshot (i.e. not
# cumulative).  Everything else is treated as monotonically increasing
# from cluster start, and reported as a delta (after - before).
_INSTANTANEOUS_METRIC_NAMES = {
    "ray_spill_manager_objects",
    "ray_spill_manager_objects_bytes",
}


def _sample_key(sample: Dict[str, Any]) -> tuple:
    """Stable key (name, sorted labels) used to align samples across snapshots."""
    return (sample["name"], tuple(sorted(sample["labels"].items())))


def compute_metrics_delta(
    before: Dict[str, Any], after: Dict[str, Any]
) -> Dict[str, Any]:
    """Return per-node delta payload: cumulative metrics become
    ``after - before``; instantaneous gauges (objects[State]) pass
    through the after-value unchanged.

    Output shape matches the ``per_node`` field of
    :func:`collect_spill_metrics` so the same ``_aggregate`` reducer
    can consume it.
    """
    delta: Dict[str, Any] = {}
    for endpoint, after_data in after.items():
        if "samples" not in after_data:
            delta[endpoint] = after_data
            continue
        before_samples_by_key: Dict[tuple, float] = {}
        before_data = before.get(endpoint, {})
        for s in before_data.get("samples", []):
            before_samples_by_key[_sample_key(s)] = s["value"]
        delta_samples: List[Dict[str, Any]] = []
        for s in after_data["samples"]:
            if s["name"] in _INSTANTANEOUS_METRIC_NAMES:
                delta_value = s["value"]
            else:
                delta_value = s["value"] - before_samples_by_key.get(
                    _sample_key(s), 0.0
                )
            delta_samples.append(
                {"name": s["name"], "labels": s["labels"], "value": delta_value}
            )
        delta[endpoint] = {"samples": delta_samples}
    return delta


@ray.remote(num_cpus=0)
def _append_finalize_line(line: str) -> Dict[str, Any]:
    """Append one finalize line to this node's spill events log.

    Path resolution mirrors the C++ side (see local_object_manager.cc
    ``InitSpillEventLogger``): env var first, then the persistent
    ``/home/ray/default/raylet_spill_events/`` directory, fallback to
    ``/tmp/raylet_spill_events.out``.
    """
    candidates: List[str] = []
    env_path = os.environ.get("RAY_SPILL_EVENTS_LOG_PATH")
    if env_path:
        candidates.append(env_path)
    candidates.append("/home/ray/default/raylet_spill_events/raylet_spill_events.out")
    candidates.append("/tmp/raylet_spill_events.out")
    chosen: Optional[str] = None
    for path in candidates:
        parent = os.path.dirname(path) or "."
        try:
            os.makedirs(parent, exist_ok=True)
            chosen = path
            break
        except Exception:
            continue
    if chosen is None:
        return {"ok": False, "error": "no writable spill log path"}
    try:
        with open(chosen, "a") as f:
            f.write(line)
            if not line.endswith("\n"):
                f.write("\n")
    except Exception as e:
        return {"ok": False, "path": chosen, "error": str(e)}
    return {"ok": True, "path": chosen}


def _samples_to_flat_kvs(samples: List[Dict[str, Any]], prefix: str) -> List[str]:
    """Project a list of {name,labels,value} samples to flat ``k=v`` tokens.

    The token name is ``<prefix>.<metric_name_short>.<label_value>`` where:
    - ``metric_name_short`` strips the ``ray_spill_manager_`` prefix
    - ``label_value`` is the dominant label (State for objects, Type for
      throughput/request) or ``all`` when unlabeled

    This produces a compact line whose contents survive ``KV_RE`` parsing
    in ``aggregate_observ_spill.py`` and stay grep-friendly in raw logs.
    """
    out: List[str] = []
    for s in samples:
        name = s["name"]
        if name.startswith("ray_spill_manager_"):
            short = name[len("ray_spill_manager_") :]
        else:
            short = name
        label = (
            s["labels"].get("State")
            or s["labels"].get("Type")
            or "all"
        )
        out.append(f"{prefix}.{short}.{label}={s['value']:.3f}")
    return out


def write_benchmark_summary_to_raylet_logs(
    *,
    benchmark_tag: str,
    snapshot_before: Dict[str, Any],
    snapshot_after: Dict[str, Any],
    delta_per_node: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Fan out one finalize-line write to every alive node.

    Each node appends one newline-terminated line of the form::

        <epoch_ts> phase=benchmark_finalize tag=<tag> endpoint=<ip:port>
            delta.throughput_mb.Spilled=<float>
            after.throughput_mb.Spilled=<float>
            delta.objects_bytes.Spilled=<float>
            after.objects_bytes.Pinned=<float>
            ...

    Using flat ``k=v`` tokens means ``aggregate_observ_spill.py``'s
    existing ``KV_RE`` parser absorbs the line cleanly; we add
    ``phase=benchmark_finalize`` so the summary loop in that aggregator
    can skip these synthetic entries when bucketing per-spill events.

    Returns a list of per-node write results.
    """
    nodes = [n for n in ray.nodes() if n.get("Alive")]
    futures = []
    timestamp = datetime.now().timestamp()
    for node in nodes:
        ip = node.get("NodeManagerAddress")
        port = node.get("MetricsExportPort")
        node_id = node["NodeID"]
        if not ip or not port:
            continue
        endpoint = f"{ip}:{port}"
        after_samples = snapshot_after.get(endpoint, {}).get("samples", [])
        delta_samples = delta_per_node.get(endpoint, {}).get("samples", [])
        tokens = [
            f"{timestamp:.3f}",
            "phase=benchmark_finalize",
            f"tag={benchmark_tag}",
            f"endpoint={endpoint}",
        ]
        tokens.extend(_samples_to_flat_kvs(delta_samples, "delta"))
        tokens.extend(_samples_to_flat_kvs(after_samples, "after"))
        line = " ".join(tokens)
        sched = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
        futures.append(
            _append_finalize_line.options(scheduling_strategy=sched).remote(line)
        )
    return ray.get(futures) if futures else []


def collect_spill_metrics(
    start_ts: float = 0.0,
    output_dir: str = ".",
    snapshot_before: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Scrape metrics and collect raw spill logs from all nodes.

    If ``snapshot_before`` is provided, the returned ``aggregated`` and
    ``per_node`` fields reflect the **delta** since that snapshot (i.e.
    only what this benchmark contributed).  The raw post-benchmark
    snapshot is preserved under ``raw_after`` so Grafana / external
    dashboards that consume the cumulative Prometheus values stay
    untouched.  When ``snapshot_before`` is None, behavior is unchanged
    from the pre-delta version (per_node = raw post snapshot).
    """
    per_node: Dict[str, Any] = snapshot_spill_metrics()
    raw_after = per_node
    delta_per_node: Optional[Dict[str, Any]] = None
    if snapshot_before is not None:
        delta_per_node = compute_metrics_delta(snapshot_before, raw_after)
        per_node = delta_per_node
    nodes = [n for n in ray.nodes() if n.get("Alive")]

    # 2. Collect raw spill logs from all nodes
    log_futures = []
    for node in nodes:
        node_id = node["NodeID"]
        sched = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
        log_futures.append(_fetch_node_spill_log.options(scheduling_strategy=sched).remote())
    
    per_node_logs = ray.get(log_futures)
    
    # Save raw logs and aggregate events
    all_events = []
    num_nodes_with_content = 0
    for node_data in per_node_logs:
        node_id = node_data["node_id"]
        content = node_data["content"]
        
        if content:
            num_nodes_with_content += 1
            # Save raw log file: <output_dir>/<node_id>_spill_log.txt
            log_filename = f"{node_id}_spill_log.txt"
            log_path = os.path.join(output_dir, log_filename)
            with open(log_path, "w") as f:
                f.write(content)
            
            # Parse for aggregation
            for line in content.splitlines():
                parts = line.split(maxsplit=1)
                if len(parts) < 2:
                    continue
                try:
                    ts = float(parts[0])
                except ValueError:
                    continue
                # Keep the 10s safety buffer logic
                if ts >= start_ts - 10.0:
                    all_events.append(_parse_kvs(parts[1]))

    event_summary = _aggregate_events([{"events": all_events}])
    event_summary["num_nodes_with_events"] = num_nodes_with_content

    result = {
        "per_node": per_node,
        "aggregated": _aggregate(per_node),
        "events": {
            "summary": event_summary,
            "num_nodes_collected": len(per_node_logs),
            "num_nodes_with_content": num_nodes_with_content,
        },
    }
    if snapshot_before is not None:
        # Preserve raw cumulative values for Grafana / external dashboards
        # and so callers can show both views side-by-side.
        result["raw_after"] = raw_after
        result["raw_after_aggregated"] = _aggregate(raw_after)
        result["is_delta"] = True
    else:
        result["is_delta"] = False
    return result


def summarize(metrics: Dict[str, Any]) -> str:
    """Render a compact human-readable summary of the aggregated metrics."""
    delta_mode = metrics.get("is_delta", False)
    header = (
        "Spill metrics (THIS BENCHMARK ONLY — delta from start):"
        if delta_mode
        else "Spill metrics (aggregated across nodes):"
    )
    lines = [header]
    for base in sorted(metrics["aggregated"]):
        by_label = metrics["aggregated"][base]
        for label, stat in sorted(by_label.items()):
            tag = f"[{label}]" if label != "_no_label" else ""
            lines.append(f"  {base}{tag}  value={stat['value']:.2f}")
    if delta_mode and "raw_after_aggregated" in metrics:
        lines.append("")
        lines.append("Raw cumulative (cluster lifetime, matches Grafana):")
        for base in sorted(metrics["raw_after_aggregated"]):
            by_label = metrics["raw_after_aggregated"][base]
            for label, stat in sorted(by_label.items()):
                tag = f"[{label}]" if label != "_no_label" else ""
                lines.append(f"  {base}{tag}  value={stat['value']:.2f}")
            
    if "events" in metrics:
        s = metrics["events"]["summary"]
        lines.append("\nSpill Event Summary:")
        lines.append(f"  Total events: {s['total_events']}")
        lines.append(f"  Nodes with events: {s.get('num_nodes_with_events', 'N/A')}")
        
        lines.append("  Spilled by trigger|creator:")
        for k, v in s["by_trigger_creator"]["spilled"].items():
            lines.append(f"    {k}: {v['count']} objects, {v['bytes_sum']/1e9:.2f} GB")
            
        lines.append("  LOM entry calls:")
        for k, v in s["lom_entry_calls"].items():
            lines.append(f"    {k}: {v}")
            
    return "\n".join(lines)


def default_output_dir() -> str:
    """Pick the workspace's persistent log directory if it exists, else cwd."""
    persistent = "/home/ray/default/benchmark_logs"
    if os.path.isdir(persistent):
        return persistent
    return os.getcwd()


def dump_spill_metrics(path: str) -> Dict[str, Any]:
    """Convenience: collect + write to `path`."""
    metrics = collect_spill_metrics()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(metrics, f, indent=2)
    return metrics


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Dump per-raylet spill metrics from a running Ray cluster."
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON path. Defaults to "
        "<persistent>/spill_metrics_<timestamp>.json.",
    )
    args = parser.parse_args()

    ray.init(address="auto")

    out_path: Optional[str] = args.output
    if out_path is None:
        from datetime import datetime

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(default_output_dir(), f"spill_metrics_{ts}.json")

    m = dump_spill_metrics(out_path)
    print(summarize(m))
    print(f"\nWrote {out_path}")
