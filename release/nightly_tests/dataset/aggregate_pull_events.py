"""Scan every node's raylet_pull_events.out and aggregate bundle-pull latencies.

This file is written by the dedicated spdlog logger initialized in
``PullManager`` (see ``InitPullEventLogger`` in
``src/ray/object_manager/pull_manager.cc``).  Every line is one event; no
JSON envelope.  See ``PULL_EVENTS_LOG_FORMAT.md`` for the schema.

Receiver-side per-bundle phases (joined by ``(node_ip, req_id)``):
  - phase=bundle_locate   (T0 -> T1): subscribe to all-locations-resolved
  - phase=bundle_active   (T1 -> T2): plasma-quota wait before activation
  - phase=bundle_complete (T0 -> T3): full end-to-end fetch wall time
  - phase=bundle_terminal             at CancelPull, captures dx/reax churn
  - phase=object_first_byte           first chunk arrival, attempt counter
  - phase=plasma_create_wait          receiver-side admission wait

Sender-side per-push phases (joined by ``(node_ip, object_id, dest_node)``):
  - phase=push_queued     (T0 -> T1): wait inside PushManager for window
  - phase=push_dispatched (T1 -> T2): time to feed all chunks into network
  - phase=object_pushed   (T-1 -> T3): full sender-side push wall time
  (drain_ms = wall - queue - dispatch is computed by this script.)

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

    # Terminal stats (one bundle_terminal per bundle at CancelPull). These
    # are the only reliable signal for the population that never reaches
    # bundle_complete + the deactivate/reactivate churn that bundle_active
    # cannot see (first-activation only).
    per_task_terminal_lifetime: Dict[str, List[float]] = defaultdict(list)
    per_task_dx_count: Dict[str, List[int]] = defaultdict(list)
    per_task_dx_total_ms: Dict[str, List[float]] = defaultdict(list)
    # (task -> {"complete": int, "no_complete": int}) outcome counters.
    per_task_outcome: Dict[str, Dict[str, int]] = defaultdict(
        lambda: {"complete": 0, "no_complete": 0}
    )

    for (_node_ip, _req_id), phases in per_bundle.items():
        locate = phases.get("bundle_locate")
        active = phases.get("bundle_active")
        complete = phases.get("bundle_complete")
        terminal = phases.get("bundle_terminal")

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

        if terminal is not None:
            task = terminal.get("task", "-")
            outcome = terminal.get("outcome", "no_complete")
            per_task_outcome[task][outcome] = (
                per_task_outcome[task].get(outcome, 0) + 1
            )
            try:
                per_task_terminal_lifetime[task].append(
                    float(terminal["lifetime_ms"])
                )
                per_task_dx_count[task].append(int(terminal["dx_count"]))
                per_task_dx_total_ms[task].append(float(terminal["dx_total_ms"]))
            except (KeyError, ValueError):
                pass

    # Per-task summary block. Keep `task` keys stable so downstream dashboards
    # don't drift when task names change order.
    tasks = sorted(
        {task for (task, _warm) in per_task_locate}
        | set(per_task_active.keys())
        | set(per_task_complete_transfer.keys())
        | set(per_task_outcome.keys())
    )
    per_task_summary = {}
    for task in tasks:
        outcome = per_task_outcome.get(task, {})
        complete_n = outcome.get("complete", 0)
        no_complete_n = outcome.get("no_complete", 0)
        total_terminal = complete_n + no_complete_n
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
            # Terminal / churn stats. completion_ratio uses the
            # bundle_terminal population (only bundles that actually ended),
            # so it's not skewed by bundles that were still in flight at
            # scan time. The dx_total_ms distribution is the plasma-pressure
            # signal — see PULL_EVENTS_LOG_FORMAT.md §6 gotcha #7.
            "outcome_counts": {
                "complete": complete_n,
                "no_complete": no_complete_n,
            },
            "completion_ratio": (
                complete_n / total_terminal if total_terminal else 0.0
            ),
            "bundle_terminal_lifetime_ms": _summarize_distribution(
                per_task_terminal_lifetime.get(task, [])
            ),
            "bundle_terminal_dx_count": _summarize_distribution(
                [float(x) for x in per_task_dx_count.get(task, [])]
            ),
            "bundle_terminal_dx_total_ms": _summarize_distribution(
                per_task_dx_total_ms.get(task, [])
            ),
        }

    # Receiver-side per-object first-byte + re-pull aggregation. Each
    # object_first_byte line carries `attempt=N`: N=1 is the first ever
    # Pull for this object on this node; N>1 means we Pull-ed it again
    # (either in-session retry or eviction + re-pull cycle). For OOC
    # shuffle, the re-pull byte ratio quantifies how much of the
    # cross-node fetch traffic was wasted re-fetching evicted objects.
    first_byte_count = 0
    first_byte_ms_first_attempt: List[float] = []
    first_byte_ms_repull: List[float] = []
    repull_bytes = 0
    first_attempt_bytes = 0
    plasma_create_wait_ms: List[float] = []
    plasma_create_wait_fail_count = 0
    for n in per_node:
        for ev in n["events"]:
            phase = ev.get("phase")
            if phase == "object_first_byte":
                first_byte_count += 1
                try:
                    ms = float(ev["first_byte_ms"])
                    b = int(ev["bytes"])
                    attempt = int(ev.get("attempt", 1))
                except (KeyError, ValueError):
                    continue
                if attempt > 1:
                    first_byte_ms_repull.append(ms)
                    repull_bytes += b
                else:
                    first_byte_ms_first_attempt.append(ms)
                    first_attempt_bytes += b
            elif phase == "plasma_create_wait":
                try:
                    plasma_create_wait_ms.append(float(ev["wait_ms"]))
                    if ev.get("ok") != "1":
                        plasma_create_wait_fail_count += 1
                except (KeyError, ValueError):
                    pass

    # Sender-side per-object push timing aggregation. These are completely
    # independent from the per-bundle events above — `phase=object_pushed`
    # is emitted by the SENDER node when it finishes serving an object to
    # some requester. Splitting by `from_disk`:
    #   from_disk=1: PushFromFilesystem (spill-file -> network), the
    #                ONLY signal for cross-node spill IO (restored_* in
    #                spill_events misses this entirely).
    #   from_disk=0: PushLocalObject (plasma -> network).
    #
    # Also decomposes push_wall_ms into PushManager-internal phases:
    #   queue_ms     (phase=push_queued)     T0 -> T1: time waiting in
    #                                        push_requests_with_chunks_to_send_
    #                                        for the chunks_in_flight window.
    #   dispatch_ms  (phase=push_dispatched) T1 -> T2: time to hand every
    #                                        chunk to chunk_send_fn_.
    #   drain_ms     = push_wall_ms - queue_ms - dispatch_ms (T2 -> T3):
    #                                        wait for the last in-flight
    #                                        chunks to ack on the network.
    # Joined by (node_ip, object_id, dest_node) since the same object can
    # be pushed to multiple destinations from the same sender.
    push_disk_bytes = 0
    push_disk_ms = 0.0
    push_disk_ms_per_obj: List[float] = []
    push_plasma_bytes = 0
    push_plasma_ms = 0.0
    push_plasma_ms_per_obj: List[float] = []
    push_disk_count = 0
    push_plasma_count = 0

    # Per-(node, object, dest) row collecting the three phases for join.
    # Each push gets at most one row; if the same object is pushed twice
    # to the same destination, later phase events overwrite (we treat them
    # as the most-recent attempt — matches push_wall_ms semantics).
    push_rows: Dict[Tuple[str, str, str], Dict[str, Any]] = defaultdict(dict)
    for n in per_node:
        node_ip = n["node_ip"]
        for ev in n["events"]:
            phase = ev.get("phase")
            if phase not in ("object_pushed", "push_queued", "push_dispatched"):
                continue
            object_id = ev.get("object_id")
            dest_node = ev.get("dest_node")
            if object_id is None or dest_node is None:
                continue
            row = push_rows[(node_ip, object_id, dest_node)]
            if phase == "object_pushed":
                try:
                    row["bytes"] = int(ev["bytes"])
                    row["wall_ms"] = float(ev["push_wall_ms"])
                    row["from_disk"] = ev.get("from_disk") == "1"
                    # get_chunk_ms_total is added in newer raylet builds; old
                    # logs (or non-from_disk pushes — though we still emit it
                    # for from_disk=0, it'll just be near-zero) may lack it.
                    gc = ev.get("get_chunk_ms_total")
                    if gc is not None:
                        row["get_chunk_ms"] = float(gc)
                    bl = ev.get("bounce_lag_ms_total")
                    if bl is not None:
                        row["bounce_lag_ms"] = float(bl)
                except (KeyError, ValueError):
                    pass
            elif phase == "push_queued":
                try:
                    row["queue_ms"] = float(ev["queue_ms"])
                except (KeyError, ValueError):
                    pass
            elif phase == "push_dispatched":
                try:
                    row["dispatch_ms"] = float(ev["dispatch_ms"])
                except (KeyError, ValueError):
                    pass

    # Per-bucket distributions of queue_ms / dispatch_ms / drain_ms. We
    # only compute drain_ms when wall_ms + queue_ms + dispatch_ms are all
    # present (drain has no standalone event). Pushes missing object_pushed
    # still contribute to queue/dispatch distributions — they capture the
    # PushManager-side timing regardless of network completion.
    push_disk_queue_ms: List[float] = []
    push_disk_dispatch_ms: List[float] = []
    push_disk_drain_ms: List[float] = []
    # get_chunk_ms_total = wall time inside chunk_reader->GetChunk on sender;
    # for from_disk=1 this is the spill-file read.  network_recv_ms = drain -
    # get_chunk separates "disk read on sender" from "network + receiver".
    push_disk_get_chunk_ms: List[float] = []
    push_disk_network_recv_ms: List[float] = []
    # bounce_lag_ms_total = cumulative time across this push's chunks from
    # main_service_->post(OnChunkComplete) to the lambda actually running.
    # High value = main_service_ contention is the bottleneck inside
    # network_recv_ms.  Low value = network / receiver-side is.
    push_disk_bounce_lag_ms: List[float] = []
    push_plasma_queue_ms: List[float] = []
    push_plasma_dispatch_ms: List[float] = []
    push_plasma_drain_ms: List[float] = []
    push_plasma_get_chunk_ms: List[float] = []
    push_plasma_network_recv_ms: List[float] = []
    push_plasma_bounce_lag_ms: List[float] = []
    # Rows that have queue/dispatch but no object_pushed yet — count
    # separately so the orphan ratio is observable. Large orphan share
    # means many pushes never completed (cancelled / node lost / still
    # in flight at scan time).
    queue_orphan_count = 0
    dispatch_orphan_count = 0

    for row in push_rows.values():
        wall_ms = row.get("wall_ms")
        queue_ms = row.get("queue_ms")
        dispatch_ms = row.get("dispatch_ms")
        get_chunk_ms = row.get("get_chunk_ms")
        bounce_lag_ms = row.get("bounce_lag_ms")
        from_disk = row.get("from_disk")

        if wall_ms is not None:
            b = row.get("bytes", 0)
            if from_disk:
                push_disk_bytes += b
                push_disk_ms += wall_ms
                push_disk_ms_per_obj.append(wall_ms)
                push_disk_count += 1
                if queue_ms is not None:
                    push_disk_queue_ms.append(queue_ms)
                if dispatch_ms is not None:
                    push_disk_dispatch_ms.append(dispatch_ms)
                if queue_ms is not None and dispatch_ms is not None:
                    drain = wall_ms - queue_ms - dispatch_ms
                    push_disk_drain_ms.append(drain)
                    if get_chunk_ms is not None:
                        push_disk_get_chunk_ms.append(get_chunk_ms)
                        push_disk_network_recv_ms.append(drain - get_chunk_ms)
                    if bounce_lag_ms is not None:
                        push_disk_bounce_lag_ms.append(bounce_lag_ms)
            else:
                push_plasma_bytes += b
                push_plasma_ms += wall_ms
                push_plasma_ms_per_obj.append(wall_ms)
                push_plasma_count += 1
                if queue_ms is not None:
                    push_plasma_queue_ms.append(queue_ms)
                if dispatch_ms is not None:
                    push_plasma_dispatch_ms.append(dispatch_ms)
                if queue_ms is not None and dispatch_ms is not None:
                    drain = wall_ms - queue_ms - dispatch_ms
                    push_plasma_drain_ms.append(drain)
                    if get_chunk_ms is not None:
                        push_plasma_get_chunk_ms.append(get_chunk_ms)
                        push_plasma_network_recv_ms.append(drain - get_chunk_ms)
                    if bounce_lag_ms is not None:
                        push_plasma_bounce_lag_ms.append(bounce_lag_ms)
        else:
            if queue_ms is not None:
                queue_orphan_count += 1
            if dispatch_ms is not None:
                dispatch_orphan_count += 1

    total_bundles = len(per_bundle)
    warm_total = warm_counter["warm"]
    cold_total = warm_counter["cold"]
    complete_total = sum(o.get("complete", 0) for o in per_task_outcome.values())
    no_complete_total = sum(
        o.get("no_complete", 0) for o in per_task_outcome.values()
    )

    return {
        "total_events": total_events,
        "total_bundles": total_bundles,
        "warm_bundles": warm_total,
        "cold_bundles": cold_total,
        "warm_ratio": warm_total / max(1, warm_total + cold_total),
        "phase_counts": dict(phase_counts),
        "incomplete_bundles_total": sum(incomplete_by_task.values()),
        # Cluster-wide outcome counters from bundle_terminal. Use these
        # rather than `incomplete_bundles_total` (which is computed from
        # missing bundle_complete and can include in-flight bundles).
        "bundle_terminal_total": complete_total + no_complete_total,
        "outcome_complete": complete_total,
        "outcome_no_complete": no_complete_total,
        "completion_ratio": (
            complete_total / (complete_total + no_complete_total)
            if (complete_total + no_complete_total)
            else 0.0
        ),
        "num_nodes_with_events": sum(1 for n in per_node if n["num_events"] > 0),
        "num_nodes_scanned": len(per_node),
        "per_task": per_task_summary,
        # Cluster-wide sender-side push aggregation. The cross-node spill IO
        # path (PushFromFilesystem) is invisible to spill_events.restored_*;
        # the only way to see "how many bytes did this cluster read from
        # spill files to serve other nodes" is push_disk_bytes here.
        "object_pushed": {
            "from_disk": {
                "object_count": push_disk_count,
                "bytes_total": push_disk_bytes,
                "wall_ms_total": push_disk_ms,
                "effective_mb_per_s": (
                    (push_disk_bytes / 1e6) / (push_disk_ms / 1e3)
                    if push_disk_ms > 0
                    else 0.0
                ),
                "wall_ms_per_object": _summarize_distribution(
                    push_disk_ms_per_obj
                ),
                # PushManager-internal decomposition. queue_ms = wait for
                # chunks_in_flight window; dispatch_ms = handing all chunks
                # to chunk_send_fn_; drain_ms = wall - queue - dispatch =
                # network ack wait for the final in-flight chunks. Large
                # queue_ms + small dispatch_ms => HOL blocking from other
                # pushes; large dispatch_ms => window throttle binding;
                # large drain_ms => network / receiver-plasma bound.
                "queue_ms_per_object": _summarize_distribution(
                    push_disk_queue_ms
                ),
                "dispatch_ms_per_object": _summarize_distribution(
                    push_disk_dispatch_ms
                ),
                "drain_ms_per_object": _summarize_distribution(
                    push_disk_drain_ms
                ),
                # Further split drain_ms: get_chunk_ms (disk read on sender,
                # dominant for from_disk=1) vs network_recv_ms (everything
                # else: rpc_service slack, gRPC send, network, receiver
                # plasma admission). Only present when get_chunk_ms_total
                # was emitted; older raylet builds skip these.
                "get_chunk_ms_per_object": _summarize_distribution(
                    push_disk_get_chunk_ms
                ),
                "network_recv_ms_per_object": _summarize_distribution(
                    push_disk_network_recv_ms
                ),
                # bounce_lag_ms_total / network_recv_ms identifies main_service_
                # contention.  >= 0.5 = OnChunkComplete bounce dominates and a
                # PushManager-side lock-free path is justified.  < 0.1 = gRPC /
                # network / receiver-side is the real cost.
                "bounce_lag_ms_per_object": _summarize_distribution(
                    push_disk_bounce_lag_ms
                ),
            },
            "from_plasma": {
                "object_count": push_plasma_count,
                "bytes_total": push_plasma_bytes,
                "wall_ms_total": push_plasma_ms,
                "effective_mb_per_s": (
                    (push_plasma_bytes / 1e6) / (push_plasma_ms / 1e3)
                    if push_plasma_ms > 0
                    else 0.0
                ),
                "wall_ms_per_object": _summarize_distribution(
                    push_plasma_ms_per_obj
                ),
                "queue_ms_per_object": _summarize_distribution(
                    push_plasma_queue_ms
                ),
                "dispatch_ms_per_object": _summarize_distribution(
                    push_plasma_dispatch_ms
                ),
                "drain_ms_per_object": _summarize_distribution(
                    push_plasma_drain_ms
                ),
                "get_chunk_ms_per_object": _summarize_distribution(
                    push_plasma_get_chunk_ms
                ),
                "network_recv_ms_per_object": _summarize_distribution(
                    push_plasma_network_recv_ms
                ),
                "bounce_lag_ms_per_object": _summarize_distribution(
                    push_plasma_bounce_lag_ms
                ),
            },
            "from_disk_byte_share": (
                push_disk_bytes / (push_disk_bytes + push_plasma_bytes)
                if (push_disk_bytes + push_plasma_bytes) > 0
                else 0.0
            ),
            # Rows that had push_queued / push_dispatched but no
            # object_pushed in the scanned window. Bounded by in-flight
            # pushes at scan time + pushes that were cancelled (node death,
            # HandleNodeRemoved). A large value relative to object_count
            # indicates many pushes never completed.
            "queue_only_count": queue_orphan_count,
            "dispatch_only_count": dispatch_orphan_count,
        },
        # Receiver-side first-byte + re-pull stats. `repull_byte_share`
        # is the direct "wasted bytes from evict + re-pull" indicator;
        # >0 means objects were Pull-ed more than once on this node,
        # which for OOC shuffle typically means GET-path eviction
        # pressure forced a re-fetch.
        "object_first_byte": {
            "event_count": first_byte_count,
            "first_attempt": {
                "byte_total": first_attempt_bytes,
                "ms_distribution": _summarize_distribution(
                    first_byte_ms_first_attempt
                ),
            },
            "repull": {
                "byte_total": repull_bytes,
                "ms_distribution": _summarize_distribution(first_byte_ms_repull),
            },
            "repull_byte_share": (
                repull_bytes / (repull_bytes + first_attempt_bytes)
                if (repull_bytes + first_attempt_bytes) > 0
                else 0.0
            ),
        },
        "plasma_create_wait": {
            "event_count": len(plasma_create_wait_ms),
            "failed_count": plasma_create_wait_fail_count,
            "wait_ms_distribution": _summarize_distribution(plasma_create_wait_ms),
        },
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
