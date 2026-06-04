"""Per-task-type phase breakdown from a ray.timeline() dump.

Answers "where does each task type spend its time, and does spill land on
the map produce-side (task:store_outputs) or the reduce restore-side
(task:execute)?" by attributing Ray Core profile events to task types.

Why a join is needed: ray.timeline() phase events (task:execute,
task:store_outputs, task:deserialize_arguments) carry NO task name, and the
umbrella ``task::<func>`` events are capped/evicted. So we attribute each
phase event to a task via **worker_id**: the timeline ``tid`` is
``worker:<id>`` and a task's records (from ray.util.state.list_tasks, saved
to a snapshot) give worker_id + [start,end]. The phase event belongs to the
task on the same worker whose window contains the event's ts.

Clock-skew note: every reported number is a DURATION (a single-worker
start->end delta) and the join is WITHIN one worker (one node, one clock),
so cross-node NTP skew does not affect the results — it would only affect
cross-node absolute-time ordering, which we never use. The --since/--until
window uses absolute ts but with minutes of margin vs typical <100ms skew.

Inputs:
  timeline JSON      : ray.timeline() Chrome trace
  --tasks <json>     : task-records snapshot (list of {name,worker_id,start_time_ms,end_time_ms})
  --since/--until    : unix seconds window (a run's BENCHMARK_START_TS .. +elapsed)

Outputs (next to the timeline, or --out-prefix):
  <prefix>_phase_breakdown.csv  and  .md   (per task type x phase: n/sum/mean/p50/p99/max ms)

Usage:
  python analyze_task_phases.py timeline_512.json --tasks task_records.json \
      --since 1780597971 --until 1780598220 --label 512_spill --out-prefix /tmp/tl512
"""

import argparse
import bisect
import csv
import json
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np

PHASES = ("task:deserialize_arguments", "task:execute", "task:store_outputs")


def load_intervals(tasks_path: str) -> Tuple[Dict, Dict]:
    """worker_id -> sorted [(start_us, end_us, name)], plus start-list for bisect."""
    recs = json.load(open(tasks_path))
    by_worker: Dict[str, List[Tuple[float, float, str]]] = defaultdict(list)
    for r in recs:
        w, s, e = r.get("worker_id"), r.get("start_time_ms"), r.get("end_time_ms")
        if w and s and e:
            by_worker[w].append((s * 1000.0, e * 1000.0, r["name"]))  # ms -> us
    for w in by_worker:
        by_worker[w].sort()
    starts = {w: [x[0] for x in v] for w, v in by_worker.items()}
    return by_worker, starts


def attribute(
    timeline_path: str, by_worker: Dict, starts: Dict, since: Optional[float], until: Optional[float]
) -> Dict[str, Dict[str, List[float]]]:
    ev = json.load(open(timeline_path))
    ev = ev["traceEvents"] if isinstance(ev, dict) else ev
    lo = since * 1e6 if since else -np.inf
    hi = until * 1e6 if until else np.inf
    agg: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    unattr = 0
    for e in ev:
        if e.get("ph") != "X" or e.get("name") not in PHASES:
            continue
        ts = e.get("ts", 0.0)
        if ts < lo or ts > hi:
            continue
        w = e.get("tid", "").split("worker:")[-1]
        lst = by_worker.get(w)
        name = None
        if lst:
            i = bisect.bisect_right(starts[w], ts) - 1
            for j in range(i, max(-1, i - 3), -1):
                if j < 0:
                    break
                s, en, nm = lst[j]
                if s <= ts <= en:
                    name = nm
                    break
        if name is None:
            unattr += 1
            name = "?unattributed"
        agg[name][e["name"]].append(e["dur"] / 1000.0)  # us -> ms
    if unattr:
        print(f"  ({unattr} phase events unattributed)")
    return agg


def rows(agg: Dict, label: str) -> List[dict]:
    out = []
    for task in sorted(agg):
        for phase in PHASES:
            v = agg[task].get(phase)
            if not v:
                continue
            a = np.array(v)
            out.append({
                "run": label, "task": task, "phase": phase.split(":")[1],
                "n": len(a), "sum_s": round(a.sum() / 1e3, 2),
                "mean_ms": round(a.mean(), 2), "p50_ms": round(float(np.percentile(a, 50)), 2),
                "p99_ms": round(float(np.percentile(a, 99)), 1), "max_ms": round(a.max(), 1),
            })
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("timeline")
    p.add_argument("--tasks", required=True, help="task-records snapshot json")
    p.add_argument("--since", type=float, default=None)
    p.add_argument("--until", type=float, default=None)
    p.add_argument("--label", default="run")
    p.add_argument("--out-prefix", default=None)
    args = p.parse_args()

    by_worker, starts = load_intervals(args.tasks)
    agg = attribute(args.timeline, by_worker, starts, args.since, args.until)
    data = rows(agg, args.label)

    prefix = args.out_prefix or args.timeline.rsplit(".", 1)[0]
    cols = ["run", "task", "phase", "n", "sum_s", "mean_ms", "p50_ms", "p99_ms", "max_ms"]
    with open(prefix + "_phase_breakdown.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(data)
    with open(prefix + "_phase_breakdown.md", "w") as f:
        f.write(f"# Task phase breakdown — {args.label}\n\n")
        f.write("| " + " | ".join(cols) + " |\n|" + "|".join("---" for _ in cols) + "|\n")
        for r in data:
            f.write("| " + " | ".join(str(r[c]) for c in cols) + " |\n")

    print(f"\n{args.label}:")
    print(f"{'task':22} {'phase':14} {'n':>5} {'sum_s':>7} {'mean_ms':>8} {'p50':>7} {'p99':>8} {'max':>8}")
    for r in data:
        print(f"{r['task'][:22]:22} {r['phase']:14} {r['n']:>5} {r['sum_s']:>7} {r['mean_ms']:>8} {r['p50_ms']:>7} {r['p99_ms']:>8} {r['max_ms']:>8}")
    print(f"\nwrote {prefix}_phase_breakdown.csv / .md")


if __name__ == "__main__":
    main()
