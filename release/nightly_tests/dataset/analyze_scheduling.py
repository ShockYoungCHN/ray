"""Scheduling / concurrency analysis — is an operator (e.g. read) blocked?

Two complementary measurements, both reproducible from on-disk artifacts:

1. CONCURRENCY (from ray.timeline + a task-records snapshot):
   per-operator effective parallelism (sum(execute) / wall span), max/p90
   simultaneous running tasks, and the cluster-wide execute concurrency
   (vs total CPUs). If an op runs at low concurrency *while the cluster is
   saturated*, it is contention/scheduling-starved; if the cluster has idle
   headroom, it is not CPU-blocked.

2. BACKPRESSURE (from the streaming-executor progress log, e.g. sweep.log):
   an operator's running-task count, queued OUTPUT blocks, and CPU over the
   periodic progress ticks. A draining output queue => not backpressured by
   downstream; a growing/full queue => backpressured.

Clock-skew note: effective parallelism and per-op durations are single-worker
deltas (skew-immune). Cluster concurrency merges per-node ts via a sweep line;
typical <100ms NTP skew is negligible vs multi-second tasks, and only blurs
the concurrency curve slightly (it never shifts a duration).

Usage:
  # concurrency + cluster saturation
  python analyze_scheduling.py --timeline timeline_512.json \
      --tasks task_records.json --since 1780597971 --until 1780598220 \
      --total-cpus 256 --label 512_spill --out-prefix /tmp/sched512

  # backpressure (output queue) for the read op from the progress log
  python analyze_scheduling.py --progress-log sweep.log --run-size 512 \
      --op ReadFilesParquetV2 --total-cpus 256 --out-prefix /tmp/sched512
"""

import argparse
import bisect
import csv
import json
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np

OPS = ["ReadFilesParquetV2", "_shuffle_map_task", "_shuffle_reduce_task", "Write"]


def _load_intervals(tasks_path: str):
    recs = json.load(open(tasks_path))
    by_worker: Dict[str, List[Tuple[float, float, str]]] = defaultdict(list)
    for r in recs:
        w, s, e = r.get("worker_id"), r.get("start_time_ms"), r.get("end_time_ms")
        if w and s and e:
            by_worker[w].append((s * 1000.0, e * 1000.0, r["name"]))
    for w in by_worker:
        by_worker[w].sort()
    return by_worker, {w: [x[0] for x in v] for w, v in by_worker.items()}


def _concurrency(intervals: List[Tuple[float, float]], n: int = 400):
    """Sweep-line max / p90 / mean simultaneous, plus active span (s)."""
    if not intervals:
        return 0.0, 0.0, 0.0, 0.0
    lo = min(i[0] for i in intervals)
    hi = max(i[1] for i in intervals)
    st = sorted(i[0] for i in intervals)
    en = sorted(i[1] for i in intervals)
    grid = np.linspace(lo, hi, n)
    cnt = np.array([bisect.bisect_right(st, t) - bisect.bisect_right(en, t) for t in grid])
    return float(cnt.max()), float(np.percentile(cnt, 90)), float(cnt.mean()), (hi - lo) / 1e6


def analyze_concurrency(timeline: str, tasks: str, since: float, until: float,
                        total_cpus: int, label: str) -> List[dict]:
    by_worker, starts = _load_intervals(tasks)

    def lbl(tid, ts):
        w = tid.split("worker:")[-1]
        lst = by_worker.get(w)
        if not lst:
            return None
        i = bisect.bisect_right(starts[w], ts) - 1
        for j in range(i, max(-1, i - 3), -1):
            if j < 0:
                break
            s, e, nm = lst[j]
            if s <= ts <= e:
                return nm
        return None

    ev = json.load(open(timeline))
    ev = ev["traceEvents"] if isinstance(ev, dict) else ev
    lo, hi = since * 1e6, until * 1e6
    by_op: Dict[str, List[Tuple[float, float]]] = defaultdict(list)
    allexec: List[Tuple[float, float]] = []
    for e in ev:
        if e.get("ph") != "X" or e.get("name") != "task:execute":
            continue
        ts = e.get("ts", 0.0)
        if ts < lo or ts > hi:
            continue
        iv = (ts, ts + e["dur"])
        allexec.append(iv)
        nm = lbl(e.get("tid", ""), ts)
        if nm:
            by_op[nm].append(iv)

    rows = []
    cmax, c90, cmean, _ = _concurrency(allexec)
    rows.append({"run": label, "op": "CLUSTER", "n": len(allexec), "exec_sum_s": "",
                 "span_s": "", "eff_parallel": "", "conc_max": round(cmax), "conc_p90": round(c90),
                 "conc_mean": round(cmean), "total_cpus": total_cpus,
                 "note": f"saturation {cmax:.0f}/{total_cpus}"})
    for op in OPS:
        iv = by_op.get(op, [])
        if not iv:
            continue
        s = sum(b - a for a, b in iv) / 1e6
        mx, p90, mean, span = _concurrency(iv)
        rows.append({"run": label, "op": op, "n": len(iv), "exec_sum_s": round(s),
                     "span_s": round(span, 1), "eff_parallel": round(s / span) if span else 0,
                     "conc_max": round(mx), "conc_p90": round(p90), "conc_mean": round(mean),
                     "total_cpus": total_cpus, "note": ""})
    return rows


def analyze_backpressure(progress_log: str, run_size: Optional[str], op: str) -> List[dict]:
    log = open(progress_log).read()
    body = log
    if run_size:
        parts = re.split(r'==================== RUN (\d+)GB', log)
        for i in range(1, len(parts), 2):
            if parts[i] == str(run_size):
                body = parts[i + 1]
                break
    pat = re.compile(
        rf'{re.escape(op)}: [\d/]+\n.*?Tasks: (\d+);.*?Queued blocks: (\d+) \(([^)]+)\);.*?Resources: ([\d.]+) CPU',
        re.S)
    rows = []
    for k, m in enumerate(pat.finditer(body)):
        rows.append({"tick": k, "op": op, "run_tasks": int(m.group(1)),
                     "queued_blocks": int(m.group(2)), "queued_size": m.group(3),
                     "op_cpu": float(m.group(4))})
    return rows


def _dump(rows: List[dict], cols: List[str], prefix: str, suffix: str, title: str) -> None:
    if not rows:
        return
    with open(f"{prefix}_{suffix}.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    with open(f"{prefix}_{suffix}.md", "w") as f:
        f.write(f"# {title}\n\n| " + " | ".join(cols) + " |\n|" + "|".join("---" for _ in cols) + "|\n")
        for r in rows:
            f.write("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |\n")
    print(f"wrote {prefix}_{suffix}.csv / .md ({len(rows)} rows)")
    for r in rows:
        print("  " + "  ".join(f"{c}={r.get(c)}" for c in cols))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--timeline")
    p.add_argument("--tasks")
    p.add_argument("--since", type=float)
    p.add_argument("--until", type=float)
    p.add_argument("--progress-log")
    p.add_argument("--run-size", default=None)
    p.add_argument("--op", default="ReadFilesParquetV2")
    p.add_argument("--total-cpus", type=int, default=256)
    p.add_argument("--label", default="run")
    p.add_argument("--out-prefix", required=True)
    args = p.parse_args()

    if args.timeline:
        assert args.tasks and args.since and args.until, "concurrency mode needs --tasks/--since/--until"
        rows = analyze_concurrency(args.timeline, args.tasks, args.since, args.until,
                                   args.total_cpus, args.label)
        _dump(rows, ["run", "op", "n", "exec_sum_s", "span_s", "eff_parallel",
                     "conc_max", "conc_p90", "conc_mean", "total_cpus", "note"],
              args.out_prefix, "concurrency", f"Operator concurrency — {args.label}")
    if args.progress_log:
        rows = analyze_backpressure(args.progress_log, args.run_size, args.op)
        _dump(rows, ["tick", "op", "run_tasks", "queued_blocks", "queued_size", "op_cpu"],
              args.out_prefix, "backpressure", f"{args.op} output-queue / CPU over time")


if __name__ == "__main__":
    main()
