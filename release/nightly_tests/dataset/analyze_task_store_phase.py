"""Measure whether map tasks block on spill-create, from a Ray timeline.

Ray wraps each task in profile events (``python/ray/_raylet.pyx``):
  * ``task::<func_name>``   — umbrella event spanning the whole task; the ONLY
                              event that carries the function identity
                              (e.g. ``task::_shuffle_map_task``).
  * ``task:execute``        — the UDF body (what ds.stats() "Remote wall time" sees)
  * ``task:store_outputs``  — storing return values into plasma; this Put/Seal
                              BLOCKS on eviction/spill when the object store is
                              full (the EvictionOnCreate path).

The phase events carry no name, so we attribute each phase to the umbrella
``task::<func>`` event on the same (pid, tid) whose [ts, ts+dur] contains it.

If spill is on the map hot path, ``task:store_outputs`` for
``_shuffle_map_task`` balloons under spill but stays flat without it — while
``task:execute`` stays roughly constant either way.

``ray.timeline()`` dumps the WHOLE cluster session, so pass --since/--until
(unix seconds, from the run's ``BENCHMARK_START_TS``) to scope to one run.

    python analyze_task_store_phase.py timeline_512.json --since 1780597971 --until 1780598220
    python analyze_task_store_phase.py timeline_64.json  --since 1780598230 --until 1780598275
"""

import argparse
import bisect
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import json

import numpy as np

PHASES = ("task:execute", "task:store_outputs", "task:deserialize_arguments")


def attribute_phases(
    path: str, since: Optional[float], until: Optional[float]
) -> Dict[str, Dict[str, List[float]]]:
    """Return {func_name: {phase: [durations_ms]}} via (pid,tid,time) containment."""
    with open(path) as f:
        data = json.load(f)
    events = data["traceEvents"] if isinstance(data, dict) else data

    lo = since * 1e6 if since else -np.inf  # ts is epoch microseconds
    hi = until * 1e6 if until else np.inf

    # Umbrella task events per (pid,tid), sorted by start ts for bisect lookup.
    umbrellas: Dict[Tuple, List[Tuple[float, float, str]]] = defaultdict(list)
    phase_events: List[dict] = []
    for ev in events:
        if ev.get("ph") != "X" or "dur" not in ev:
            continue
        ts = ev.get("ts", 0.0)
        if ts < lo or ts > hi:
            continue
        name = ev.get("name", "")
        if name.startswith("task::"):  # umbrella
            key = (ev.get("pid"), ev.get("tid"))
            umbrellas[key].append((ts, ts + ev["dur"], name[len("task::"):]))
        elif name in PHASES:
            phase_events.append(ev)

    for key in umbrellas:
        umbrellas[key].sort()
    starts: Dict[Tuple, List[float]] = {k: [u[0] for u in v] for k, v in umbrellas.items()}

    out: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    unattributed = 0
    for ev in phase_events:
        key = (ev.get("pid"), ev.get("tid"))
        ts = ev["ts"]
        func = None
        ulist = umbrellas.get(key)
        if ulist:
            i = bisect.bisect_right(starts[key], ts) - 1
            # scan back a few in case of nested/adjacent umbrellas
            for j in range(i, max(-1, i - 3), -1):
                if j < 0:
                    break
                u_start, u_end, u_func = ulist[j]
                if u_start <= ts <= u_end:
                    func = u_func
                    break
        if func is None:
            unattributed += 1
            func = "?unattributed"
        out[func][ev["name"]].append(ev["dur"] / 1000.0)  # us -> ms
    if unattributed:
        print(f"  ({unattributed} phase events could not be attributed to a task)")
    return out


def summarize(path: str, since: Optional[float], until: Optional[float]) -> None:
    data = attribute_phases(path, since, until)
    print(f"\n===== {path}  since={since} until={until} =====")
    print(f"{'func':24} {'phase':22} {'n':>6} {'sum_s':>8} {'mean_ms':>8} {'p50':>7} {'p99':>9} {'max_ms':>9}")
    for func in sorted(data):
        for phase in PHASES:
            vals = data[func].get(phase)
            if not vals:
                continue
            a = np.array(vals)
            print(
                f"{func[:24]:24} {phase:22} {len(a):>6} {a.sum() / 1e3:>8.1f} "
                f"{a.mean():>8.2f} {np.percentile(a, 50):>7.2f} "
                f"{np.percentile(a, 99):>9.1f} {a.max():>9.1f}"
            )


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("timeline", nargs="+")
    p.add_argument("--since", type=float, default=None, help="unix seconds lower bound")
    p.add_argument("--until", type=float, default=None, help="unix seconds upper bound")
    args = p.parse_args()
    for path in args.timeline:
        summarize(path, args.since, args.until)


if __name__ == "__main__":
    main()
