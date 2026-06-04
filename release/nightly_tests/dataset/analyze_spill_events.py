"""Analyze spill object size distribution and timing (burstiness) from
``raylet_spill_events.out`` logs, and render plots.

Reads the per-node spill-event logs described in
``SPILL_EVENTS_LOG_FORMAT.md``.  Focuses on ``phase=spilled`` events
(one line per object: ``ts object_id size trigger creator``) and answers:

  1. Size distribution  -- how big are spilled objects, broken down by trigger.
  2. Timing / burstiness -- is spilling steady or bursty over wall-clock time.

Inputs (any of):
  * A directory containing ``*_spill_log.txt`` files (what the benchmark
    drivers write via ``collect_spill_metrics`` into the experiment dir).
  * A glob, e.g. ``'/path/*_spill_log.txt'`` or ``/tmp/raylet_spill_events.out``.
  * ``--fanout`` to read each live node's local log via a Ray task
    (mirrors ``aggregate_observ_spill.py``); requires an attached cluster.

Because the log is **append-only across raylet restarts**, by default we
isolate the most recent "session": events are split on inter-event gaps
larger than ``--gap-split`` seconds and only the last group is analyzed.
Use ``--all`` to analyze everything, or ``--since``/``--until`` (unix
seconds) to scope an explicit window.

Usage:
    python analyze_spill_events.py /home/ray/default/benchmark_logs/20260604_094111
    python analyze_spill_events.py '/tmp/raylet_spill_events.out' --all
    python analyze_spill_events.py --fanout --out-dir /tmp/spill_plots
"""

import argparse
import glob
import os
import re
from typing import Dict, List, Optional, Tuple

import numpy as np

import matplotlib

matplotlib.use("Agg")  # headless: write PNGs, never open a window
import matplotlib.pyplot as plt  # noqa: E402

# Canonical line grammar from SPILL_EVENTS_LOG_FORMAT.md section 2.
LINE_RE = re.compile(r"^(\d+\.\d+)\s+(.*)$")
KV_RE = re.compile(r"(\w+)=(\S+)")

MIB = 1024.0 * 1024.0


def parse_line(line: str) -> Optional[Dict[str, object]]:
    """Parse one log line into ``{'ts': float, <kv>...}`` or None.

    Keeps the timestamp (the in-repo aggregator drops it).  Skips lines
    that don't match the strict grammar (empty / partial-write rows).
    """
    m = LINE_RE.match(line.strip())
    if not m:
        return None
    return {"ts": float(m.group(1)), **dict(KV_RE.findall(m.group(2)))}


def load_spilled_events(paths: List[str]) -> List[Dict[str, object]]:
    """Read all files and return ``phase=spilled`` events with parsed fields."""
    events: List[Dict[str, object]] = []
    for path in paths:
        node = os.path.basename(path).split("_spill_log")[0]
        with open(path, errors="replace") as f:
            for line in f:
                ev = parse_line(line)
                if ev is None or ev.get("phase") != "spilled":
                    continue
                try:
                    ev["size"] = int(ev["size"])  # type: ignore[arg-type]
                except (KeyError, ValueError):
                    continue
                ev["node"] = node
                events.append(ev)
    events.sort(key=lambda e: e["ts"])
    return events


def fanout_collect(tmp_path: str) -> List[str]:
    """Pull each live node's local spill log via a Ray task; return temp file paths."""
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    ray.init(address="auto", logging_level="ERROR")

    @ray.remote(num_cpus=0)
    def _read(p: str) -> Tuple[str, str]:
        ip = ray.util.get_node_ip_address()
        try:
            with open(p, errors="replace") as f:
                return ip, f.read()
        except OSError:
            return ip, ""

    nodes = [n for n in ray.nodes() if n.get("Alive")]
    futs = [
        _read.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=n["NodeID"], soft=False)
        ).remote(tmp_path)
        for n in nodes
    ]
    out_dir = "/tmp/_spill_fanout"
    os.makedirs(out_dir, exist_ok=True)
    written: List[str] = []
    for ip, content in ray.get(futs):
        if not content.strip():
            continue
        fp = os.path.join(out_dir, f"{ip.replace('.', '-')}_spill_log.txt")
        with open(fp, "w") as f:
            f.write(content)
        written.append(fp)
    return written


def select_session(
    events: List[Dict[str, object]],
    gap_split: float,
    analyze_all: bool,
    since: Optional[float],
    until: Optional[float],
) -> List[Dict[str, object]]:
    """Scope events to one logical run (the log spans multiple sessions)."""
    if since is not None or until is not None:
        lo = since if since is not None else -np.inf
        hi = until if until is not None else np.inf
        return [e for e in events if lo <= e["ts"] <= hi]
    if analyze_all or len(events) < 2:
        return events
    # Split on large inter-event gaps; keep the last (most recent) group.
    ts = np.array([e["ts"] for e in events])
    gaps = np.diff(ts)
    breaks = np.where(gaps > gap_split)[0]
    start = breaks[-1] + 1 if len(breaks) else 0
    if len(breaks):
        print(
            f"NOTE: log spans {len(breaks) + 1} sessions (gaps > {gap_split}s). "
            f"Analyzing the most recent ({len(events) - start} of {len(events)} events). "
            f"Use --all to include everything."
        )
    return events[start:]


def burstiness(ts: np.ndarray, bin_s: float) -> Dict[str, float]:
    """Compute burstiness metrics from event timestamps.

    Returns Fano factor (per-bin count over-dispersion vs Poisson==1),
    inter-arrival CV (>1 bursty), the Goh-Barabasi burstiness coefficient
    B in [-1, 1] (>0 bursty, 0 Poisson, <0 regular), and peak/mean rate.
    """
    span = float(ts[-1] - ts[0]) if len(ts) > 1 else 0.0
    out: Dict[str, float] = {"span_s": span, "n": float(len(ts))}
    if len(ts) < 3 or span <= 0:
        return out
    # Fano factor on counts per fixed bin.
    nbins = max(1, int(np.ceil(span / bin_s)))
    counts, _ = np.histogram(ts, bins=nbins, range=(ts[0], ts[0] + nbins * bin_s))
    mean_c = counts.mean()
    out["fano_factor"] = float(counts.var() / mean_c) if mean_c > 0 else 0.0
    # Inter-arrival statistics.
    iat = np.diff(ts)
    mu, sigma = iat.mean(), iat.std()
    out["iat_cv"] = float(sigma / mu) if mu > 0 else 0.0
    out["burstiness_B"] = float((sigma - mu) / (sigma + mu)) if (sigma + mu) > 0 else 0.0
    out["peak_over_mean_rate"] = float(counts.max() / mean_c) if mean_c > 0 else 0.0
    return out


def verdict(b: Dict[str, float]) -> str:
    """Human verdict from burstiness metrics."""
    if b.get("n", 0) < 3:
        return "too few events to judge"
    fano = b.get("fano_factor", 0.0)
    cv = b.get("iat_cv", 0.0)
    bb = b.get("burstiness_B", 0.0)
    bursty = (fano > 2.0) or (cv > 1.0 and bb > 0.1)
    label = "BURSTY" if bursty else "fairly STEADY"
    return (
        f"{label}  (Fano={fano:.1f} [>1 over-dispersed], "
        f"IAT CV={cv:.2f} [>1 bursty], B={bb:+.2f} [>0 bursty], "
        f"peak/mean rate={b.get('peak_over_mean_rate', 0):.1f}x)"
    )


def print_size_report(events: List[Dict[str, object]]) -> None:
    sizes = np.array([e["size"] for e in events], dtype=float)
    total = sizes.sum()
    print("\n===== SIZE DISTRIBUTION (phase=spilled) =====")
    print(f"  objects spilled : {len(sizes):,}")
    print(f"  total spilled   : {total / 1e9:.2f} GB")
    print(f"  mean / median   : {sizes.mean() / MIB:.2f} / {np.median(sizes) / MIB:.2f} MiB")
    print(f"  min / max       : {sizes.min() / MIB:.2f} / {sizes.max() / MIB:.2f} MiB")
    pcts = [50, 90, 95, 99]
    pv = np.percentile(sizes, pcts)
    print("  percentiles MiB : " + ", ".join(f"p{p}={v / MIB:.1f}" for p, v in zip(pcts, pv)))
    # By trigger.
    print("  by trigger:")
    triggers = sorted({str(e.get("trigger", "?")) for e in events})
    for t in triggers:
        ts_sizes = np.array([e["size"] for e in events if e.get("trigger") == t], dtype=float)
        print(
            f"    {t:<18} {len(ts_sizes):>8,} objs  {ts_sizes.sum() / 1e9:>7.2f} GB  "
            f"mean {ts_sizes.mean() / MIB:>6.2f} MiB"
        )


def plot_size_distribution(events: List[Dict[str, object]], out_path: str) -> None:
    sizes_mib = np.array([e["size"] for e in events], dtype=float) / MIB
    triggers = sorted({str(e.get("trigger", "?")) for e in events})
    lo, hi = max(sizes_mib.min(), 1e-3), sizes_mib.max()
    bins = np.logspace(np.log10(lo), np.log10(hi + 1e-9), 40)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 4.5))
    # Stacked histogram by trigger (count).
    per_trigger = [
        np.array([e["size"] for e in events if e.get("trigger") == t], dtype=float) / MIB
        for t in triggers
    ]
    ax1.hist(per_trigger, bins=bins, stacked=True, label=triggers)
    ax1.set_xscale("log")
    ax1.set_xlabel("object size (MiB, log)")
    ax1.set_ylabel("count")
    ax1.set_title("Spilled object size distribution (by trigger)")
    ax1.legend(fontsize=8)
    # CDF by bytes (where does the spilled volume live?).
    s = np.sort(sizes_mib)
    cum_bytes = np.cumsum(s) / s.sum()
    ax2.plot(s, cum_bytes, lw=2)
    ax2.set_xscale("log")
    ax2.set_xlabel("object size (MiB, log)")
    ax2.set_ylabel("cumulative fraction of spilled BYTES")
    ax2.set_title("Where the spilled volume lives")
    ax2.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    print(f"  wrote {out_path}")


def plot_timeline(events: List[Dict[str, object]], bin_s: float, out_path: str) -> None:
    ts = np.array([e["ts"] for e in events])
    sizes = np.array([e["size"] for e in events], dtype=float)
    t0 = ts[0]
    rel = ts - t0
    span = max(rel[-1], bin_s)
    nbins = max(1, int(np.ceil(span / bin_s)))
    edges = np.linspace(0, nbins * bin_s, nbins + 1)
    bytes_per_bin, _ = np.histogram(rel, bins=edges, weights=sizes)
    count_per_bin, _ = np.histogram(rel, bins=edges)
    centers = (edges[:-1] + edges[1:]) / 2

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 7), sharex=True)
    ax1.bar(centers, bytes_per_bin / MIB / bin_s, width=bin_s * 0.95, color="tab:red", alpha=0.8)
    ax1.set_ylabel(f"spill rate (MiB/s)\n[{bin_s:g}s bins]")
    ax1.set_title("Spill timeline — is it bursty?")
    ax1.grid(True, alpha=0.3)
    ax2.bar(centers, count_per_bin / bin_s, width=bin_s * 0.95, color="tab:blue", alpha=0.8)
    ax2.set_ylabel(f"object rate (obj/s)\n[{bin_s:g}s bins]")
    ax2.set_xlabel(f"time since first spill (s)   |   t0 = {t0:.3f}")
    ax2.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    print(f"  wrote {out_path}")


def plot_interarrival(events: List[Dict[str, object]], out_path: str) -> None:
    ts = np.array([e["ts"] for e in events])
    iat = np.diff(ts)
    iat = iat[iat > 0]
    if len(iat) < 2:
        return
    fig, ax = plt.subplots(figsize=(7, 4.5))
    bins = np.logspace(np.log10(iat.min()), np.log10(iat.max()), 40)
    ax.hist(iat * 1e3, bins=bins * 1e3, color="tab:purple", alpha=0.8)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("inter-arrival time between spills (ms, log)")
    ax.set_ylabel("count (log)")
    ax.set_title("Spill inter-arrival distribution\n(heavy right tail => bursty)")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    print(f"  wrote {out_path}")


def resolve_inputs(args: argparse.Namespace) -> List[str]:
    if args.fanout:
        return fanout_collect(args.fanout_path)
    target = args.input
    if os.path.isdir(target):
        files = sorted(glob.glob(os.path.join(target, "*_spill_log.txt")))
        if not files:
            files = sorted(glob.glob(os.path.join(target, "*.txt")))
        return files
    return sorted(glob.glob(target)) or ([target] if os.path.exists(target) else [])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "input",
        nargs="?",
        default="/tmp/raylet_spill_events.out",
        help="Directory of *_spill_log.txt, a glob, or a single log file.",
    )
    p.add_argument("--fanout", action="store_true", help="Read each live node's local log via Ray.")
    p.add_argument("--fanout-path", default="/tmp/raylet_spill_events.out", help="Per-node log path for --fanout.")
    p.add_argument("--out-dir", default=None, help="Where to write PNGs (default: alongside input / cwd).")
    p.add_argument("--bin-s", type=float, default=1.0, help="Time bin width (s) for rate plots / Fano factor.")
    p.add_argument("--gap-split", type=float, default=300.0, help="Inter-event gap (s) that separates sessions.")
    p.add_argument("--all", action="store_true", help="Analyze all sessions, not just the most recent.")
    p.add_argument("--since", type=float, default=None, help="Only events with ts >= this (unix seconds).")
    p.add_argument("--until", type=float, default=None, help="Only events with ts <= this (unix seconds).")
    p.add_argument("--per-node", action="store_true", help="Also print a per-node object/byte breakdown.")
    args = p.parse_args()

    files = resolve_inputs(args)
    if not files:
        p.error(f"no spill log files found for input: {args.input!r}")
    print(f"Reading {len(files)} file(s):")
    for f in files[:5]:
        print(f"  {f}")
    if len(files) > 5:
        print(f"  ... (+{len(files) - 5} more)")

    events = load_spilled_events(files)
    if not events:
        print("No phase=spilled events found.")
        return
    events = select_session(events, args.gap_split, args.all, args.since, args.until)
    if not events:
        print("No events in the selected window.")
        return

    out_dir = args.out_dir or (args.input if os.path.isdir(args.input) else os.path.dirname(os.path.abspath(files[0])))
    os.makedirs(out_dir, exist_ok=True)

    print_size_report(events)
    if args.per_node:
        print("  by node:")
        nodes = sorted({str(e["node"]) for e in events})
        for n in nodes:
            ns = np.array([e["size"] for e in events if e["node"] == n], dtype=float)
            print(f"    {n[:16]:<16} {len(ns):>8,} objs  {ns.sum() / 1e9:>6.2f} GB")

    ts = np.array([e["ts"] for e in events])
    b = burstiness(ts, args.bin_s)
    print("\n===== TIMING / BURSTINESS =====")
    print(f"  window span     : {b['span_s']:.1f} s over {len(events):,} spills")
    if b["span_s"] > 0:
        print(f"  mean spill rate : {len(events) / b['span_s']:.1f} obj/s, "
              f"{sum(e['size'] for e in events) / 1e6 / b['span_s']:.1f} MB/s")
    print(f"  VERDICT         : {verdict(b)}")

    print("\n===== PLOTS =====")
    plot_size_distribution(events, os.path.join(out_dir, "spill_size_distribution.png"))
    plot_timeline(events, args.bin_s, os.path.join(out_dir, "spill_timeline.png"))
    plot_interarrival(events, os.path.join(out_dir, "spill_interarrival.png"))
    print(f"\nDone. Plots + report in {out_dir}")


if __name__ == "__main__":
    main()
