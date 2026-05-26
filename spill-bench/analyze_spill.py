"""Analyze spill timing CSVs and produce a stacked-proportion plot.

Reads the python and cpp CSVs from the sweep, joins per object-size tag, derives
the three segments and prints a table. If matplotlib is available it also writes
a 100%-stacked area chart (x=object size, y=fraction of T_total).

Python CSV cols: wall_ts, op, backend, tag, num_objects, total_bytes, t_python_ns, t_io_ns
Cpp CSV cols:    op, tag, num_objects, total_bytes, dur_ns

Usage: python analyze_spill.py <out_dir>
"""
import csv
import os
import statistics
import sys

out_dir = sys.argv[1] if len(sys.argv) > 1 else "/tmp/spillbench/out"
py_csv = os.path.join(out_dir, "spill_py.csv")
cpp_csv = os.path.join(out_dir, "spill_cpp.csv")


def load_py(path):
    rows = []
    with open(path) as f:
        for r in csv.reader(f):
            if len(r) < 8:
                continue
            rows.append({
                "op": r[1], "backend": r[2], "tag": int(r[3]),
                "num": int(r[4]), "bytes": int(r[5]),
                "t_python_ns": int(r[6]), "t_io_ns": int(r[7]),
            })
    return rows


def load_cpp(path):
    rows = []
    with open(path) as f:
        for r in csv.reader(f):
            if len(r) < 5:
                continue
            rows.append({
                "op": r[0], "tag": int(r[1]), "num": int(r[2]),
                "bytes": int(r[3]), "dur_ns": int(r[4]),
            })
    return rows


def med(xs):
    return statistics.median(xs) if xs else 0.0


py = [r for r in load_py(py_csv) if r["op"] == "spill"]
cpp = [r for r in load_cpp(cpp_csv) if r["op"] == "spill"]

tags = sorted(set(r["tag"] for r in py) & set(r["tag"] for r in cpp))

print(
    "%-10s %8s %10s %10s %10s %12s   %6s %6s %6s"
    % ("obj_size", "batches", "T_io_us", "T_pylog_us", "T_rpcgil_us",
       "T_total_us", "io%", "py%", "rpc%")
)
print("-" * 92)

plot_x, plot_label, plot_io, plot_py, plot_rpc = [], [], [], [], []
for tag in tags:
    pr = [r for r in py if r["tag"] == tag]
    cr = [r for r in cpp if r["tag"] == tag]
    if not pr or not cr:
        continue
    t_python = med([r["t_python_ns"] for r in pr])
    t_io = med([r["t_io_ns"] for r in pr])
    t_total = med([r["dur_ns"] for r in cr])
    t_pylogic = max(0.0, t_python - t_io)
    t_rpcgil = max(0.0, t_total - t_python)
    denom = t_io + t_pylogic + t_rpcgil
    if denom <= 0:
        continue
    io_p = 100 * t_io / denom
    py_p = 100 * t_pylogic / denom
    rpc_p = 100 * t_rpcgil / denom
    # per-object size from batches
    obj_sz = med([r["bytes"] / max(1, r["num"]) for r in pr])
    label = (
        "%dKB" % (tag // 1024) if tag < 1024 * 1024 else "%dMB" % (tag // 1024 // 1024)
    )
    print(
        "%-10s %8d %10.1f %10.1f %10.1f %12.1f   %6.1f %6.1f %6.1f"
        % (label, len(cr), t_io / 1e3, t_pylogic / 1e3, t_rpcgil / 1e3,
           t_total / 1e3, io_p, py_p, rpc_p)
    )
    plot_x.append(obj_sz)
    plot_label.append(label)
    plot_io.append(io_p)
    plot_py.append(py_p)
    plot_rpc.append(rpc_p)

# Optional plot.
try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9, 5))
    # Stack order = bottom→top. Python logic on top (it is the most
    # size-independent / fixed-overhead segment).
    ax.stackplot(
        plot_x, plot_io, plot_rpc, plot_py,
        labels=["I/O (f.write)", "gRPC + GIL", "Python logic"],
        colors=["#4c78a8", "#e45756", "#f58518"], alpha=0.85,
    )
    ax.set_xscale("log", base=2)
    ax.set_xlim(min(plot_x), max(plot_x))
    ax.set_xticks(plot_x)
    ax.set_xticklabels(plot_label, rotation=45, ha="right")
    ax.get_xaxis().set_minor_locator(plt.NullLocator())
    ax.set_xlabel("object size")
    ax.set_ylabel("% of end-to-end spill time")
    ax.set_ylim(0, 100)
    ax.set_title("Spill time breakdown vs object size (filesystem backend)")
    ax.legend(loc="upper right")
    out_png = os.path.join(out_dir, "spill_breakdown.png")
    fig.tight_layout()
    fig.savefig(out_png, dpi=130)
    print("\nwrote", out_png)
except ImportError:
    print("\n(matplotlib not available; table only)")
