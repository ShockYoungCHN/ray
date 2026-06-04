"""Render a schematic of Ray Data's V2 hash-shuffle design, annotated with
the measured findings from the OOC spill study.

Output: shuffle_design.png
"""
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle  # noqa: E402


def box(ax, x, y, w, h, text, fc, ec="#333", fs=9, bold=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.06",
                                fc=fc, ec=ec, lw=1.3))
    ax.text(x + w / 2, y + h / 2, text, ha="center", va="center", fontsize=fs,
            fontweight="bold" if bold else "normal", zorder=5)


def arrow(ax, x1, y1, x2, y2, color="#444", lw=1.4, style="-|>", ls="-", alpha=1.0):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle=style, mutation_scale=12,
                                 color=color, lw=lw, linestyle=ls, alpha=alpha, zorder=3))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/home/ray/default/benchmark_logs/shuffle_design.png")
    args = ap.parse_args()

    fig, ax = plt.subplots(figsize=(17, 11))
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 11)
    ax.axis("off")
    ax.set_title("Ray Data V2 Hash Shuffle — design + measured behavior (500 partitions, OOC study)",
                 fontsize=13, fontweight="bold", y=0.99)

    NODE = "#eaf2fb"; MAPC = "#cfe3f7"; READC = "#dfeede"; PLAS = "#fff2cc"
    DISK = "#f4cccc"; REDC = "#f9d9c0"; WRITEC = "#e6d8f0"

    node_y = [6.1, 3.1, 0.1]            # three worker nodes (mappers)
    nh = 2.6
    for i, y in enumerate(node_y):
        # worker node container (map side)
        ax.add_patch(Rectangle((0.3, y), 6.7, nh, fc=NODE, ec="#7aa6d6", lw=1.5, zorder=1))
        ax.text(0.45, y + nh - 0.18, f"Worker node {i+1}", fontsize=8, color="#36598c",
                fontweight="bold", va="top")
        box(ax, 0.6, y + 0.85, 1.35, 0.9, "Read\n(ParquetV2)", READC, fs=8)
        box(ax, 2.5, y + 0.85, 1.5, 0.9, "HashShuffleMap\nhash→500 shards", MAPC, fs=8)
        arrow(ax, 1.95, y + 1.3, 2.5, y + 1.3)  # read->map local
        ax.text(2.22, y + 1.55, "local", fontsize=6.5, color="#2a7a2a", ha="center")
        # plasma + spill on this node
        box(ax, 4.5, y + 1.25, 2.2, 0.7, "plasma: 500 shards", PLAS, fs=7.5)
        box(ax, 4.5, y + 0.35, 2.2, 0.65, "local disk (spill)", DISK, fs=7.5)
        arrow(ax, 4.0, y + 1.3, 4.5, y + 1.55)   # map -> plasma
        arrow(ax, 5.6, y + 1.25, 5.6, y + 1.0, color="#b5651d", style="-|>")  # plasma->disk
        arrow(ax, 5.9, y + 1.0, 5.9, y + 1.25, color="#1d7db5", style="-|>", ls=(0, (2, 2)))  # restore back

    # all-to-all network band
    ax.text(7.95, 4.55, "map → reduce\nALL-TO-ALL\n(network)", ha="center", fontsize=8.5,
            color="#a11", fontweight="bold",
            bbox=dict(boxstyle="round", fc="white", ec="#a11", alpha=0.9))
    red_y = node_y
    for i, y in enumerate(red_y):
        box(ax, 9.2, y + 0.55, 3.3, 1.4,
            f"HashShuffleReduce  p{i+1}\nray.get ALL shards (batch=16)\n→ restore if spilled → reduce_fn",
            REDC, fs=7.6)
        box(ax, 13.1, y + 0.8, 1.6, 0.9, "Write\n(parquet)", WRITEC, fs=8)
        arrow(ax, 12.5, y + 1.25, 13.1, y + 1.25)
    # all-to-all arrows: every node's shards -> every reducer
    for sy in node_y:
        for ry in red_y:
            arrow(ax, 6.75, sy + 1.45, 9.2, ry + 1.25, color="#c66", lw=0.8, alpha=0.55)

    # ---- annotation callouts ----
    def note(x, y, w, h, title, body, fc):
        ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.04,rounding_size=0.05",
                                    fc=fc, ec="#888", lw=1))
        ax.text(x + 0.12, y + h - 0.12, title, fontsize=7.8, fontweight="bold", va="top")
        ax.text(x + 0.12, y + h - 0.42, body, fontsize=6.9, va="top")

    note(0.3, 9.15, 6.0, 1.55, "MAP side (produce) — NOT the bottleneck (×1.1)",
         "• read→map co-located via NodeAffinity(soft):\n  local plasma read, no network\n"
         "• map output EXCLUDED from executor 50% budget\n  → no backpressure on shards\n"
         "• spill async: ThresholdMonitor@80% (proactive)\n  + EvictionOnCreate (reactive)\n"
         "• map store_outputs stall only +35 ms/task → off hot path", "#eef7ee")

    note(6.5, 9.15, 3.0, 1.55, "object store (plasma)",
         "spill bounded by\nmin(data, 0.95×disk).\n\n2 layers (independent):\n"
         "• executor 50% =\n  backpressure\n• raylet 80% =\n  spill trigger", "#fffaee")

    note(9.7, 9.15, 6.0, 1.55, "REDUCE side (consume) — THE bottleneck (×4.8)",
         "• each reducer ray.get ALL its shards from EVERY\n  mapper (all-to-all, network)\n"
         "• synchronous, batched(16); blocks until all back\n"
         "• spilled shards RESTORED from disk on critical path\n"
         "• reduce.ray_get_s ~20×; restore = 1 obj/RPC,\n  max_io_workers=4 (write fuses, read doesn't)", "#fdeee6")

    # legend for spill arrows
    ax.text(5.75, 0.05, "↓ spill (async)   ↑ restore (sync, on reduce path)", fontsize=6.5,
            color="#555", ha="center")

    fig.tight_layout()
    fig.savefig(args.out, dpi=140, bbox_inches="tight")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
