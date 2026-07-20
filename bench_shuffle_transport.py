"""Single-machine hash-shuffle transport benchmark.

Generates a synthetic Arrow dataset in memory, then runs the same
``repartition`` (pure shuffle, no aggregation) under both variants back to
back, reporting wall time and per-variant throughput.

Usage on devbox::

    python bench_shuffle_transport.py --gb 4 --partitions 32 --block-mb 32
"""

import argparse
import os
import time

import numpy as np
import pyarrow as pa

import ray
from ray.data.context import DataContext, ShuffleStrategy


def build_dataset(total_gb: float, block_mb: int, num_int_cols: int = 4):
    row_bytes = num_int_cols * 8
    rows_per_block = (block_mb * 1024 * 1024) // row_bytes
    total_bytes = int(total_gb * 1024 * 1024 * 1024)
    num_blocks = max(1, total_bytes // (rows_per_block * row_bytes))

    def gen_block(batch):
        rng = np.random.default_rng()
        cols = {
            f"c{i}": rng.integers(0, 1 << 60, rows_per_block, dtype=np.int64)
            for i in range(num_int_cols)
        }
        return pa.table(cols)

    return ray.data.range(num_blocks).map_batches(gen_block, batch_size=1)


def bench_once(label: str, ds, num_partitions: int, use_external: bool):
    ctx = DataContext.get_current()
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = use_external

    total_bytes = ds.size_bytes() or 0
    print(
        f"\n=== {label}: use_external={use_external}, "
        f"num_partitions={num_partitions}, input={total_bytes / 1e9:.2f} GB ==="
    )

    start = time.time()
    out = ds.repartition(num_partitions, keys=["c0"]).materialize()
    elapsed = time.time() - start

    tput = total_bytes / elapsed / 1e9 if elapsed > 0 else 0.0
    print(f"[{label}] elapsed:    {elapsed:6.2f} s")
    print(f"[{label}] throughput: {tput:6.2f} GB/s")
    print(f"[{label}] output rows: {out.count()}")
    return elapsed, total_bytes, tput


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--gb", type=float, default=2.0)
    p.add_argument("--block-mb", type=int, default=32)
    p.add_argument("--partitions", type=int, default=32)
    p.add_argument("--only", choices=["external", "v2", "both"], default="both")
    p.add_argument(
        "--object-store-gb",
        type=float,
        default=None,
        help="cap the object-store size to force v2 spill (external is unaffected)",
    )
    args = p.parse_args()

    init_kwargs = {"num_cpus": os.cpu_count(), "include_dashboard": False}
    if args.object_store_gb is not None:
        init_kwargs["object_store_memory"] = int(args.object_store_gb * 1024**3)
        print(f"Capping object store to {args.object_store_gb} GB")
    ray.init(**init_kwargs)

    print(f"Building dataset: ~{args.gb} GB, blocks of {args.block_mb} MB")
    ds = build_dataset(args.gb, args.block_mb).materialize()

    results = {}
    variants = {
        "v2 (in-memory)": False,
        "external": True,
    }
    for label, use_external in variants.items():
        if args.only == "external" and not use_external:
            continue
        if args.only == "v2" and use_external:
            continue
        results[label] = bench_once(label, ds, args.partitions, use_external)

    print("\n=== Summary ===")
    for label, (elapsed, total_bytes, tput) in results.items():
        print(f"  {label:20s} {elapsed:7.2f} s  {tput:6.2f} GB/s")


if __name__ == "__main__":
    main()
