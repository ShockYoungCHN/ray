"""Daft shuffle benchmark -- mirrors bench_shuffle.py's data setup so the wall
time is directly comparable to the Ray Data in-memory/external runs.

Pipeline (same read -> limit -> hash-repartition -> write as bench_shuffle.py):
    daft.read_parquet(path).limit(target_rows)
        .repartition(num_partitions, *KEY_COLUMNS).write_parquet(OUTPUT_DIR)

Runs on the cluster's Ray via the Daft Ray runner. Setup:
    pip install getdaft
    DAFT_RUNNER=ray python bench_daft.py --data-size-gb 512 --num-partitions 500
"""

import argparse
import time

import daft

KEY_COLUMNS = ["column00"]  # l_orderkey, same as bench_shuffle.py
APPROX_BYTES_PER_ROW = 145


def pick_sf(data_size_gb: int) -> int:
    if data_size_gb <= 70:
        return 100
    if data_size_gb <= 700:
        return 1000
    return 10000


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-size-gb", type=int, required=True)
    ap.add_argument("--num-partitions", type=int, required=True)
    ap.add_argument("--output-path", default="/tmp/daft_out")
    args = ap.parse_args()

    print("daft version:", daft.__version__, flush=True)

    sf = pick_sf(args.data_size_gb)
    target_rows = int(args.data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    path = f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"

    print(
        f"[daft] read(sf{sf}, limit {target_rows:,}) + "
        f"repartition({args.num_partitions}, {KEY_COLUMNS}) + write -> {args.output_path}",
        flush=True,
    )
    start = time.perf_counter()
    (
        daft.read_parquet(path)
        .limit(target_rows)
        .repartition(args.num_partitions, *KEY_COLUMNS)
        .write_parquet(args.output_path)
    )
    elapsed = time.perf_counter() - start
    gbps = args.data_size_gb / elapsed if elapsed > 0 else 0.0
    print(
        f"RESULT engine=daft size={args.data_size_gb}GB parts={args.num_partitions} "
        f"rows={target_rows:,} wall={elapsed:.1f}s throughput={gbps:.2f}GB/s",
        flush=True,
    )


if __name__ == "__main__":
    main()
