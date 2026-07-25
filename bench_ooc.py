"""Unified out-of-core shuffle benchmark: Ray Data vs Spark vs Daft.

All three engines run the SAME logical pipeline -- read TPC-H lineitem parquet,
hash-repartition by l_orderkey (``column00``), write parquet -- and print one
RESULT line, so the wall-clock is directly comparable across engines. The shared
data setup (scale factor, target size, S3 path, key, output) lives in one place;
only the per-engine execution differs.

  --engine ray    read_parquet.limit(rows).repartition(n, keys=keys).write_parquet
                  external/file-transport shuffle by default (--ray-shuffle in-memory
                  to compare against the object-store path).
  --engine daft   daft.read_parquet.limit(rows).repartition(n, *keys).write_parquet
                  runs on the cluster's Ray runner (set DAFT_RUNNER=ray).
  --engine spark  spark.read.parquet(*files).repartition(n, key).write.parquet
                  Spark's .limit(n) is a GLOBAL op that funnels everything through
                  one partition, so it is not comparable; instead we size the input
                  by selecting whole parquet files up to --data-size-gb.

The three share pick_sf / the S3 path / KEY / the RESULT format below, which is the
whole point of keeping them in one file -- the data setup can't drift between engines.

Usage:
  python bench_ooc.py --engine ray   --data-size-gb 512 --num-partitions 500
  DAFT_RUNNER=ray python bench_ooc.py --engine daft --data-size-gb 512 --num-partitions 500
  python bench_ooc.py --engine spark --data-size-gb 512 --num-partitions 500
"""

import argparse
import time

KEY_COLUMNS = ["column00"]  # l_orderkey
APPROX_BYTES_PER_ROW = 145


def pick_sf(data_size_gb: int) -> int:
    if data_size_gb <= 70:
        return 100
    if data_size_gb <= 700:
        return 1000
    return 10000


def s3_path(sf: int) -> str:
    return f"s3://ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"


def _result(engine: str, data_size_gb: int, num_partitions: int, rows, elapsed: float) -> None:
    gbps = data_size_gb / elapsed if elapsed > 0 else 0.0
    rows_str = f"{rows:,}" if rows is not None else "n/a"
    print(
        f"RESULT engine={engine} size={data_size_gb}GB parts={num_partitions} "
        f"rows={rows_str} wall={elapsed:.1f}s throughput={gbps:.2f}GB/s",
        flush=True,
    )


# --------------------------------------------------------------------------- ray
def run_ray(args) -> None:
    import ray
    from ray.data.context import DataContext, ShuffleStrategy

    sf = pick_sf(args.data_size_gb)
    target_rows = int(args.data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    path = s3_path(sf)

    ray.init(address="auto")
    ctx = DataContext.get_current()
    ctx.use_datasource_v2 = True
    ctx.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    ctx.use_external_hash_shuffle = args.ray_shuffle == "external"

    print(
        f"[ray/{args.ray_shuffle}] read(sf{sf}, limit {target_rows:,}) + "
        f"repartition({args.num_partitions}, keys={KEY_COLUMNS}) + write -> {args.output_path}",
        flush=True,
    )
    ds = ray.data.read_parquet(path).limit(target_rows)
    start = time.perf_counter()
    ds.repartition(args.num_partitions, keys=KEY_COLUMNS).write_parquet(args.output_path)
    elapsed = time.perf_counter() - start
    _result(f"ray-{args.ray_shuffle}", args.data_size_gb, args.num_partitions, target_rows, elapsed)


# -------------------------------------------------------------------------- daft
def run_daft(args) -> None:
    import daft

    sf = pick_sf(args.data_size_gb)
    target_rows = int(args.data_size_gb * 1024**3 / APPROX_BYTES_PER_ROW)
    path = s3_path(sf)
    print("daft version:", daft.__version__, flush=True)
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
    _result("daft", args.data_size_gb, args.num_partitions, target_rows, elapsed)


# ------------------------------------------------------------------------- spark
def _select_files_to_size(sf: int, data_size_gb: int):
    """Pick whole parquet files under the lineitem prefix until their combined
    size reaches --data-size-gb. Spark's .limit funnels through one partition, so
    we size the input by file selection instead (mirrors read.parquet(*selected))."""
    import pyarrow.fs as pafs

    fs = pafs.S3FileSystem()
    prefix = f"ray-benchmark-data/tpch/parquet/sf{sf}/lineitem"
    infos = fs.get_file_info(pafs.FileSelector(prefix, recursive=True))
    files = sorted(
        (i for i in infos if i.is_file and i.path.endswith(".parquet")),
        key=lambda i: i.path,
    )
    budget = data_size_gb * 1024**3
    selected, total = [], 0
    for i in files:
        # s3a:// is Spark/Hadoop's S3 scheme (needs hadoop-aws on the classpath).
        selected.append("s3a://" + i.path)
        total += i.size
        if total >= budget:
            break
    return selected, total


def run_spark(args) -> None:
    from pyspark.sql import SparkSession

    sf = pick_sf(args.data_size_gb)
    selected, total = _select_files_to_size(sf, args.data_size_gb)
    print(
        f"[spark] {len(selected)} parquet files (~{total / 1024**3:.0f} GB) + "
        f"repartition({args.num_partitions}, {KEY_COLUMNS[0]}) + write -> {args.output_path}",
        flush=True,
    )
    spark = SparkSession.builder.appName("bench_ooc_spark").getOrCreate()
    start = time.perf_counter()
    (
        spark.read.parquet(*selected)
        .repartition(args.num_partitions, KEY_COLUMNS[0])
        .write.mode("overwrite")
        .parquet(args.output_path)
    )
    elapsed = time.perf_counter() - start
    _result("spark", args.data_size_gb, args.num_partitions, None, elapsed)
    spark.stop()


ENGINES = {"ray": run_ray, "daft": run_daft, "spark": run_spark}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", choices=list(ENGINES), required=True)
    ap.add_argument("--data-size-gb", type=int, required=True)
    ap.add_argument("--num-partitions", type=int, required=True)
    ap.add_argument("--output-path", default="/tmp/ooc_out")
    ap.add_argument(
        "--ray-shuffle",
        choices=["external", "in-memory"],
        default="external",
        help="ray engine only: external=file-transport, in-memory=object-store.",
    )
    args = ap.parse_args()
    ENGINES[args.engine](args)


if __name__ == "__main__":
    main()
