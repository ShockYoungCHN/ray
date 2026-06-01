"""Benchmark: TPC-H query subset for shuffle A/B testing.

Runs a curated subset of TPC-H queries (Q1 / Q3 / Q5 / Q12) against
sf-scaled TPC-H parquet files on S3, times the full pipeline end to
end (read → joins → groupby → consume), and writes per-query JSON.

The query set is chosen to span shuffle complexity:
    Q1  — single-table groupby; ~4 groups, heavy fan-in
    Q3  — 3-way join + groupby + sort/limit
    Q5  — 6-way join + small groupby (heaviest shuffle)
    Q12 — 2-way join + small groupby (medium)

Overlap with ``tpch/tpch_qN.py``
--------------------------------
The official Ray Data TPC-H suite lives in ``release/nightly_tests/
dataset/tpch/`` (21 queries: Q1–Q22 except Q16/Q19), wired into
``release_data_tests.yaml`` as individual nightly jobs at sf=100/1000.
For long-term regression tracking that suite is canonical.

This file overlaps on Q1, Q3, Q5, Q12 (same SQL semantics) but is
deliberately a single-file driver with a different focus:

  * single ``ray.init()`` runs N queries back-to-back so a shuffle
    A/B can be measured without paying setup cost per query
  * ``wait_for_object_store_to_drain`` between queries to isolate
    each measurement from the previous one's leftover spill / refs
  * forwards ``RAY_DATA_SHUFFLE_PROFILE`` into workers
    (same dance as ``benchmark_ooc_shuffle.py``) and dumps
    ``ds.stats()`` per query when set
  * one combined JSON output, easier to diff across runs

Use the ``tpch/`` suite for CI signal; use this file for local
shuffle-operator iteration.

Usage:
    python benchmark_tpch_queries.py --scale-factor 10 --queries q1,q3,q5,q12
    python benchmark_tpch_queries.py --scale-factor 100 --queries q5 \\
        --num-partitions 400
"""

import argparse
import gc
import json
import os
import time
from datetime import date, datetime
from typing import Callable, Dict, List, Optional, Tuple

import ray
from ray.data.aggregate import Count, Mean, Sum
from ray.data.context import ShuffleStrategy
from ray.data.expressions import col, lit

# Env vars that must be visible inside Ray worker processes; see
# `benchmark_ooc_shuffle.py` for why this forwarding dance is needed
# on Anyscale workspaces.
_WORKER_ENV_VARS_TO_FORWARD = ("RAY_DATA_SHUFFLE_PROFILE",)

S3_ROOT = "s3://ray-benchmark-data/tpch/parquet"

# Canonical TPC-H column names, by table, in positional order — matches
# the column00..N layout of the parquet files on S3.  `load_table`
# uses these to project + rename in one shot.
TPCH_SCHEMA: Dict[str, List[str]] = {
    "lineitem": [
        "l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax",
        "l_returnflag", "l_linestatus",
        "l_shipdate", "l_commitdate", "l_receiptdate",
        "l_shipinstruct", "l_shipmode", "l_comment",
    ],
    "orders": [
        "o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice",
        "o_orderdate", "o_orderpriority", "o_clerk",
        "o_shippriority", "o_comment",
    ],
    "customer": [
        "c_custkey", "c_name", "c_address", "c_nationkey",
        "c_phone", "c_acctbal", "c_mktsegment", "c_comment",
    ],
    "supplier": [
        "s_suppkey", "s_name", "s_address", "s_nationkey",
        "s_phone", "s_acctbal", "s_comment",
    ],
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "region": ["r_regionkey", "r_name", "r_comment"],
}


def _source_col_name(table: str, idx: int) -> str:
    """Source parquet column name for the ``idx``-th canonical TPC-H column.

    lineitem on S3 uses 2-digit zero-padded names (``column00..column16``);
    every other table uses non-padded names (``column0..columnN``).  Every
    table also has one extra trailing column beyond the TPC-H spec — we
    drop it implicitly by only ever projecting the columns we want.
    """
    if table == "lineitem":
        return f"column{idx:02d}"
    return f"column{idx}"


def load_table(
    table: str,
    scale_factor: int,
    columns: Optional[List[str]] = None,
):
    """Read a TPC-H table from S3 and project + rename in one shot.

    `columns`, if given, names the canonical TPC-H columns to keep
    (e.g. ``["l_orderkey", "l_extendedprice"]``).  Projection happens
    at parquet read time so we never drag unused columns (l_comment,
    etc.) through the shuffle.
    """
    path = f"{S3_ROOT}/sf{scale_factor}/{table}"
    schema = TPCH_SCHEMA[table]
    cols_to_load = schema if columns is None else columns
    indices = [schema.index(c) for c in cols_to_load]
    src_cols = [_source_col_name(table, i) for i in indices]
    rename_map = dict(zip(src_cols, cols_to_load))
    return ray.data.read_parquet(path, columns=src_cols).rename_columns(rename_map)


# ----------------------------------------------------------------------
# Queries
# ----------------------------------------------------------------------


def q1(sf: int, num_partitions: int):
    """Q1: Pricing Summary Report.

    Single-table groupby by ``(l_returnflag, l_linestatus)`` — only
    ~4 distinct groups, but every lineitem row reduces into them.
    Tests the hash-shuffle path with extreme fan-in per partition.
    """
    li = load_table(
        "lineitem", sf,
        columns=[
            "l_quantity", "l_extendedprice", "l_discount", "l_tax",
            "l_returnflag", "l_linestatus", "l_shipdate",
        ],
    )
    li = li.filter(expr=col("l_shipdate") <= lit(date(1998, 9, 2)))
    li = li.add_column(
        "disc_price",
        lambda df: df["l_extendedprice"] * (1 - df["l_discount"]),
    )
    li = li.add_column(
        "charge",
        lambda df: df["l_extendedprice"]
        * (1 - df["l_discount"])
        * (1 + df["l_tax"]),
    )
    return (
        li.groupby(
            ["l_returnflag", "l_linestatus"], num_partitions=num_partitions,
        )
        .aggregate(
            Sum("l_quantity"),
            Sum("l_extendedprice"),
            Sum("disc_price"),
            Sum("charge"),
            Mean("l_quantity"),
            Mean("l_extendedprice"),
            Mean("l_discount"),
            Count(),
        )
    )


def q3(sf: int, num_partitions: int):
    """Q3: Shipping Priority.

    customer ⋈ orders ⋈ lineitem, then groupby
    ``(l_orderkey, o_orderdate, o_shippriority)``.  Three sequential
    shuffles for the joins + one for the groupby.
    """
    customer = load_table(
        "customer", sf, columns=["c_custkey", "c_mktsegment"],
    )
    customer = customer.filter(expr=col("c_mktsegment") == lit("BUILDING"))

    orders = load_table(
        "orders", sf,
        columns=["o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"],
    )
    orders = orders.filter(expr=col("o_orderdate") < lit(date(1995, 3, 15)))

    lineitem = load_table(
        "lineitem", sf,
        columns=["l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"],
    )
    lineitem = lineitem.filter(expr=col("l_shipdate") > lit(date(1995, 3, 15)))
    lineitem = lineitem.add_column(
        "revenue",
        lambda df: df["l_extendedprice"] * (1 - df["l_discount"]),
    )

    co = customer.join(
        orders, join_type="inner", num_partitions=num_partitions,
        on=("c_custkey",), right_on=("o_custkey",),
    )
    col_lo = co.join(
        lineitem, join_type="inner", num_partitions=num_partitions,
        on=("o_orderkey",), right_on=("l_orderkey",),
    )
    # NOTE: the join collapses ``o_orderkey``/``l_orderkey`` into the left
    # name only; group by ``o_orderkey`` even though TPC-H spec writes it
    # as ``l_orderkey`` — they're equal post-join.
    return (
        col_lo.groupby(
            ["o_orderkey", "o_orderdate", "o_shippriority"],
            num_partitions=num_partitions,
        )
        .aggregate(Sum("revenue"))
    )


def q5(sf: int, num_partitions: int):
    """Q5: Local Supplier Volume.

    region ⋈ nation ⋈ supplier ⋈ customer ⋈ orders ⋈ lineitem.
    The heaviest shuffle workload in this subset; small result
    (5 rows in the reference answer) but every join shuffles a
    large fact table.
    """
    region = load_table("region", sf, columns=["r_regionkey", "r_name"])
    region = region.filter(expr=col("r_name") == lit("ASIA"))

    nation = load_table(
        "nation", sf, columns=["n_nationkey", "n_name", "n_regionkey"],
    )
    supplier = load_table("supplier", sf, columns=["s_suppkey", "s_nationkey"])
    customer = load_table("customer", sf, columns=["c_custkey", "c_nationkey"])
    orders = load_table(
        "orders", sf, columns=["o_orderkey", "o_custkey", "o_orderdate"],
    )
    orders = orders.filter(
        expr=(col("o_orderdate") >= lit(date(1994, 1, 1)))
        & (col("o_orderdate") < lit(date(1995, 1, 1)))
    )
    lineitem = load_table(
        "lineitem", sf,
        columns=["l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"],
    )
    lineitem = lineitem.add_column(
        "revenue",
        lambda df: df["l_extendedprice"] * (1 - df["l_discount"]),
    )

    # Build dim chain first so the big fact joins see smaller right sides.
    rn = region.join(
        nation, join_type="inner", num_partitions=num_partitions,
        on=("r_regionkey",), right_on=("n_regionkey",),
    )
    rns = rn.join(
        supplier, join_type="inner", num_partitions=num_partitions,
        on=("n_nationkey",), right_on=("s_nationkey",),
    )
    rnsc = rns.join(
        customer, join_type="inner", num_partitions=num_partitions,
        on=("n_nationkey",), right_on=("c_nationkey",),
    )
    rnsco = rnsc.join(
        orders, join_type="inner", num_partitions=num_partitions,
        on=("c_custkey",), right_on=("o_custkey",),
    )
    rnscol = rnsco.join(
        lineitem, join_type="inner", num_partitions=num_partitions,
        on=("o_orderkey",), right_on=("l_orderkey",),
    )
    # TPC-H Q5 also requires l_suppkey = s_suppkey (customer and supplier
    # share a nation AND the specific lineitem was supplied by that
    # supplier).  The shuffle chain above only enforces the nation match,
    # so apply the suppkey predicate as a streaming filter post-join.
    rnscol = rnscol.filter(expr=col("l_suppkey") == col("s_suppkey"))
    return (
        rnscol.groupby(["n_name"], num_partitions=num_partitions)
        .aggregate(Sum("revenue"))
    )


def q12(sf: int, num_partitions: int):
    """Q12: Shipping Modes and Order Priority.

    orders ⋈ lineitem on ``o_orderkey = l_orderkey``, then groupby
    ``l_shipmode``.  Single large join + small groupby — a middle
    point between Q1 (no join) and Q5 (six joins).
    """
    orders = load_table(
        "orders", sf, columns=["o_orderkey", "o_orderpriority"],
    )
    lineitem = load_table(
        "lineitem", sf,
        columns=[
            "l_orderkey", "l_shipmode",
            "l_commitdate", "l_receiptdate", "l_shipdate",
        ],
    )
    lineitem = lineitem.filter(
        expr=col("l_shipmode").is_in(["MAIL", "SHIP"])
        & (col("l_commitdate") < col("l_receiptdate"))
        & (col("l_shipdate") < col("l_commitdate"))
        & (col("l_receiptdate") >= lit(date(1994, 1, 1)))
        & (col("l_receiptdate") < lit(date(1995, 1, 1)))
    )

    ol = orders.join(
        lineitem, join_type="inner", num_partitions=num_partitions,
        on=("o_orderkey",), right_on=("l_orderkey",),
    )
    # high_line / low_line based on priority bucket.
    ol = ol.add_column(
        "high_line",
        lambda df: ((df["o_orderpriority"] == "1-URGENT")
                    | (df["o_orderpriority"] == "2-HIGH")).astype("int64"),
    )
    ol = ol.add_column(
        "low_line",
        lambda df: ((df["o_orderpriority"] != "1-URGENT")
                    & (df["o_orderpriority"] != "2-HIGH")).astype("int64"),
    )
    return (
        ol.groupby(["l_shipmode"], num_partitions=num_partitions)
        .aggregate(Sum("high_line"), Sum("low_line"))
    )


QUERIES: Dict[str, Callable] = {
    "q1": q1,
    "q3": q3,
    "q5": q5,
    "q12": q12,
}


# ----------------------------------------------------------------------
# Harness
# ----------------------------------------------------------------------


def materialize(ds) -> Tuple[int, int]:
    """Force execution by iterating; returns (num_rows, num_batches).

    Used in place of `take_all` because Q3/Q5 results can be wide
    enough that pulling everything to the driver is wasteful — we
    just want to time the shuffle, not the driver-side collection.
    """
    rows, batches = 0, 0
    for batch in ds.iter_batches(batch_size=4096, batch_format="pyarrow"):
        rows += batch.num_rows
        batches += 1
    return rows, batches


def wait_for_object_store_to_drain(threshold_pct=20, timeout_s=180, poll_s=5):
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        mem = ray.cluster_resources().get("object_store_memory", 1)
        avail = ray.available_resources().get("object_store_memory", 0)
        used_pct = (1 - avail / mem) * 100 if mem > 0 else 0
        if used_pct < threshold_pct:
            return
        print(
            f"    waiting for object store to drain ({used_pct:.0f}% used)...",
            flush=True,
        )
        time.sleep(poll_s)
    print(f"    object store drain timed out after {timeout_s}s", flush=True)


def run_one(name: str, sf: int, num_partitions: int) -> Dict:
    qfn = QUERIES[name]
    print(f"  [{name}] building plan ... ", end="", flush=True)
    ds = qfn(sf, num_partitions)
    ds.context.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE

    start = time.perf_counter()
    rows, batches = materialize(ds)
    elapsed = time.perf_counter() - start
    print(f"{elapsed:.1f}s ({rows:,} rows, {batches} batches)")

    if os.environ.get("RAY_DATA_SHUFFLE_PROFILE") == "1":
        print(f"\n========== {name} ds.stats() ==========")
        print(ds.stats())
        print("=" * (24 + len(name)))
        print()

    info = {
        "query": name,
        "elapsed_s": round(elapsed, 3),
        "num_rows": rows,
        "num_batches": batches,
        "status": "ok",
    }

    del ds
    gc.collect()
    wait_for_object_store_to_drain()
    return info


def main():
    parser = argparse.ArgumentParser(description="TPC-H query benchmark")
    parser.add_argument(
        "--output", type=str, default="benchmark_tpch_results.json",
    )
    parser.add_argument(
        "--scale-factor", type=int, required=True,
        help="TPC-H scale factor (must match a sf{N} directory on S3).",
    )
    parser.add_argument(
        "--num-partitions", type=int, default=200,
        help="Shuffle partitions for joins and groupbys.",
    )
    parser.add_argument(
        "--queries", type=str, default="q1,q3,q5,q12",
        help="Comma-separated subset of queries to run.",
    )
    args = parser.parse_args()

    selected = [q.strip() for q in args.queries.split(",") if q.strip()]
    unknown = [q for q in selected if q not in QUERIES]
    if unknown:
        raise SystemExit(
            f"Unknown queries: {unknown}. Available: {sorted(QUERIES)}"
        )

    forwarded = {
        k: os.environ[k] for k in _WORKER_ENV_VARS_TO_FORWARD
        if k in os.environ
    }
    runtime_env = {"env_vars": forwarded} if forwarded else None
    if forwarded:
        print(f"Forwarding env vars to workers: {forwarded}")
    ray.init(runtime_env=runtime_env)

    cluster = ray.cluster_resources()
    total_cpu = cluster.get("CPU", 0)
    total_mem_gb = cluster.get("memory", 0) / 1e9
    total_obj_gb = cluster.get("object_store_memory", 0) / 1e9

    print(
        f"Cluster: {total_cpu:.0f} CPUs, {total_mem_gb:.0f} GB memory, "
        f"{total_obj_gb:.0f} GB object store"
    )
    print(
        f"Config: sf={args.scale_factor}, num_partitions={args.num_partitions},"
        f" queries={selected}"
    )
    print()

    results: List[Dict] = []
    for name in selected:
        print(f"--- {name} (sf{args.scale_factor}) ---")
        try:
            info = run_one(name, args.scale_factor, args.num_partitions)
        except Exception as e:
            print(f"  [{name}] FAILED: {type(e).__name__}: {e}", flush=True)
            info = {
                "query": name,
                "status": "error",
                "error_type": type(e).__name__,
                "error_message": str(e),
            }
        results.append(info)
        print()

    out = {
        "timestamp": datetime.now().isoformat(),
        "cluster": {
            "num_cpus": total_cpu,
            "total_memory_gb": round(total_mem_gb, 1),
            "object_store_gb": round(total_obj_gb, 1),
        },
        "config": {
            "scale_factor": args.scale_factor,
            "num_partitions": args.num_partitions,
            "queries": selected,
        },
        "results": results,
    }
    with open(args.output, "w") as f:
        json.dump(out, f, indent=2)

    print(f"Results written to {args.output}")
    print()
    print("Summary:")
    for r in results:
        if r["status"] == "ok":
            print(
                f"  {r['query']:>4}: {r['elapsed_s']:>7.1f}s  "
                f"({r['num_rows']:>12,} rows)"
            )
        else:
            print(f"  {r['query']:>4}: FAILED ({r['error_type']})")


if __name__ == "__main__":
    main()
