from typing import Dict, Iterable, List, Tuple

import pyarrow as pa

from ray.data._internal.arrow_ops.transform_pyarrow import hash_partition
from ray.data._internal.execution.operators.shuffle_operators._shuffle_tasks import (
    MapBlockTransformer,
    PartitionFn,
    ReduceFn,
)
from ray.data.aggregate import AggregateFn
from ray.data.block import Block

# Isolate shuffle map workers into a dedicated worker pool so that
# ReadParquet/Project tasks don't run on the same workers.  Without this,
# shared memory pages from object store accesses (mmap'd during
# combine_chunks) accumulate across task types and inflate worker RSS.
_SHUFFLE_MAP_RUNTIME_ENV = {"env_vars": {"RAY_DATA_SHUFFLE_MAP_WORKER": "1"}}


def _make_hash_partition_fn(key_columns: List[str], num_partitions: int) -> PartitionFn:
    """Return a partition function that hash-partitions by key_columns."""

    def _partition(block: pa.Table) -> Dict[int, pa.Table]:
        return hash_partition(
            block, hash_cols=key_columns, num_partitions=num_partitions
        )

    return _partition


def _concat_reduce(partition_id: int, tables: List[pa.Table]) -> Iterable[pa.Table]:
    """Concatenate all shards of a partition into a single block.

    Used under the partition = block contract: called once per partition with
    the full shard list so the output is exactly one block.
    """
    if not tables:
        return
    yield pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def _sort_reduce(key_columns: List[str]) -> ReduceFn:
    """Return a reduce function that concatenates then sorts by key_columns.

    Requires blocking mode because sorting needs all shards before emitting.
    """

    def _reduce(partition_id: int, tables: List[pa.Table]) -> Iterable[pa.Table]:
        if not tables:
            return
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        yield combined.sort_by([(k, "ascending") for k in key_columns])

    return _reduce


def _make_aggregate_pre_agg_transformer(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple[AggregateFn, ...],
) -> MapBlockTransformer:
    """Per-block partial aggregation applied inside the map task.

    Reuses the V1 transformer (column pruning, optional key sort, partial
    aggregation) so the V2 wire payload matches what HashShufflingOperatorBase
    sent — letting the reducer finalize via the same _combine_aggregated_blocks
    contract.
    """
    from ray.data._internal.execution.operators.hash_aggregate import (
        _create_aggregating_transformer,
    )

    return _create_aggregating_transformer(key_columns, aggregation_fns)


def _make_aggregate_reduce_fn(
    key_columns: Tuple[str, ...],
    aggregation_fns: Tuple[AggregateFn, ...],
) -> ReduceFn:
    """Reduce fn that finalizes partial-aggregated shards into one block.

    Each map task pre-aggregates via `_make_aggregate_pre_agg_transformer`,
    so the reducer's job is to combine those partial results and apply the
    final aggregation step.  Requires blocking mode — the full partition
    must be present to produce correct group-level aggregates.

    `_combine_aggregated_blocks` uses `heapq.merge` and assumes each input
    block is internally sorted by the key columns; a map task that processes
    multiple input blocks emits one shard per partition that is the concat
    of several internally-sorted runs (not globally sorted), so we concat
    and sort once here before finalize.  Global aggregation skips the sort
    since `_combine_aggregated_blocks` treats every row as one group.
    """
    from ray.data._internal.execution.operators.hash_aggregate import (
        ReducingAggregation,
    )
    from ray.data.block import BlockAccessor

    sort_key = ReducingAggregation._get_sort_key(key_columns)
    sort_columns = list(key_columns)

    def _reduce(partition_id: int, tables: List[pa.Table]) -> Iterable[Block]:
        if not tables:
            return
        combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
        if sort_columns:
            combined = combined.sort_by(
                [(c, "ascending") for c in sort_columns]
            )
        block_accessor = BlockAccessor.for_block(combined)
        final_block, _ = block_accessor._combine_aggregated_blocks(
            [combined],
            sort_key=sort_key,
            aggs=aggregation_fns,
            finalize=True,
        )
        yield final_block

    return _reduce
