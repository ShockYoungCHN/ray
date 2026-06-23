"""Physical planner for the V2 ``ListFiles`` source operator.

Emits ``FileManifest`` blocks by (a) sharding user-supplied paths into
parallel listing tasks, (b) invoking the configured ``FileIndexer``, and
optionally (c) globally shuffling + size-balanced bucketing before the
downstream ``ReadFiles`` physical op consumes them.

Checkpoint filtering is not attached here — it's wrapped around the
downstream ``ReadFiles`` physical op by
:func:`plan_read_files_op_with_checkpoint_filter`, matching V1's
dispatch pattern.
"""

from __future__ import annotations

import logging
from functools import partial
from typing import TYPE_CHECKING, List, Optional

import numpy as np
import pyarrow as pa

if TYPE_CHECKING:
    from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
        FilePartitioner,
    )

import ray
from ray.data._internal.datasource_v2.listing.file_manifest import (
    PATH_COLUMN_NAME,
)
from ray.data._internal.datasource_v2.listing.listing_utils import (
    list_files_for_each_block,
    partition_files,
    shuffle_files,
)
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformer,
    MapTransformFn,
)
from ray.data._internal.logical.operators import ListFiles
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext

logger = logging.getLogger(__name__)

# Cap on the number of parallel listing tasks. In practice most reads
# pass a single directory (one task); this matters when users hand in
# thousands of explicit paths.
DEFAULT_MAX_NUM_LIST_FILES_TASKS = 200


def plan_list_files_op(
    op: ListFiles,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> MapOperator:
    assert len(physical_children) == 0

    # NOTE: Avoid capturing ``op`` inside closures — only its field values.
    file_extensions = op.file_extensions
    partition_filter = op.partition_filter
    filesystem = op.filesystem
    indexer = op.file_indexer
    partitioner = op.file_partitioner

    # Full-scan footer-fetch elision: when no Limit was pushed down, the
    # row-group-aware ParquetFileChunker's per-file footer read at listing
    # time is redundant -- every read task opens the same file and reads
    # the same footer to slice anyway. Swap to the byte-estimate chunker
    # (``reads_file_metadata = False``) so the listing pass returns
    # without any per-file I/O. The reader handles both metadata shapes.
    _maybe_swap_chunker_for_full_scan(indexer, op.pushed_limit)

    # Projection-aware sizing (v2c): when the read projects a subset of columns
    # (recorded on ``ListFiles.projected_columns`` by ``ProjectionPushdown``),
    # tell the Parquet chunker so its footer-derived in-memory size hint counts
    # only those columns. Otherwise size-balanced bucketing would over-size
    # partitions by the columns the read drops.
    _apply_projected_columns_to_chunker(indexer, op.projected_columns)

    # Limit-pushdown wiring: the indexer's lazy-enumeration path (driven by
    # ``target_rows`` below) trims the manifest stream to exactly
    # ``pushed_limit`` rows -- the boundary chunk's ``max_emit_rows`` is
    # stamped at listing time, and chunks beyond the boundary are dropped.
    # Nothing here needs to re-cap the row count; we only need to give the
    # partitioner the requested ``num_partitions`` so it balances reads
    # across CPUs instead of falling back to the file-count floor.
    if (
        op.pushed_limit is not None
        and op.pushed_limit > 0
        and op.pushed_max_rows_per_partition is not None
        and partitioner is not None
    ):
        partitioner = _rebuild_partitioner_with_row_cap(
            partitioner,
            new_max_rows_per_partition=op.pushed_max_rows_per_partition,
            num_partitions=op.pushed_num_partitions,
        )

    shuffle_config = op.shuffle_config_factory()

    # Forward the pushed limit to the indexer so it can short-circuit footer
    # enumeration via the lazy-enumeration path once running rows cover the
    # target. Only kicks in for Parquet (the only chunker stamping num_rows
    # today); other formats see ``target_rows=None`` and use the eager
    # streaming path.
    lazy_target_rows = (
        op.pushed_limit if op.pushed_limit is not None and op.pushed_limit > 0 else None
    )

    transform_fns: List[MapTransformFn] = [
        BlockMapTransformFn(
            partial(
                list_files_for_each_block,
                indexer=indexer,
                filesystem=filesystem,
                file_extensions=file_extensions,
                partition_filter=partition_filter,
                preserve_order=data_context.execution_options.preserve_order,
                target_rows=lazy_target_rows,
            ),
            # Disable block-shaping: produce manifest blocks as-is.
            disable_block_shaping=True,
        ),
    ]

    if shuffle_config is not None:
        transform_fns.append(
            BlockMapTransformFn(
                partial(
                    shuffle_files,
                    shuffle_config=shuffle_config,
                    execution_idx=data_context._execution_idx,
                ),
                disable_block_shaping=True,
            )
        )

    if partitioner is not None:
        transform_fns.append(
            BlockMapTransformFn(
                partial(partition_files, partitioner=partitioner),
                disable_block_shaping=True,
            )
        )

    map_transformer = MapTransformer(transform_fns)

    map_op = MapOperator.create(
        map_transformer,
        _create_input_data_buffer(
            op,
            data_context,
            # Shuffle needs every manifest on a single task to compute one
            # global RNG over the full listing.
            should_parallelize=shuffle_config is None,
        ),
        data_context,
        name="ListFiles",
        # Listing is extremely fast; default backpressure would starve the
        # downstream reader of inputs.
        ray_remote_args={"_generator_backpressure_num_objects": -1},
        # Don't fuse into the downstream ``ReadFiles`` — listing and reading
        # have different resource profiles.
        supports_fusion=False,
    )
    map_op.throttling_disabled = lambda: True
    return map_op


def _rebuild_partitioner_with_row_cap(
    partitioner: "FilePartitioner",
    *,
    new_max_rows_per_partition: int,
    num_partitions: Optional[int] = None,
) -> "FilePartitioner":
    """Return a partitioner with the given row cap stacked on top of its
    existing caps.

    Preserves the original byte cap and estimator -- the limit-pushdown
    path layers a row cap onto whatever the non-limit path was sized with,
    so wide-row files still flush on the byte cap before a row task
    balloons in memory. For non-Parquet chunkers (no num_rows on chunks)
    the row cap never trips and the byte cap continues to drive flushing
    as before.

    When ``num_partitions`` is provided, enables the static cross-file
    mode on :class:`FileAffinityPartitioner` -- chunks are buffered and
    redistributed across ``num_partitions`` balanced buckets at finalize,
    so the partitioner can merge chunks across files and hit the requested
    parallelism even when the file count would otherwise floor it (e.g.,
    3000-file datasets with parallelism=500). Without it, the partitioner
    stays in per-file mode and ``new_max_rows_per_partition`` acts only
    as an upper cap on file splitting.

    Falls back to partitioner unchanged for types that don't expose a
    row-count cap. RoundRobinPartitioner is not row-count-aware and is
    left as-is.
    """
    from ray.data._internal.datasource_v2.partitioners.file_affinity_partitioner import (
        FileAffinityPartitioner,
    )

    if isinstance(partitioner, FileAffinityPartitioner):
        return FileAffinityPartitioner(
            in_memory_size_estimator=partitioner._in_memory_size_estimator,
            max_bucket_size=partitioner._max_bucket_size,
            max_rows_per_partition=new_max_rows_per_partition,
            num_partitions=num_partitions,
        )
    return partitioner


def _maybe_swap_chunker_for_full_scan(indexer, pushed_limit) -> None:
    """Swap row-group-aware ParquetFileChunker -> byte-estimate chunker
    when there's no pushed limit.

    The row-group-aware chunker reads a Parquet footer per file at listing
    time to compute exact (start, end) row-group ranges and a row count.
    That cost is only worth paying when the lazy-enumeration path needs
    the row count to stop early -- i.e. when a Limit was pushed down.
    For full scans the row count is unused, every read task ends up
    opening the file and reading its own footer anyway, and the
    listing-time pre-fetch is pure duplicate I/O (~14-30s on
    sf1000-class datasets).

    No-op when (a) a limit IS pushed down (lazy enum needs num_rows),
    (b) the chunker is not the row-group-aware ParquetFileChunker
    (already byte-estimate, whole-file, line-delimited, etc.), or
    (c) the indexer doesn't expose a settable ``file_chunker``.
    """
    if pushed_limit is not None and pushed_limit > 0:
        return
    chunker = getattr(indexer, "file_chunker", None)
    from ray.data._internal.datasource_v2.chunkers.file_chunker import (
        ByteEstimateParquetFileChunker,
        ParquetFileChunker,
    )

    if not isinstance(chunker, ParquetFileChunker):
        return
    try:
        indexer.file_chunker = ByteEstimateParquetFileChunker()
    except AttributeError:
        # Indexer didn't expose a setter; leave the chunker in place.
        pass


def _apply_projected_columns_to_chunker(indexer, projected_columns) -> None:
    """Propagate the read's projected column set to the indexer's Parquet
    chunker so its footer-derived in-memory size hint counts only those columns
    (v2c projection-aware sizing). No-op for non-Parquet chunkers or when no
    projection was pushed down (``projected_columns is None``).
    """
    if projected_columns is None:
        return
    from ray.data._internal.datasource_v2.chunkers.file_chunker import (
        ParquetFileChunker,
    )

    chunker = getattr(indexer, "file_chunker", None)
    if isinstance(chunker, ParquetFileChunker):
        chunker.projected_columns = projected_columns


def _create_input_data_buffer(
    op: ListFiles,
    data_context: DataContext,
    *,
    should_parallelize: bool,
) -> InputDataBuffer:
    """Wrap ``op.paths`` into listing-input RefBundles.

    Each bundle's block is a 1-column arrow table ``{"__path": [paths...]}``
    that :func:`list_files_for_each_block` expands into manifest blocks.
    """
    if should_parallelize and op.paths:
        path_splits = np.array_split(
            list(op.paths),
            min(DEFAULT_MAX_NUM_LIST_FILES_TASKS, len(op.paths)),
        )
    else:
        path_splits = [list(op.paths)]

    input_data: List[RefBundle] = []
    for path_split in path_splits:
        paths = list(path_split)
        if not paths:
            continue
        block = pa.Table.from_pydict({PATH_COLUMN_NAME: paths})
        metadata = BlockAccessor.for_block(block).get_metadata(
            input_files=None, block_exec_stats=None
        )
        block_ref: ray.ObjectRef[Block] = ray.put(block)
        ref_bundle = RefBundle(
            (
                # pyrefly: ignore[bad-argument-type]
                BlockEntry(block_ref, metadata),
            ),
            # ``owns_blocks=False``: these are the root of the DAG and
            # must not be freed eagerly, or the DAG can't be reconstructed.
            owns_blocks=False,
            schema=BlockAccessor.for_block(block).schema(),
        )
        input_data.append(ref_bundle)

    return InputDataBuffer(data_context, input_data=input_data)
