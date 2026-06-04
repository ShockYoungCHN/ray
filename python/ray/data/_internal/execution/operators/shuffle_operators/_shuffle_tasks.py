"""Shared remote tasks + helpers for ShuffleMapOp / ShuffleReduceOp."""

import os
import pickle
import time
from contextlib import contextmanager
from typing import Callable, Dict, Generator, Iterable, List, Optional, Tuple, Union

import pyarrow as pa

import ray
from ray import ObjectRef
from ray._raylet import StreamingGeneratorStats
from ray.data._internal.output_buffer import BlockOutputBuffer, OutputBlockSizeOption
from ray.data._internal.table_block import TableBlockAccessor
from ray.data.block import (
    Block,
    BlockAccessor,
    BlockExecStats,
    BlockMetadataWithSchema,
    BlockType,
    TaskExecWorkerStats,
)

# Maps rows of a pa.Table block to output partition IDs.  num_partitions
# and any other config are bound via closure.  The operator filters out empty
# entries, so implementations don't need to.
PartitionFn = Callable[[pa.Table], Dict[int, pa.Table]]

# Takes (partition_id, accumulated_shard_tables) and yields output blocks.
# See ShuffleReduceOp for streaming vs. blocking call semantics.
ReduceFn = Callable[[int, List[pa.Table]], Iterable[Block]]

# Multi-input variant for joins / N-way combiners.  Receives a dict mapping
# input_seq_index -> list of decoded shard tables.  Always blocking: the
# reducer task collects every shard from every side before invoking the fn.
MultiSeqReduceFn = Callable[[int, Dict[int, List[pa.Table]]], Iterable[Block]]

# Optional per-block transform applied inside the map task before partitioning
# (e.g. partial pre-aggregation, projection pushdown).  Must preserve the
# semantics expected by the paired reduce_fn — typically reducing the block's
# byte footprint without changing the row set in ways the reducer can't merge.
MapBlockTransformer = Callable[[Block], Block]


# Number of ObjectRefs fetched per ray.get() call in reducers.
_REDUCE_BATCH_SIZE = 16


# Env-var gated stage-level profiling.  Read once at import; the worker imports
# this module on first task dispatch so the env value at that moment is what
# applies for the rest of the worker's lifetime.
SHUFFLE_PROFILE_ENV_VAR = "RAY_DATA_SHUFFLE_PROFILE"
_SHUFFLE_PROFILE_ENABLED = os.environ.get(SHUFFLE_PROFILE_ENV_VAR) == "1"


class _StageTimings:
    """Accumulator for stage-level wall-clock timings and byte counters.

    When shuffle profiling is disabled, ``record``/``add`` are no-ops and
    ``as_dict`` returns None, so callers can unconditionally call them
    without paying any cost on the hot path.
    """

    __slots__ = ("_timings", "_enabled")

    def __init__(self):
        self._enabled = _SHUFFLE_PROFILE_ENABLED
        self._timings: Optional[Dict[str, float]] = {} if self._enabled else None

    @contextmanager
    def record(self, key: str):
        if not self._enabled:
            yield
            return
        t0 = time.perf_counter()
        try:
            yield
        finally:
            self._timings[key] = self._timings.get(key, 0.0) + (
                time.perf_counter() - t0
            )

    def add(self, key: str, value: float) -> None:
        if not self._enabled:
            return
        self._timings[key] = self._timings.get(key, 0.0) + value

    def as_dict(self) -> Optional[Dict[str, float]]:
        return self._timings


# ---------------------------------------------------------------------------
# Map-side helpers
# ---------------------------------------------------------------------------


def _partition_blocks_to_shards(
    blocks: Tuple[Block, ...],
    partition_fn: PartitionFn,
    timings: Optional["_StageTimings"] = None,
    input_block_transformer: Optional["MapBlockTransformer"] = None,
) -> Dict[int, List[pa.Table]]:
    """Run partition_fn on each block; collect non-empty shards by pid.

    Each block is partitioned independently so we never materialize a single
    concatenated table across all inputs.  Chunked columns are defragmented
    here (combine_chunks) before calling partition_fn — this is a real
    performance constraint, not optional: hash_partition's per-column take
    is sensitive to chunk count, and leaving chunked input alone halves map
    throughput.

    If ``input_block_transformer`` is provided, it runs on each input block
    before chunk defragmentation and partitioning — used by hash-aggregate
    for partial pre-aggregation that shrinks the shuffle payload.
    """
    if timings is None:
        timings = _StageTimings()
    partition_accumulators: Dict[int, List[pa.Table]] = {}
    for block in blocks:
        block = TableBlockAccessor.try_convert_block_type(
            block, block_type=BlockType.ARROW
        )
        if block.num_rows == 0:
            continue
        if input_block_transformer is not None:
            with timings.record("map.input_transform_s"):
                block = input_block_transformer(block)
                block = TableBlockAccessor.try_convert_block_type(
                    block, block_type=BlockType.ARROW
                )
            if block.num_rows == 0:
                continue
        assert isinstance(block, pa.Table), f"Expected pa.Table, got {type(block)}"
        if any(col.num_chunks > 1 for col in block.columns):
            with timings.record("map.combine_chunks_s"):
                block = block.combine_chunks()
        with timings.record("map.partition_fn_s"):
            block_partitions = partition_fn(block)
        for pid, shard in block_partitions.items():
            if shard.num_rows > 0:
                partition_accumulators.setdefault(pid, []).append(shard)
        del block, block_partitions
    return partition_accumulators


def _encode_partition_ipc(
    table: pa.Table,
    ipc_write_options: pa.ipc.IpcWriteOptions,
) -> pa.Buffer:
    """ZSTD-encode one partition's shard as a single Arrow IPC stream.

    ``combine_chunks`` usually collapses a Table to one batch, but the
    Arrow API doesn't strictly guarantee that — e.g. tables hitting the
    2 GB-per-batch offset limit may come out with multiple batches.  We
    write every batch so the reader can re-merge correctly.
    """
    if table.num_columns > 0:
        table = table.combine_chunks()
    batches = table.to_batches()
    if not batches:
        return pa.py_buffer(b"")

    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, batches[0].schema, options=ipc_write_options)
    for batch in batches:
        writer.write_batch(batch)
    writer.close()
    return sink.getvalue()


@ray.remote
def _shuffle_map_task(
    *blocks: Block,
    partition_fn: PartitionFn,
    num_partitions: int,
    input_block_transformer: Optional["MapBlockTransformer"] = None,
    per_partition_post_transformer: Optional["MapBlockTransformer"] = None,
):
    """Map stage: partition input blocks and return one shard per partition.

    Multiple input blocks may be passed when pre-map merge is enabled; they
    are partitioned individually (to avoid a large combine_chunks on the
    merged table) and their shards are accumulated per pid before encoding.

    Must be called with ``num_returns = num_partitions + 1``.

    Args:
        *blocks: One or more input blocks.
        partition_fn: Callable (pa.Table) -> Dict[int, pa.Table] that
            assigns rows to output partitions.
        num_partitions: Total number of output partitions.
        input_block_transformer: Optional per-block transformer applied
            before partitioning (e.g. partial pre-aggregation for
            HashAggregate, per-block dedup for SEMI/ANTI joins).
        per_partition_post_transformer: Optional transformer applied
            once per (task, partition) **after** the per-pid concat,
            right before IPC encoding.  Used by SEMI/ANTI joins to do
            a per-task cluster-incremental dedup on the merged
            partition shard — collapses cross-block duplication that
            the per-block ``input_block_transformer`` can't catch.
            Must be idempotent (called even when shards already
            unique).

    Returns:
        Tuple of ``num_partitions + 1`` values:

        - Index 0: (BlockMetadata, Dict) — aggregate input metadata and a
          per-partition (rows, bytes) size dict (keys are the partitions
          that received any rows from this mapper).
        - Index 1 … N: a ZSTD-compressed Arrow IPC stream for that
          partition, or ``None`` if the partition received no rows.
    """
    from dataclasses import replace as _dc_replace

    stats = BlockExecStats.builder()
    timings = _StageTimings()

    if not blocks:
        empty_meta = BlockAccessor.for_block(pa.table({})).get_metadata(
            block_exec_stats=stats.build(
                block_ser_time_s=0,
                shuffle_stage_timings_s=timings.as_dict(),
            )
        )
        return (empty_meta, {}), *([None] * num_partitions)

    # Use BlockAccessor so we work for non-Arrow block types (pandas, numpy)
    # whose num_rows / nbytes attributes aren't named the same.
    accessors = [BlockAccessor.for_block(b) for b in blocks]
    total_rows = sum(a.num_rows() for a in accessors)
    total_bytes = sum((a.size_bytes() or 0) for a in accessors)

    # Empty-input fast path.
    if total_rows == 0:
        input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
            block_exec_stats=stats.build(
                block_ser_time_s=0,
                shuffle_stage_timings_s=timings.as_dict(),
            ),
        )
        return (input_meta, {}), *([None] * num_partitions)

    # Step 1: partition each input block into (pid -> [shard_table, ...]).
    partition_accumulators = _partition_blocks_to_shards(
        blocks,
        partition_fn,
        timings=timings,
        input_block_transformer=input_block_transformer,
    )

    # Step 2: merge per-pid shards and ZSTD-encode each partition.
    ipc_write_options = pa.ipc.IpcWriteOptions(compression=pa.Codec("zstd"))
    shard_sizes: Dict[int, Tuple[int, int]] = {}
    partition_bufs: List[Optional[pa.Buffer]] = [None] * num_partitions
    for pid in sorted(partition_accumulators.keys()):
        tables = partition_accumulators.pop(pid)
        if len(tables) > 1:
            with timings.record("map.concat_tables_s"):
                merged = pa.concat_tables(tables)
        else:
            merged = tables[0]
        # Per-task cluster-incremental transform applied after concat.
        # For SEMI/ANTI joins this is the dedup function; collapsing
        # cross-block duplication that the per-block path missed before
        # the IPC encode cuts plasma+network bytes for free.
        if per_partition_post_transformer is not None:
            with timings.record("map.post_concat_transform_s"):
                merged = per_partition_post_transformer(merged)
        shard_sizes[pid] = (merged.num_rows, merged.nbytes)
        timings.add("map.zstd_bytes_in", float(merged.nbytes))
        with timings.record("map.ipc_encode_s"):
            buf = _encode_partition_ipc(merged, ipc_write_options)
        timings.add("map.zstd_bytes_out", float(len(buf)))
        partition_bufs[pid] = buf
        del merged

    # Step 3: build aggregate input metadata and return.
    input_meta = BlockAccessor.for_block(blocks[0]).get_metadata(
        block_exec_stats=stats.build(
            block_ser_time_s=0,
            shuffle_stage_timings_s=timings.as_dict(),
        ),
    )
    input_meta = _dc_replace(input_meta, num_rows=total_rows, size_bytes=total_bytes)
    return (input_meta, shard_sizes), *partition_bufs


# ---------------------------------------------------------------------------
# Reduce-side helpers
# ---------------------------------------------------------------------------


def _maybe_empty_table_for_schema(schema) -> Optional[pa.Table]:
    """Build a zero-row pa.Table that has the given ``schema``.

    Returns ``None`` for non-pyarrow schemas (e.g. PandasBlockSchema) so
    callers can fall back to the original empty-list semantics.

    Used by multi-input reduce tasks to substitute for sides that
    produced no rows for a given partition.  Carrying the typed empty
    table forward lets reduce_fn (e.g. JoiningAggregation.finalize)
    resolve column references against the empty side instead of
    failing with "no match for field reference" on a schemaless block.
    """
    if not isinstance(schema, pa.Schema):
        return None
    arrays = [pa.array([], type=field.type) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)


def _read_partition_ipc(buf: pa.Buffer) -> Optional[pa.Table]:
    """Decompress one partition shard.  Returns None for empty buffers."""
    if len(buf) == 0:
        return None
    reader = pa.ipc.open_stream(buf)
    schema = reader.schema
    batches: List[pa.RecordBatch] = []
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if batch.num_rows > 0:
            batches.append(batch)
    if not batches:
        return None
    return pa.Table.from_batches(batches, schema=schema)


@ray.remote(max_calls=1)
def _shuffle_reduce_task(
    shard_refs: List[ObjectRef],
    partition_id: int,
    reduce_fn: ReduceFn,
    target_max_block_size: Optional[int],
    streaming: bool = True,
) -> Generator[Union[Block, bytes], None, None]:
    """Reduce stage: fetch one partition's shards and call reduce_fn.

    Both modes use batched ray.get (_REDUCE_BATCH_SIZE refs at a time) to
    avoid memory spikes from a single large dereference.

    Streaming mode (streaming=True):
        Calls reduce_fn(partition_id, accumulated_tables) whenever the
        accumulator exceeds target_max_block_size, then pushes the produced
        blocks through a BlockOutputBuffer so each emitted block respects
        target_max_block_size.  Peak input accumulator bounded to
        ~target_max_block_size.  reduce_fn must produce valid output from
        partial data.

    Blocking mode (streaming=False):
        Accumulates all shards before calling reduce_fn(partition_id,
        all_tables) once.  Use this when reduce_fn requires the full
        partition (sort, aggregate).

    Args:
        shard_refs: ObjectRefs to this partition's compressed IPC shards
            from all mappers.  May include ``None`` entries for mappers
            that produced no rows for this partition.
        partition_id: Partition this reducer is responsible for.
        reduce_fn: User-supplied reduce callable.
        target_max_block_size: Target output block size (also the streaming
            flush threshold).  ``None`` means emit blocks as-is (no
            BlockOutputBuffer reshaping, no streaming flush) — used under
            the "partition = block" contract.
        streaming: Whether to flush incrementally (True) or accumulate all
            shards before reducing (False).

    Returns:
        Generator yielding output blocks, each followed by a serialized
        BlockMetadataWithSchema.  Emits one block per non-empty partition;
        partitions that received no rows produce no output.
    """
    start_time_s = time.perf_counter()
    timings = _StageTimings()

    accum_tables: List[pa.Table] = []
    accum_bytes: int = 0
    output_buffer: Optional[BlockOutputBuffer] = None  # lazily created

    def _yield_with_stats(block: Block):
        """Yield (block, pickled metadata) following the streaming-gen protocol."""
        exec_stats_builder = BlockExecStats.builder()
        exec_stats_builder.finish()
        gen_stats: StreamingGeneratorStats = yield block
        exec_stats = exec_stats_builder.build(
            block_ser_time_s=(gen_stats.object_creation_dur_s if gen_stats else None),
            shuffle_stage_timings_s=timings.as_dict(),
        )
        yield pickle.dumps(
            BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )
        )

    def _flush(tables: List[pa.Table]):
        nonlocal output_buffer
        if output_buffer is None:
            output_buffer = BlockOutputBuffer(
                OutputBlockSizeOption.of(
                    target_max_block_size=target_max_block_size,
                )
            )
        with timings.record("reduce.reduce_fn_s"):
            produced_blocks = list(reduce_fn(partition_id, tables))
        for block in produced_blocks:
            with timings.record("reduce.output_buffer_s"):
                output_buffer.add_block(block)
                ready = []
                while output_buffer.has_next():
                    ready.append(output_buffer.next())
            for out_block in ready:
                yield from _yield_with_stats(out_block)

    # Step 1: fetch shard refs in batches, decompress, accumulate.  In
    # streaming mode, when the accumulator reaches target_max_block_size,
    # flush through reduce_fn and yield any ready output blocks.
    for batch_start in range(0, len(shard_refs), _REDUCE_BATCH_SIZE):
        batch = shard_refs[batch_start : batch_start + _REDUCE_BATCH_SIZE]
        with timings.record("reduce.ray_get_s"):
            fetched = ray.get(batch)
        for buf in fetched:
            if buf is None:
                continue
            with timings.record("reduce.ipc_decode_s"):
                table = _read_partition_ipc(buf)
            if table is None:
                continue
            accum_tables.append(table)
            accum_bytes += table.nbytes

            if (
                streaming
                and target_max_block_size is not None
                and accum_bytes >= target_max_block_size
            ):
                tables, accum_tables = accum_tables, []
                accum_bytes = 0
                yield from _flush(tables)

    # Step 2: drain remaining shards through reduce_fn.  This is the only
    # reduce_fn call in blocking mode, and the tail-flush in streaming mode.
    if accum_tables:
        yield from _flush(accum_tables)

    # Step 3: if reduce_fn ran at least once, finalize the buffer to flush
    # any partial block.
    if output_buffer is not None:
        with timings.record("reduce.finalize_s"):
            output_buffer.finalize()
            ready = []
            while output_buffer.has_next():
                ready.append(output_buffer.next())
        for out_block in ready:
            yield from _yield_with_stats(out_block)


@ray.remote(max_calls=1)
def _shuffle_multi_seq_reduce_task(
    shard_refs_by_seq: Dict[int, List[ObjectRef]],
    partition_id: int,
    reduce_fn: "MultiSeqReduceFn",
    target_max_block_size: Optional[int],
    schemas_by_seq: Optional[Dict[int, "pa.Schema"]] = None,
) -> Generator[Union[Block, bytes], None, None]:
    """Multi-input (e.g. join) reduce stage.

    Decodes every input sequence's shards for this partition, then invokes
    ``reduce_fn(partition_id, {seq -> [tables]})`` exactly once.  Multi-input
    combiners (joins, set ops) need the full partition on every side before
    they can produce a correct output, so streaming flush is not supported
    here.

    Empty sides (a partition where one side produced no rows) are replaced
    with a single schema-aware empty Table when ``schemas_by_seq`` carries
    that seq's schema, so reduce_fn always sees a Table that knows its
    columns even when the side contributed zero rows.  Without this,
    PyArrow operations (e.g. Table.join) cannot resolve key column
    references against the empty side.

    Output blocks are pushed through a BlockOutputBuffer so they respect
    ``target_max_block_size`` — same contract as the single-input task.
    """
    start_time_s = time.perf_counter()
    timings = _StageTimings()
    if schemas_by_seq is None:
        schemas_by_seq = {}

    decoded_by_seq: Dict[int, List[pa.Table]] = {}
    for seq_idx, refs in shard_refs_by_seq.items():
        tables: List[pa.Table] = []
        for batch_start in range(0, len(refs), _REDUCE_BATCH_SIZE):
            batch = refs[batch_start : batch_start + _REDUCE_BATCH_SIZE]
            with timings.record("reduce.ray_get_s"):
                fetched = ray.get(batch)
            for buf in fetched:
                if buf is None:
                    continue
                with timings.record("reduce.ipc_decode_s"):
                    table = _read_partition_ipc(buf)
                if table is not None:
                    tables.append(table)
        if not tables and seq_idx in schemas_by_seq:
            empty = _maybe_empty_table_for_schema(schemas_by_seq[seq_idx])
            if empty is not None:
                # Materialize a typed empty Table so PyArrow ops downstream
                # can resolve column references against this side.
                tables = [empty]
        decoded_by_seq[seq_idx] = tables

    output_buffer = BlockOutputBuffer(
        OutputBlockSizeOption.of(
            target_max_block_size=target_max_block_size,
        )
    )

    with timings.record("reduce.reduce_fn_s"):
        produced_blocks = list(reduce_fn(partition_id, decoded_by_seq))

    def _yield_with_stats(block: Block):
        exec_stats_builder = BlockExecStats.builder()
        exec_stats_builder.finish()
        gen_stats: StreamingGeneratorStats = yield block
        exec_stats = exec_stats_builder.build(
            block_ser_time_s=(gen_stats.object_creation_dur_s if gen_stats else None),
            shuffle_stage_timings_s=timings.as_dict(),
        )
        yield pickle.dumps(
            BlockMetadataWithSchema.from_block(
                block,
                block_exec_stats=exec_stats,
                task_exec_stats=TaskExecWorkerStats(
                    task_wall_time_s=time.perf_counter() - start_time_s,
                ),
            )
        )

    for block in produced_blocks:
        with timings.record("reduce.output_buffer_s"):
            output_buffer.add_block(block)
            ready = []
            while output_buffer.has_next():
                ready.append(output_buffer.next())
        for out_block in ready:
            yield from _yield_with_stats(out_block)

    with timings.record("reduce.finalize_s"):
        output_buffer.finalize()
        ready = []
        while output_buffer.has_next():
            ready.append(output_buffer.next())
    for out_block in ready:
        yield from _yield_with_stats(out_block)
