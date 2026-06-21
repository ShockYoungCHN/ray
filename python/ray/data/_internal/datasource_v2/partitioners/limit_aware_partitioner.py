"""Limit-aware file partitioner wrapper for DataSourceV2.

Wraps an inner :class:`FilePartitioner` and trims its input chunk stream
so we stop generating partitions once the accumulated row count satisfies
the pushed scanner limit. Without this wrapper, the listing path emits
one partition per data bucket and dispatches a ReadTask for each — even
for buckets whose rows will never be needed because the downstream
``LimitOperator`` will already be satisfied. Per-task limit pushdown still
caps each read, but every dispatched task pays a startup cost and reads
at least one row group.

Globally precise row count is still the downstream ``LimitOperator``'s
job: chunks are atomic at the row-group level, so the boundary chunk that
crosses the limit is admitted in full and the operator trims the
overshoot.

The wrapper is no-op for chunkers whose metadata lacks a ``num_rows``
key (everything except :class:`ParquetFileChunkMetadata` today): we
forward all input unchanged. This keeps the wrapper safe to enable
unconditionally on the V2 read path; it only kicks in when both the
scanner has a limit pushed down and the chunker exposes per-chunk row
counts.
"""
from typing import Optional

import pyarrow as pa

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FileManifest,
    chunk_num_rows,
)
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data.block import BlockAccessor


class LimitAwareFilePartitioner(FilePartitioner):
    """Wraps a ``FilePartitioner`` and stops emitting partitions once
    ``limit_rows`` is reached.

    Semantics:

    - Cumulative row counts are read from each chunk's ``num_rows`` field
      (populated by :class:`ParquetFileChunker`). Chunks without a
      ``num_rows`` key contribute 0 to the running total — they are
      forwarded but don't drive cutoff (safe degradation for non-Parquet
      chunkers).
    - The boundary chunk that pushes the cumulative count over
      ``limit_rows`` is still admitted (chunks are atomic at the row-group
      level). Downstream ``LimitOperator`` trims the overshoot to exactly
      ``limit_rows``.
    - Once the limit is reached, subsequent ``add_input`` calls become
      no-ops; ``finalize`` still flushes whatever the inner partitioner
      has buffered.

    Args:
        inner: The wrapped ``FilePartitioner`` (e.g.
            :class:`FileAffinityPartitioner` or
            :class:`RoundRobinPartitioner`).
        limit_rows: Pushed scanner limit. Must be a positive integer.
    """

    def __init__(self, inner: FilePartitioner, limit_rows: int):
        if limit_rows <= 0:
            raise ValueError(
                f"limit_rows must be positive, got {limit_rows}"
            )
        self._inner = inner
        self._limit_rows = int(limit_rows)
        self._rows_so_far = 0
        self._limit_reached = False

    def add_input(self, input_manifest: FileManifest) -> None:
        if self._limit_reached:
            return

        chunk_metadatas = input_manifest.file_chunk_metadatas
        if len(chunk_metadatas) == 0:
            return

        # Find the cutoff index: the smallest k such that the first k+1
        # chunks already cover ``limit_rows``. Chunks at index <= k are
        # admitted (including the boundary); chunks beyond are dropped.
        cutoff_idx: Optional[int] = None
        running = self._rows_so_far
        rows_before_boundary = self._rows_so_far
        for idx, chunk_md in enumerate(chunk_metadatas):
            chunk_rows = chunk_num_rows(chunk_md)
            if running + chunk_rows >= self._limit_rows:
                cutoff_idx = idx
                # ``rows_before_boundary`` is the cumulative count BEFORE
                # admitting the boundary chunk; the boundary's max_emit
                # is the residual.
                break
            running += chunk_rows
            rows_before_boundary = running

        if cutoff_idx is None:
            # No boundary inside this manifest; forward all chunks and
            # accumulate.
            self._rows_so_far = running
            self._inner.add_input(input_manifest)
            return

        # Boundary lies inside this manifest. The chunk metadata column is
        # stored as a PyArrow struct, so in-place mutation of the dict
        # returned by ``to_numpy()`` would NOT persist (each access
        # synthesizes a fresh Python dict from struct fields). We rebuild
        # the manifest with the boundary metadata updated and the post-
        # boundary rows sliced out in one pass.
        max_emit = self._limit_rows - rows_before_boundary

        self._rows_so_far = self._limit_rows
        self._limit_reached = True

        new_manifest = _build_manifest_with_boundary_stamped(
            input_manifest,
            keep_rows_until=cutoff_idx + 1,
            boundary_idx=cutoff_idx,
            max_emit_rows=max_emit,
        )
        self._inner.add_input(new_manifest)

    def has_partition(self) -> bool:
        return self._inner.has_partition()

    def next_partition(self) -> FileManifest:
        return self._inner.next_partition()

    def finalize(self) -> None:
        self._inner.finalize()


def _build_manifest_with_boundary_stamped(
    manifest: FileManifest,
    *,
    keep_rows_until: int,
    boundary_idx: int,
    max_emit_rows: int,
) -> FileManifest:
    """Return a new manifest holding rows [0, keep_rows_until) with the
    boundary chunk's max_emit_rows stamped.

    Chunk metadata lives in a PyArrow struct column, so the dicts returned
    by to_pylist() are fresh copies -- mutating them doesn't write back.
    Rebuild the column as a new PyArrow array and swap it in via
    set_column (other columns -- __path, __file_size -- carry through).
    """
    block = manifest.as_block()
    table = BlockAccessor.for_block(block).to_arrow()
    sliced = table.slice(0, keep_rows_until)

    mds = sliced.column(FILE_CHUNK_METADATA_COLUMN_NAME).to_pylist()
    mds[boundary_idx] = {**mds[boundary_idx], "max_emit_rows": int(max_emit_rows)}

    col_idx = sliced.schema.get_field_index(FILE_CHUNK_METADATA_COLUMN_NAME)
    new_col = pa.array(mds, type=sliced.schema.field(col_idx).type)
    rebuilt = sliced.set_column(col_idx, FILE_CHUNK_METADATA_COLUMN_NAME, new_col)
    return FileManifest(rebuilt)
