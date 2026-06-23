"""File-affinity partitioner for DataSourceV2.

Two modes, picked at construction:

**Per-file mode (default)**: Groups each file's chunks into bounded
partitions, preserving file locality -- every emitted partition holds
chunks of exactly one file; a file is split into multiple partitions
of consecutive row-group chunks when its accumulated size exceeds
``max_rows_per_partition`` or ``max_bucket_size``. The number of
partitions is data-driven: ``sum over files of
ceil(file_total / cap)``. Floor is ``file_count`` -- this mode never
merges chunks across files.

**Static cross-file mode**: opted into by passing ``num_partitions``.
``add_input`` only buffers chunks; ``finalize`` runs a one-shot
balanced partitioning over all buffered chunks. Targets are computed
against the *exact* total row count (summed from buffered chunks),
which is what makes this "static" rather than "adaptive" -- we know
the denominator before allocating. The partitioning walks chunks in
arrival order and re-targets each remaining partition as
``(total_rows - emitted_rows) / remaining_partitions``, so an overshoot
on partition K shrinks the target for K+1..N. Result:
~``num_partitions`` evenly-sized partitions regardless of file count.
File affinity is sacrificed for parallelism control -- partitions can
straddle file boundaries. Pairs with the lazy-enumeration listing
path so ``read_parquet(parallelism=N).limit(M)`` produces ~N read
tasks even on many-small-file datasets.

Contrast with :class:`RoundRobinPartitioner`, which spreads chunks
by byte size across a fixed num_buckets target. Static mode here
uses row counts directly, matching the row-cap semantics of the
limit-pushdown path.
"""

import collections
from typing import Dict, List, Optional, Tuple

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FileManifest,
    chunk_num_rows,
)
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.datasource_v2.readers.in_memory_size_estimator import (
    InMemorySizeEstimator,
)


def _effective_rows(chunk_metadata: Optional[dict]) -> int:
    """Rows this chunk will emit at read time.

    Boundary chunks stamped by the lazy-enumeration path carry a
    ``max_emit_rows < num_rows`` -- the reader trims to it. For balanced
    partitioning we have to size against the trimmed count, otherwise the
    boundary partition appears artificially large.
    """
    if chunk_metadata is None:
        return 0
    try:
        num_rows = int(chunk_metadata.get("num_rows", 0))
    except (AttributeError, TypeError):
        return 0
    max_emit = chunk_metadata.get("max_emit_rows")
    if max_emit is None:
        return num_rows
    try:
        return min(num_rows, int(max_emit))
    except (TypeError, ValueError):
        return num_rows


class _Bucket:
    """Accumulates chunks (single file or multi-file) until flushed.

    Used for both per-file mode (one bucket per path) and static cross-file
    mode (one shared bucket per emitted partition). ``to_manifest()`` sorts
    by ``(file_seq, row_group_start)`` so inter-file ordering is
    deterministic in cross-file mode and degenerates to row_group_start
    in single-file mode.
    """

    def __init__(self):
        self.paths: List[str] = []
        self.file_sizes: List[int] = []
        self.chunk_metadatas: List[Optional[dict]] = []
        self.sort_keys: List[tuple] = []
        self.in_memory_size: int = 0
        self.num_rows: int = 0
        self._file_seq: Dict[str, int] = {}

    def add(self, path, file_size, chunk_metadata, in_memory_size, num_rows, rg_start):
        if path not in self._file_seq:
            self._file_seq[path] = len(self._file_seq)
        self.paths.append(path)
        self.file_sizes.append(file_size)
        self.chunk_metadatas.append(chunk_metadata)
        self.sort_keys.append((self._file_seq[path], rg_start))
        self.in_memory_size += in_memory_size
        self.num_rows += num_rows

    def to_manifest(self) -> FileManifest:
        order = sorted(range(len(self.paths)), key=lambda i: self.sort_keys[i])
        return FileManifest.construct_manifest(
            [self.paths[i] for i in order],
            [self.file_sizes[i] for i in order],
            [self.chunk_metadatas[i] for i in order],
        )


# Tuple of fields needed to (re-)add a chunk to a bucket.
_BufferedChunk = Tuple[str, int, Optional[dict], int, int, int]
# (path, file_size, chunk_metadata, in_memory_size, effective_rows, rg_start)


class FileAffinityPartitioner(FilePartitioner):
    """Partitions chunks per file (default) or statically across files.

    See module docstring for the two modes. Per-file mode caps
    (``max_rows_per_partition``, ``max_bucket_size``) are optional; static
    mode is opted into by passing ``num_partitions``. ``total_rows`` is
    accepted for API compatibility but ignored -- the static path always
    computes the total from buffered chunks.
    """

    def __init__(
        self,
        *,
        in_memory_size_estimator: Optional[InMemorySizeEstimator] = None,
        max_bucket_size: Optional[int] = None,
        max_rows_per_partition: Optional[int] = None,
        total_rows: Optional[int] = None,
        num_partitions: Optional[int] = None,
    ):
        self._in_memory_size_estimator = in_memory_size_estimator
        self._max_bucket_size = max_bucket_size
        self._max_rows_per_partition = max_rows_per_partition

        # Static cross-file mode triggers on num_partitions alone. The
        # ``total_rows`` arg is accepted only for backward API compatibility
        # -- the static path computes the exact total from buffered chunks
        # at finalize, so any caller-supplied value would be ignored anyway.
        self._static = num_partitions is not None and num_partitions > 0
        self._num_partitions: int = int(num_partitions) if num_partitions else 0
        _ = total_rows  # silence "unused argument" without touching the API.

        # Per-file mode state.
        self._open_buckets: Dict[str, _Bucket] = {}
        # Static-mode buffer (filled in add_input, drained in finalize).
        self._buffered: List[_BufferedChunk] = []

        self._output_queue: "collections.deque[FileManifest]" = collections.deque()

    def add_input(self, input_manifest: FileManifest):
        in_memory_sizes = (
            self._in_memory_size_estimator.estimate_in_memory_sizes(input_manifest)
            if self._in_memory_size_estimator is not None
            else [0] * len(input_manifest)
        )
        for path, file_size, chunk_metadata, in_memory_size in zip(
            input_manifest.paths,
            input_manifest.file_sizes,
            input_manifest.file_chunk_metadatas,
            in_memory_sizes,
        ):
            rg_start = (
                int(chunk_metadata["row_group_start"])
                if chunk_metadata is not None and "row_group_start" in chunk_metadata
                else 0
            )
            num_rows = chunk_num_rows(chunk_metadata)
            eff_rows = _effective_rows(chunk_metadata) if num_rows > 0 else 0
            size = int(in_memory_size or 0)

            if self._static:
                # Static mode buffers; ``finalize`` does the packing.
                self._buffered.append(
                    (path, int(file_size), chunk_metadata, size, eff_rows, rg_start)
                )
            else:
                self._add_per_file(
                    path, file_size, chunk_metadata, size, num_rows, rg_start
                )

    def _add_per_file(self, path, file_size, chunk_metadata, size, num_rows, rg_start):
        bucket = self._open_buckets.get(path)
        if bucket is None:
            bucket = _Bucket()
            self._open_buckets[path] = bucket
        bucket.add(path, int(file_size), chunk_metadata, size, num_rows, rg_start)
        if self._should_flush_per_file(bucket):
            self._output_queue.append(bucket.to_manifest())
            del self._open_buckets[path]

    def _should_flush_per_file(self, bucket: _Bucket) -> bool:
        if (
            self._max_rows_per_partition is not None
            and bucket.num_rows >= self._max_rows_per_partition
        ):
            return True
        if (
            self._max_bucket_size is not None
            and bucket.in_memory_size >= self._max_bucket_size
        ):
            return True
        return False

    def has_partition(self) -> bool:
        return len(self._output_queue) > 0

    def next_partition(self) -> FileManifest:
        return self._output_queue.popleft()

    def finalize(self):
        if self._static:
            self._finalize_static()
            return
        # Per-file mode: each remaining file becomes its own partition;
        # sort by path for deterministic output across retries.
        for path in sorted(self._open_buckets.keys()):
            bucket = self._open_buckets[path]
            if bucket.paths:
                self._output_queue.append(bucket.to_manifest())
        self._open_buckets.clear()

    def _finalize_static(self):
        """One-shot balanced partitioning over the buffered chunks.

        Algorithm: walk chunks in arrival order, packing into a single
        bucket. After each add, re-target as
        ``(total_rows - emitted_rows) / remaining_partitions``. When the
        bucket reaches the current target AND we still have at least 2
        partitions to emit, flush. The very last partition swallows
        whatever is left -- chunk atomicity means it may be smaller than
        the target. Earlier partitions automatically shrink when prior
        ones overshot, keeping the tail bounded by one chunk's worth.

        Empty buffer is a no-op. ``num_partitions > chunk count`` caps the
        emitted count at chunk count: you can't have more partitions than
        atomic units.
        """
        chunks = self._buffered
        self._buffered = []
        if not chunks:
            return

        total_rows = sum(c[4] for c in chunks)
        n_parts = max(1, min(self._num_partitions, len(chunks)))

        bucket = _Bucket()
        emitted_rows = 0
        emitted_count = 0

        for path, file_size, meta, size, eff_rows, rg_start in chunks:
            bucket.add(path, file_size, meta, size, eff_rows, rg_start)
            remaining_parts = n_parts - emitted_count
            if remaining_parts <= 1:
                # Last partition swallows the rest -- no more flushes.
                continue
            cur_target = (total_rows - emitted_rows) / remaining_parts
            if bucket.num_rows >= cur_target:
                self._output_queue.append(bucket.to_manifest())
                emitted_rows += bucket.num_rows
                emitted_count += 1
                bucket = _Bucket()

        if bucket.paths:
            self._output_queue.append(bucket.to_manifest())
