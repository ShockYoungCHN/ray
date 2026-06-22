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

**Adaptive cross-file mode**: opted into by passing both ``total_rows``
and ``num_partitions``. A single shared bucket accumulates chunks
across files; flush is gated by an *adaptive target* recomputed at
each step as ``remaining_rows / remaining_partitions``. Last partition
swallows any leftover. Result: ~``num_partitions`` evenly-sized
partitions regardless of file count -- file affinity is sacrificed
for exact parallelism control. Used by the limit-pushdown path so
``read_parquet(parallelism=N).limit(M)`` produces ~N read tasks even
on many-small-file datasets where the per-file mode would floor at
file count.

Contrast with :class:`RoundRobinPartitioner`, which spreads chunks
by byte size across a fixed num_buckets target. Adaptive mode here
uses row counts directly, matching the row-cap semantics of the
limit-pushdown path.
"""

import collections
from typing import Dict, List, Optional

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


class _Bucket:
    """Accumulates chunks (single file or multi-file) until flushed.

    Used for both per-file mode (one bucket per path) and adaptive
    cross-file mode (one shared bucket). to_manifest() preserves
    insertion order across files and row-group-start order within a
    file.
    """

    def __init__(self):
        self.paths: List[str] = []
        self.file_sizes: List[int] = []
        self.chunk_metadatas: List[Optional[dict]] = []
        # (file_seq, row_group_start) — file_seq is the order this file
        # was first encountered in the bucket, so ordering after sort
        # keeps inter-file ordering deterministic in cross-file mode
        # and degenerates to row_group_start in single-file mode.
        self.sort_keys: List[tuple] = []
        self.in_memory_size: int = 0
        self.num_rows: int = 0
        # Maps path -> file_seq (insertion index) for sort_key building.
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


class FileAffinityPartitioner(FilePartitioner):
    """Partitions chunks per file (default) or adaptively across files.

    See module docstring for the two modes. Both caps for the per-file
    mode (``max_rows_per_partition``, ``max_bucket_size``) are optional;
    adaptive mode is opted into by passing both ``total_rows`` and
    ``num_partitions``.
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

        # Adaptive mode requires both. When either is missing, fall back to
        # per-file mode (existing behavior).
        self._adaptive = (
            total_rows is not None
            and total_rows > 0
            and num_partitions is not None
            and num_partitions > 0
        )
        self._total_rows: int = int(total_rows) if total_rows else 0
        self._num_partitions: int = int(num_partitions) if num_partitions else 0

        # Per-file mode state.
        self._open_buckets: Dict[str, _Bucket] = {}
        # Adaptive mode state.
        self._shared_bucket: Optional[_Bucket] = _Bucket() if self._adaptive else None
        self._emitted_count: int = 0
        self._emitted_rows: int = 0

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
            size = int(in_memory_size or 0)

            if self._adaptive:
                self._add_adaptive(
                    path, file_size, chunk_metadata, size, num_rows, rg_start
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

    def _add_adaptive(self, path, file_size, chunk_metadata, size, num_rows, rg_start):
        assert self._shared_bucket is not None
        self._shared_bucket.add(
            path, int(file_size), chunk_metadata, size, num_rows, rg_start
        )
        # Adaptive target: divide whatever's left to distribute by the
        # remaining partitions (including the one currently being built).
        # Last partition (remaining=1) never flushes mid-stream -- it
        # swallows whatever leftover comes at finalize. Earlier partitions
        # shrink slightly when prior ones overshot (due to chunk atomicity),
        # keeping the tail from collapsing.
        remaining_partitions = self._num_partitions - self._emitted_count
        if remaining_partitions <= 1:
            return
        target = (self._total_rows - self._emitted_rows) / remaining_partitions
        if self._shared_bucket.num_rows >= target:
            self._emit_shared()

    def _emit_shared(self):
        assert self._shared_bucket is not None
        self._emitted_rows += self._shared_bucket.num_rows
        self._emitted_count += 1
        self._output_queue.append(self._shared_bucket.to_manifest())
        self._shared_bucket = _Bucket()

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
        if self._adaptive:
            # Emit the last (possibly partially-full) partition.
            if self._shared_bucket is not None and self._shared_bucket.paths:
                self._emit_shared()
            self._shared_bucket = None
            return
        # Per-file mode: each remaining file becomes its own partition;
        # sort by path for deterministic output across retries.
        for path in sorted(self._open_buckets.keys()):
            bucket = self._open_buckets[path]
            if bucket.paths:
                self._output_queue.append(bucket.to_manifest())
        self._open_buckets.clear()
