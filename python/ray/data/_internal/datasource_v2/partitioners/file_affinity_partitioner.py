"""File-affinity partitioner for DataSourceV2.

Groups each file's chunks into bounded partitions, preserving file
locality: every emitted partition holds chunks of exactly one file, and a
file is split into multiple partitions of consecutive ("sister") row-group
chunks when its accumulated size exceeds either of the configured caps.
This gives read-task locality (one file per task -> one open + one footer
read + sequential I/O) plus sub-file parallelism for large files.

Two flush caps may be configured (either or both):

- ``max_rows_per_partition``: row count cap, used directly by the limit-
  pushdown path (``LimitPushdownRule`` derives it from ``limit_rows /
  (2 * cpus)`` with no bytes intermediate). Row counts come from chunk
  metadata (``num_rows`` field); chunkers that don't stamp it contribute
  0 here, so the row cap is a no-op for non-row-aware formats.

- ``max_bucket_size``: in-memory byte cap, used by the default read path
  (``read_api`` derives it from ``DataContext.target_max_block_size``).
  Byte estimates come from the ``in_memory_size_estimator`` (footer-
  derived for Parquet under ``parquet_use_footer_size_estimate``, fallback
  on-disk * ratio otherwise).

Either cap flushing triggers a partition emit; both can be set
simultaneously. When neither is set, each file yields exactly one
partition holding all its chunks (at ``finalize`` time).

Contrast with :class:`RoundRobinPartitioner`, which spreads a file's
chunks across buckets and so can mix multiple files within a single
partition.
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


class _FileBucket:
    """Accumulates one file's chunks until it is flushed as a partition."""

    def __init__(self):
        self.paths: List[str] = []
        self.file_sizes: List[int] = []
        self.chunk_metadatas: List[Optional[dict]] = []
        self.sort_keys: List[int] = []
        self.in_memory_size: int = 0
        self.num_rows: int = 0

    def add(
        self, path, file_size, chunk_metadata, in_memory_size, num_rows, sort_key
    ):
        self.paths.append(path)
        self.file_sizes.append(file_size)
        self.chunk_metadatas.append(chunk_metadata)
        self.sort_keys.append(sort_key)
        self.in_memory_size += in_memory_size
        self.num_rows += num_rows

    def to_manifest(self) -> FileManifest:
        # Sort by row-group start so each partition is a clean ascending range:
        # deterministic output (the FilePartitioner contract) and lets the
        # reader coalesce contiguous row groups into a single scan.
        order = sorted(range(len(self.paths)), key=lambda i: self.sort_keys[i])
        return FileManifest.construct_manifest(
            [self.paths[i] for i in order],
            [self.file_sizes[i] for i in order],
            [self.chunk_metadatas[i] for i in order],
        )


class FileAffinityPartitioner(FilePartitioner):
    """Partitions chunks per file, bounded by row count and/or in-memory bytes.

    All chunks of a file go to partitions of that file only (never mixed
    with other files); a file accumulating past either cap is split into
    multiple partitions of consecutive row-group chunks. ``num_buckets``
    is intentionally absent: the number of partitions is data-driven
    (sum over files of ``ceil(file_total / cap)``).

    Groups by path (not arrival position): the indexer may interleave
    different files' chunks via parallel footer reads, so contiguity in
    the input stream is not assumed.

    Both caps are optional. When both are set, whichever fills first
    triggers the flush. When neither is set, each file yields exactly
    one partition.
    """

    def __init__(
        self,
        *,
        in_memory_size_estimator: Optional[InMemorySizeEstimator] = None,
        max_bucket_size: Optional[int] = None,
        max_rows_per_partition: Optional[int] = None,
    ):
        self._in_memory_size_estimator = in_memory_size_estimator
        self._max_bucket_size = max_bucket_size
        self._max_rows_per_partition = max_rows_per_partition
        # path -> bucket currently accumulating that file's chunks.
        self._open_buckets: Dict[str, _FileBucket] = {}
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
            bucket = self._open_buckets.get(path)
            if bucket is None:
                bucket = _FileBucket()
                self._open_buckets[path] = bucket
            sort_key = (
                int(chunk_metadata["row_group_start"])
                if chunk_metadata is not None and "row_group_start" in chunk_metadata
                else 0
            )
            bucket.add(
                path,
                int(file_size),
                chunk_metadata,
                int(in_memory_size or 0),
                chunk_num_rows(chunk_metadata),
                sort_key,
            )
            # Flush when EITHER cap is hit. Subsequent chunks of the same
            # file start a fresh bucket, so each partition is a consecutive
            # range of one file's row groups.
            if self._should_flush(bucket):
                self._output_queue.append(bucket.to_manifest())
                del self._open_buckets[path]

    def _should_flush(self, bucket: _FileBucket) -> bool:
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
        # Flush each file's remaining chunks; sort by path for deterministic
        # output across retries.
        for path in sorted(self._open_buckets.keys()):
            bucket = self._open_buckets[path]
            if bucket.paths:
                self._output_queue.append(bucket.to_manifest())
        self._open_buckets.clear()
