import logging
import math
import os
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Optional, Tuple

import pyarrow as pa
from pyarrow.fs import FileSystem

from ray._common.utils import env_integer
from ray.data._internal.datasource_v2.chunkers.file_chunker import (
    ChunkMetadata,
    FileChunker,
    WholeFileChunker,
)
from ray.data._internal.datasource_v2.listing.file_manifest import FileManifest
from ray.data._internal.datasource_v2.listing.file_pruners import FilePruner
from ray.data._internal.datasource_v2.listing.indexing_utils import _get_file_infos
from ray.data._internal.util import make_async_gen
from ray.data.block import BlockColumn
from ray.data.datasource.path_util import _resolve_paths_and_filesystem

logger = logging.getLogger(__name__)

# Over-estimate factor used by the lazy enumeration path -- after reading one
# file's footer to calibrate avg rows/file, we fetch
# ``ceil(target_rows / avg * _LAZY_OVER_ESTIMATE)`` more footers so undershoots
# are rare. 1.15 = 15% buffer; iterative top-up still kicks in if the actual
# average runs lower than the first file's.
_LAZY_OVER_ESTIMATE = 1.15

# Cap on PyArrow's process-wide io thread pool, applied lazily the first
# time we enter the lazy-enumeration path. PyArrow's S3FileSystem issues
# range reads via this pool; the default (~8) starves our outer
# ThreadPoolExecutor of in-flight network requests and re-enacts the same
# "25-conn pool" symptom we measured before. Override at runtime with
# ``RAY_DATA_PYARROW_IO_THREADS``. Setting too high is harmless on the
# client side -- PyArrow only spawns threads as needed.
_PYARROW_IO_THREADS = env_integer("RAY_DATA_PYARROW_IO_THREADS", 128)
_pyarrow_io_threads_applied = False

# When set, prints a one-line ``LISTING_PROF`` summary per ListFiles task
# with per-phase wall times. Mirrors ``RAY_DATA_SHUFFLE_PROFILE``'s style so
# both can be parsed by the same tooling. Off by default — no overhead.
_LIST_FILES_PROFILE = os.environ.get("RAY_DATA_LIST_FILES_PROFILE") == "1"


def _ensure_pyarrow_io_thread_count() -> None:
    """Bump ``pa.set_io_thread_count`` to ``_PYARROW_IO_THREADS`` once per
    process. Idempotent; safe to call from worker tasks (each Ray worker
    has its own Python process and applies the override on first use)."""
    global _pyarrow_io_threads_applied
    if _pyarrow_io_threads_applied:
        return
    try:
        if pa.io_thread_count() < _PYARROW_IO_THREADS:
            pa.set_io_thread_count(_PYARROW_IO_THREADS)
    except Exception:
        # PyArrow may reject sets in some embedded builds; just leave the
        # default in place rather than failing the listing task.
        pass
    _pyarrow_io_threads_applied = True


class FileIndexer(ABC):
    @property
    @abstractmethod
    def file_chunker(self) -> FileChunker:
        """The file chunker that this indexer uses."""
        ...

    @abstractmethod
    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
        target_rows: Optional[int] = None,
    ) -> Iterable[FileManifest]:
        """List files and their on-disk sizes for the given path.

        Args:
            paths: A column of paths pointing to files or directories.
            filesystem: A PyArrow filesystem object.
            pruners: A list of file pruners to apply.
            preserve_order: Whether to preserve order in file listing.
            target_rows: Optional row target. When set and the chunker
                exposes per-chunk ``num_rows`` (Parquet), the indexer reads
                only as many footers as are needed to cover ``target_rows``
                instead of every discovered file -- it calibrates an avg
                rows-per-file from one footer, parallel-fetches an
                estimate-sized batch, and trims the boundary chunk via
                ``max_emit_rows``. Falls back to the eager path otherwise.

        Returns:
            An iterator of `FileManifest` objects, each of which contains a file path
            and the on-disk size of the file in bytes.
        """
        ...


@dataclass
class FileInfo:
    """File information for file listing."""

    path: str
    size: Optional[int]


class NonSamplingFileIndexer(FileIndexer):
    """A file indexer that exhaustively lists files.

    This implementation works with paths that point to files or directories,
    although it's slow if you try to list lots of paths pointing to files
    rather than a single directory.
    """

    _DEFAULT_MAX_PATHS_PER_OUTPUT = env_integer(
        "RAY_DATA_MAX_PATHS_PER_LIST_FILES_OUTPUT", 1000
    )

    _DEFAULT_NUM_WORKERS = env_integer("RAY_DATA_LIST_FILES_THREADED_NUM_WORKERS", 4)

    # Footer-read concurrency for the lazy-enumeration path. Footer reads are
    # I/O bound (range requests on S3), so we run a much wider pool than the
    # default 4-worker path. 64 threads / 8 physical cores is the sweet spot
    # measured on sf1000 (1000 files, ~295 MB each); see bench_lazy_enumerate.py.
    _DEFAULT_LAZY_FOOTER_THREADS = env_integer("RAY_DATA_LAZY_FOOTER_THREADS", 64)

    def __init__(
        self,
        *,
        ignore_missing_paths: bool,
        num_workers: Optional[int] = None,
        max_paths_per_output: Optional[int] = None,
        file_chunker: Optional[FileChunker] = None,
    ):
        self._ignore_missing_paths = ignore_missing_paths
        self._max_paths_per_output = (
            max_paths_per_output
            if max_paths_per_output is not None
            else self._DEFAULT_MAX_PATHS_PER_OUTPUT
        )
        self._num_workers = (
            num_workers if num_workers is not None else self._DEFAULT_NUM_WORKERS
        )
        self._queue_size_per_thread = env_integer(
            "RAY_DATA_LIST_FILES_QUEUE_SIZE_PER_THREAD",
            self._max_paths_per_output * 4,
        )
        self._file_chunker: FileChunker = (
            file_chunker if file_chunker is not None else WholeFileChunker()
        )

    @property
    def file_chunker(self) -> FileChunker:
        """The file chunker that this indexer uses.

        Exposed primarily for tests and shuffle-aware planning code that needs
        to introspect or override the chunking strategy.
        """
        return self._file_chunker

    @file_chunker.setter
    def file_chunker(self, chunker: FileChunker) -> None:
        """Replace the indexer's chunker at plan time. Used by
        ``plan_list_files_op`` to swap the row-group-aware Parquet chunker
        for a byte-estimate chunker on the full-scan path -- the row-group
        chunker's footer pre-fetch is redundant when no limit is pushed,
        because each read task reads the footer once anyway.
        """
        self._file_chunker = chunker

    def list_files(
        self,
        paths: "BlockColumn",
        *,
        filesystem: "FileSystem",
        pruners: Optional[List[FilePruner]] = None,
        preserve_order: bool = False,
        target_rows: Optional[int] = None,
    ) -> Iterable[FileManifest]:
        file_info_iterator = (
            self._get_file_info_iterator_threaded(paths, filesystem, preserve_order)
            if self._num_workers > 1
            else self._get_file_info_iterator_sequential(paths, filesystem)
        )

        # Stage pipeline: list → prune (cheap, inline) → chunk (may read
        # per-file metadata) → batch into manifests. Pruning runs *before*
        # chunking so we never read a footer for a file we'd discard.
        pruned = self._filter_file_infos(file_info_iterator, pruners or [])

        # Lazy-enumeration path: when the caller passed a row target and the
        # chunker is row-count-aware (Parquet today), short-circuit footer
        # reads once the running row total covers the target. Otherwise drop
        # into the streaming pipeline.
        if (
            target_rows is not None
            and target_rows > 0
            and self._file_chunker.reads_file_metadata
            and self._file_chunker.supports_row_count_limit
        ):
            chunk_records = self._generate_chunk_records_lazy(
                pruned, filesystem, target_rows
            )
        else:
            chunk_records = self._generate_chunk_records(
                pruned, filesystem, preserve_order
            )
        yield from self._batch_chunk_records_to_manifests(chunk_records)

    def _get_file_info_iterator_sequential(
        self,
        paths: "BlockColumn",
        filesystem: "FileSystem",
    ) -> Iterable[FileInfo]:
        for input_path in paths.to_pylist():
            resolved_paths, _ = _resolve_paths_and_filesystem(input_path, filesystem)
            assert len(resolved_paths) == 1

            for path, file_size in _get_file_infos(
                resolved_paths[0], filesystem, self._ignore_missing_paths
            ):
                yield FileInfo(path=path, size=file_size)

    def _get_file_info_iterator_threaded(
        self,
        paths: "BlockColumn",
        filesystem: "FileSystem",
        preserve_order: bool = False,
    ) -> Iterable[FileInfo]:
        paths_list = paths.to_pylist()
        if len(paths_list) == 0:
            return

        num_workers = min(self._num_workers, len(paths_list))

        def process_paths(
            path_iterator: Iterator[str],
        ) -> Iterator[FileInfo]:
            for input_path in path_iterator:
                resolved_paths, _ = _resolve_paths_and_filesystem(
                    input_path, filesystem
                )
                assert len(resolved_paths) == 1

                for path, file_size in _get_file_infos(
                    resolved_paths[0],
                    filesystem,
                    self._ignore_missing_paths,
                ):
                    yield FileInfo(path=path, size=file_size)

        yield from make_async_gen(
            base_iterator=iter(paths_list),
            fn=process_paths,
            preserve_ordering=preserve_order,
            num_workers=num_workers,
            buffer_size=self._queue_size_per_thread,
        )

    def _filter_file_infos(
        self,
        file_infos: Iterable[FileInfo],
        pruners: List[FilePruner],
    ) -> Iterator[FileInfo]:
        """Drop zero-size and pruned files before any per-file metadata read."""
        for file_info in file_infos:
            if file_info.size is None or file_info.size == 0:
                logger.warning(f"Skipping zero-size file: {file_info.path!r}")
                continue
            if not all(pruner.should_include(file_info.path) for pruner in pruners):
                continue
            yield file_info

    def _generate_chunk_records(
        self,
        file_infos: Iterable[FileInfo],
        filesystem: "FileSystem",
        preserve_order: bool,
    ) -> Iterator[Tuple[str, int, Optional[ChunkMetadata]]]:
        """Drive the chunker per file, yielding ``(path, chunk_size, metadata)``.

        When the chunker reads per-file metadata (e.g. ``ParquetFileChunker``
        reading footers), fan the work across the indexer's thread pool so the
        I/O parallelizes even for a single input directory — ``make_async_gen``
        over the *discovered files*, not the input paths. Chunkers that don't
        read metadata (whole-file / line-delimited) are driven inline to avoid
        a pointless thread hand-off.
        """
        chunker = self._file_chunker

        def chunk(
            infos: Iterator[FileInfo],
        ) -> Iterator[Tuple[str, int, Optional[ChunkMetadata]]]:
            for fi in infos:
                for chunk_metadata, chunk_size in chunker.generate_chunk_metadatas(
                    fi.path, fi.size, filesystem
                ):
                    yield fi.path, chunk_size, chunk_metadata

        if chunker.reads_file_metadata and self._num_workers > 1:
            yield from make_async_gen(
                base_iterator=file_infos,
                fn=chunk,
                preserve_ordering=preserve_order,
                num_workers=self._num_workers,
                buffer_size=self._queue_size_per_thread,
            )
        else:
            yield from chunk(iter(file_infos))

    def _generate_chunk_records_lazy(
        self,
        file_infos: Iterable[FileInfo],
        filesystem: "FileSystem",
        target_rows: int,
    ) -> Iterator[Tuple[str, int, Optional[ChunkMetadata]]]:
        """Target-aware footer enumeration.

        Algorithm (mirrors bench_lazy_enumerate.py's PyArrow path):

        1. Materialize the pruned file iterator; if zero files, stop.
        2. Read file[0]'s footer to calibrate ``avg_rows_per_file``. If it
           already covers ``target_rows``, emit + trim and return.
        3. Estimate ``files_needed = ceil(target / avg * over_estimate)`` and
           parallel-fetch footers for ``files[1:files_needed]`` with a fat
           thread pool (64 by default -- footer reads are I/O bound).
        4. If running row sum still undershoots, top up with another bulk
           fetch sized to the residual.
        5. On overshoot at the last accepted chunk, stamp ``max_emit_rows``
           so the reader trims to exactly ``target_rows`` -- no downstream
           ``Limit`` op required.

        Emits ``(path, on_disk_size, ParquetFileChunkMetadata)`` tuples in
        the same shape as ``_generate_chunk_records``.
        """
        # First entry into the lazy path on this process bumps PyArrow's
        # io thread pool so the per-thread footer reads aren't serialized
        # behind a tiny internal pool.
        _ensure_pyarrow_io_thread_count()
        chunker = self._file_chunker
        t_materialize = time.perf_counter()
        files: List[FileInfo] = list(file_infos)
        materialize_s = time.perf_counter() - t_materialize
        if not files:
            return

        accepted: List[Tuple[str, int, Optional[ChunkMetadata]]] = []
        running_rows = 0
        num_threads = max(self._DEFAULT_LAZY_FOOTER_THREADS, 1)
        phase_a_s = 0.0
        phase_b_s = 0.0
        phase_c_s = 0.0
        phase_d_s = 0.0
        phase_c_iters = 0

        def fetch_one(
            fi: FileInfo,
        ) -> List[Tuple[str, int, Optional[ChunkMetadata]]]:
            return [
                (fi.path, chunk_size, meta)
                for meta, chunk_size in chunker.generate_chunk_metadatas(
                    fi.path, fi.size, filesystem
                )
            ]

        def absorb(records: List[Tuple[str, int, Optional[ChunkMetadata]]]) -> None:
            """Append a file's chunks and accumulate their row counts."""
            nonlocal running_rows
            for rec in records:
                accepted.append(rec)
                meta = rec[2]
                if isinstance(meta, dict) and "num_rows" in meta:
                    running_rows += int(meta["num_rows"])

        # Phase A: calibrate on file[0]. ``running_rows`` after this absorb
        # is the average we extrapolate from -- 0 only when the file had no
        # rows at all (truly empty parquet), in which case we can't size
        # the bulk fetch and fall back to the streaming pipeline.
        t_a = time.perf_counter()
        absorb(fetch_one(files[0]))
        phase_a_s = time.perf_counter() - t_a
        if running_rows == 0:
            yield from accepted
            yield from self._generate_chunk_records(
                iter(files[1:]), filesystem, preserve_order=False
            )
            return
        avg_rows_per_file = float(running_rows)
        if running_rows >= target_rows:
            # First file already covers the target.
            t_d = time.perf_counter()
            self._trim_to_target(accepted, target_rows)
            phase_d_s = time.perf_counter() - t_d
            self._maybe_log_lazy_profile(
                len(files),
                1,
                len(accepted),
                target_rows,
                running_rows,
                materialize_s,
                phase_a_s,
                phase_b_s,
                phase_c_s,
                phase_c_iters,
                phase_d_s,
                num_threads,
            )
            yield from accepted
            return

        # Phase B: bulk-fetch ceil(target / avg * over_estimate) more files.
        cursor = 1
        files_needed = min(
            math.ceil(target_rows / avg_rows_per_file * _LAZY_OVER_ESTIMATE),
            len(files),
        )
        if files_needed > cursor:
            t_b = time.perf_counter()
            with ThreadPoolExecutor(max_workers=num_threads) as ex:
                for records in ex.map(fetch_one, files[cursor:files_needed]):
                    absorb(records)
            phase_b_s = time.perf_counter() - t_b
            cursor = files_needed

        # Phase C: top-up loop -- only fires when our estimate undershot.
        t_c = time.perf_counter()
        while running_rows < target_rows and cursor < len(files):
            residual = target_rows - running_rows
            more = max(1, math.ceil(residual / avg_rows_per_file * _LAZY_OVER_ESTIMATE))
            end = min(cursor + more, len(files))
            with ThreadPoolExecutor(max_workers=num_threads) as ex:
                for records in ex.map(fetch_one, files[cursor:end]):
                    absorb(records)
            cursor = end
            phase_c_iters += 1
        phase_c_s = time.perf_counter() - t_c

        # Phase D: stamp ``max_emit_rows`` on the boundary and drop chunks
        # past it so the manifest carries exactly ``target_rows``.
        t_d = time.perf_counter()
        self._trim_to_target(accepted, target_rows)
        phase_d_s = time.perf_counter() - t_d
        self._maybe_log_lazy_profile(
            len(files),
            cursor,
            len(accepted),
            target_rows,
            running_rows,
            materialize_s,
            phase_a_s,
            phase_b_s,
            phase_c_s,
            phase_c_iters,
            phase_d_s,
            num_threads,
        )
        yield from accepted

    @staticmethod
    def _maybe_log_lazy_profile(
        n_files: int,
        n_footers_read: int,
        n_chunks: int,
        target_rows: int,
        running_rows: int,
        materialize_s: float,
        phase_a_s: float,
        phase_b_s: float,
        phase_c_s: float,
        phase_c_iters: int,
        phase_d_s: float,
        num_threads: int,
    ) -> None:
        """Emit a single ``LISTING_PROF`` line when the env flag is set.

        Mirrors v3_map_task's ``SHUFFLE_PROF`` format so the same parser
        can aggregate across listing + map tasks. Printed once per
        ListFiles task; cheap when the flag is off (env read at import).
        """
        if not _LIST_FILES_PROFILE:
            return
        total = materialize_s + phase_a_s + phase_b_s + phase_c_s + phase_d_s
        print(
            f"LISTING_PROF target_rows={target_rows} running_rows={running_rows} "
            f"files_total={n_files} footers_read={n_footers_read} chunks={n_chunks} "
            f"threads={num_threads} total_s={total:.3f} "
            f"materialize_s={materialize_s:.3f} phase_a_s={phase_a_s:.3f} "
            f"phase_b_s={phase_b_s:.3f} phase_c_s={phase_c_s:.3f} "
            f"phase_c_iters={phase_c_iters} phase_d_s={phase_d_s:.3f}",
            flush=True,
        )

    @staticmethod
    def _trim_to_target(
        accepted: List[Tuple[str, int, Optional[ChunkMetadata]]],
        target_rows: int,
    ) -> None:
        """Trim ``accepted`` (in place) to cover exactly ``target_rows``.

        Walks chunks in insertion order, accumulating ``num_rows``. The
        boundary chunk -- the first one whose running total reaches or
        exceeds ``target_rows`` -- gets ``max_emit_rows`` stamped with the
        residual so the reader emits exactly the right slice. Everything
        after the boundary is removed, so the manifest never carries
        over-fetched chunks downstream.
        """
        running = 0
        for idx, (_path, _size, meta) in enumerate(accepted):
            if not isinstance(meta, dict) or "num_rows" not in meta:
                continue
            num_rows = int(meta["num_rows"])
            if running + num_rows < target_rows:
                running += num_rows
                continue
            residual = target_rows - running
            if residual < num_rows:
                meta["max_emit_rows"] = residual
            del accepted[idx + 1 :]
            return

    def _batch_chunk_records_to_manifests(
        self,
        chunk_records: Iterable[Tuple[str, int, Optional[ChunkMetadata]]],
    ) -> Iterable[FileManifest]:
        """Batch chunk records into ``FileManifest`` blocks of bounded size."""
        running_paths: List[str] = []
        running_file_sizes: List[int] = []
        running_chunk_metadatas: List[Optional[ChunkMetadata]] = []
        manifests_count = 0
        chunks_count = 0

        for path, chunk_size, chunk_metadata in chunk_records:
            running_paths.append(path)
            running_file_sizes.append(chunk_size)
            running_chunk_metadatas.append(chunk_metadata)
            chunks_count += 1

            if len(running_paths) >= self._max_paths_per_output:
                manifests_count += 1
                yield FileManifest.construct_manifest(
                    running_paths,
                    running_file_sizes,
                    running_chunk_metadatas,
                )
                running_paths = []
                running_file_sizes = []
                running_chunk_metadatas = []

        if running_paths:
            manifests_count += 1
            yield FileManifest.construct_manifest(
                running_paths,
                running_file_sizes,
                running_chunk_metadatas,
            )

        logger.debug(
            f"Listing files: constructed {manifests_count} manifests "
            f"with {chunks_count} file chunks"
        )
