"""Tests for :class:`LimitAwareFilePartitioner`.

The wrapper trims the chunk stream so the inner partitioner only sees the
chunks needed to satisfy a pushed-down scanner limit. Tests focus on:

- Cutoff at the boundary chunk (atomic admission, no slicing chunks).
- Idempotent no-op when the running total never reaches the limit.
- Defensive forwarding of chunks without ``num_rows`` (non-Parquet
  chunkers).
- Forwarding ``finalize`` so the inner partitioner still flushes its
  buffer.
- Constructor validation.
"""
import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)
from ray.data._internal.datasource_v2.partitioners.file_affinity_partitioner import (
    FileAffinityPartitioner,
)
from ray.data._internal.datasource_v2.partitioners.file_partitioner import (
    FilePartitioner,
)
from ray.data._internal.datasource_v2.partitioners.limit_aware_partitioner import (
    LimitAwareFilePartitioner,
)


def _make_manifest(rows_per_chunk, file_size=100):
    """Build a 1-file-per-chunk FileManifest with the given per-chunk rows."""
    n = len(rows_per_chunk)
    return FileManifest(
        pa.Table.from_pydict(
            {
                PATH_COLUMN_NAME: [f"f{i}.parquet" for i in range(n)],
                FILE_SIZE_COLUMN_NAME: [file_size] * n,
                FILE_CHUNK_METADATA_COLUMN_NAME: [
                    {
                        "row_group_start": 0,
                        "row_group_end": 1,
                        "in_memory_size": 1000,
                        "num_rows": r,
                        # Default = emit every row in the chunk; matches the
                        # chunker's behavior. Boundary chunk gets overwritten.
                        "max_emit_rows": r,
                    }
                    for r in rows_per_chunk
                ],
            }
        )
    )


class _RecordingPartitioner(FilePartitioner):
    """Inner stub that records every ``add_input`` it sees."""

    def __init__(self):
        self.received = []  # list[list[int]] -- num_rows per call
        self.finalized = False

    def add_input(self, manifest):
        rows = [
            md["num_rows"] if md is not None else None
            for md in manifest.file_chunk_metadatas
        ]
        self.received.append(rows)

    def has_partition(self):
        return False

    def next_partition(self):
        raise RuntimeError("stub partitioner does not produce partitions")

    def finalize(self):
        self.finalized = True


def test_limit_aware_admits_boundary_chunk_and_stops():
    """Boundary chunk (the one whose addition reaches ``limit_rows``) is
    admitted in full; chunks beyond it are dropped.

    Manifest rows: [100, 100, 100, 100, 100], limit=250.
    First two chunks total 200 < 250; third chunk reaches 300 >= 250, so it is
    the boundary and is admitted. Chunks 4 and 5 are dropped.
    """
    inner = _RecordingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=250)

    wrapper.add_input(_make_manifest([100, 100, 100, 100, 100]))

    # Sliced to first 3 chunks.
    assert inner.received == [[100, 100, 100]]


def test_limit_aware_admits_all_when_total_below_limit():
    """When the entire manifest fits within ``limit_rows``, the wrapper
    forwards it unchanged (no slicing, no extra accounting)."""
    inner = _RecordingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=1000)

    wrapper.add_input(_make_manifest([100, 100, 100]))

    assert inner.received == [[100, 100, 100]]


def test_limit_aware_drops_inputs_after_limit_reached():
    """After the limit is satisfied in one ``add_input`` call, subsequent
    calls are no-ops — the inner partitioner sees nothing more."""
    inner = _RecordingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=150)

    wrapper.add_input(_make_manifest([100, 100, 100]))
    wrapper.add_input(_make_manifest([100, 100]))
    wrapper.add_input(_make_manifest([100]))

    # Only the first 2 chunks of the first call are admitted (100 + 100 >= 150
    # at idx=1).
    assert inner.received == [[100, 100]]


def test_limit_aware_spans_boundary_across_multiple_inputs():
    """Cumulative accounting carries over between ``add_input`` calls."""
    inner = _RecordingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=250)

    # First call: 100 + 100 = 200; nothing yet sliced.
    wrapper.add_input(_make_manifest([100, 100]))
    # Second call: running was 200; first chunk pushes it to 300 (boundary).
    wrapper.add_input(_make_manifest([100, 100]))

    assert inner.received == [[100, 100], [100]]


def test_limit_aware_passes_through_chunks_without_num_rows():
    """Non-Parquet chunkers (or unchunked whole-file reads) yield metadata
    that lacks ``num_rows``. The wrapper must not block on them — it
    treats their row contribution as 0 (degrades to a no-op).
    """
    inner = _RecordingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=100)

    # Manifest with ``None`` chunk metadata (whole-file read).
    none_manifest = FileManifest(
        pa.Table.from_pydict(
            {
                PATH_COLUMN_NAME: ["whole_file.json"],
                FILE_SIZE_COLUMN_NAME: [12345],
                FILE_CHUNK_METADATA_COLUMN_NAME: [None],
            }
        )
    )
    wrapper.add_input(none_manifest)
    # Running total still 0 — nothing should be sliced; all forwarded.
    assert len(inner.received) == 1
    assert inner.received[0] == [None]


def test_limit_aware_finalize_forwards_to_inner():
    """Finalize should always reach the inner partitioner so any buffered
    state is flushed (e.g., FileAffinityPartitioner's open buckets)."""
    inner = _RecordingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=50)

    wrapper.add_input(_make_manifest([100]))
    wrapper.finalize()

    assert inner.finalized is True


def test_limit_aware_rejects_non_positive_limit():
    inner = _RecordingPartitioner()
    with pytest.raises(ValueError):
        LimitAwareFilePartitioner(inner=inner, limit_rows=0)
    with pytest.raises(ValueError):
        LimitAwareFilePartitioner(inner=inner, limit_rows=-1)


def test_limit_aware_integrates_with_file_affinity_partitioner():
    """End-to-end: limit-aware wrapper around the default
    ``FileAffinityPartitioner``. Verifies the wrapper composes cleanly
    (no signature drift, no surprise interactions) — concrete byte-level
    bucket layout is tested in ``test_file_partitioners``."""
    inner = FileAffinityPartitioner(max_rows_per_partition=200)
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=150)

    wrapper.add_input(_make_manifest([100, 100, 100, 100, 100]))
    wrapper.finalize()

    partitions = []
    while wrapper.has_partition():
        partitions.append(wrapper.next_partition())

    # The wrapper trimmed input to first 2 chunks (100 + 100 >= 150 at idx=1),
    # so FileAffinity sees only those two paths.
    all_paths = []
    for p in partitions:
        all_paths.extend(p.paths.tolist())
    assert set(all_paths) == {"f0.parquet", "f1.parquet"}


class _CapturingPartitioner(FilePartitioner):
    """Inner stub that lets us read each chunk's metadata after the wrapper
    finishes — so we can assert that ``max_emit_rows`` is stamped on the
    boundary chunk (and only that chunk)."""

    def __init__(self):
        self.received_mds = []

    def add_input(self, manifest):
        # Snapshot of each chunk's metadata at admit time. The wrapper
        # mutates the boundary chunk's dict in-place, so we copy.
        for md in manifest.file_chunk_metadatas:
            self.received_mds.append(dict(md) if md is not None else None)

    def has_partition(self):
        return False

    def next_partition(self):
        raise RuntimeError("not used")

    def finalize(self):
        pass


def test_boundary_chunk_gets_max_emit_rows_stamped():
    """The chunk whose admission crosses ``limit_rows`` carries
    ``max_emit_rows = limit_rows - rows_admitted_before_it``."""
    inner = _CapturingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=250)

    wrapper.add_input(_make_manifest([100, 100, 100, 100, 100]))

    # First 3 chunks admitted: total 300 >= 250, boundary = idx 2.
    assert len(inner.received_mds) == 3
    # Non-boundary chunks keep their default max_emit_rows = num_rows.
    assert inner.received_mds[0]["max_emit_rows"] == 100
    assert inner.received_mds[1]["max_emit_rows"] == 100
    # Boundary chunk: max_emit = 250 - (100 + 100) = 50.
    assert inner.received_mds[2]["max_emit_rows"] == 50


def test_boundary_max_emit_when_first_chunk_already_exceeds_limit():
    """When the very first chunk already covers the limit, its max_emit
    is the full ``limit_rows`` (no prior chunks to subtract)."""
    inner = _CapturingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=10)

    wrapper.add_input(_make_manifest([100]))

    assert len(inner.received_mds) == 1
    assert inner.received_mds[0]["max_emit_rows"] == 10


def test_boundary_max_emit_carries_across_multiple_inputs():
    """The wrapper accumulates ``rows_so_far`` between ``add_input`` calls,
    so the boundary's ``max_emit_rows`` reflects the running total."""
    inner = _CapturingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=250)

    # First call: 100+100=200, no boundary yet.
    wrapper.add_input(_make_manifest([100, 100]))
    # Second call: starts at 200; first chunk pushes to 300 (boundary at
    # idx=0 of THIS call). max_emit = 250 - 200 = 50.
    wrapper.add_input(_make_manifest([100, 100]))

    # 2 chunks from call 1, 1 chunk from call 2 = 3 total admitted.
    assert len(inner.received_mds) == 3
    assert inner.received_mds[0]["max_emit_rows"] == 100
    assert inner.received_mds[1]["max_emit_rows"] == 100
    assert inner.received_mds[2]["max_emit_rows"] == 50


def test_tiny_limit_on_huge_dataset_emits_one_task():
    """Corner case: ``limit=1`` against many large chunks. Only the first
    chunk is admitted, with ``max_emit_rows=1``. All subsequent chunks
    are dropped — even when downstream cluster has many idle CPUs.
    This is a deliberate trade-off: limit semantics don't say "fill
    the cluster", they say "produce N rows"."""
    inner = _CapturingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=1)

    # Many large chunks, each with millions of rows.
    wrapper.add_input(_make_manifest([1_000_000] * 100))

    # Only the first chunk admitted.
    assert len(inner.received_mds) == 1
    assert inner.received_mds[0]["max_emit_rows"] == 1


@pytest.mark.parametrize(
    "limit_rows,rows_per_chunk,expected_admitted,expected_max_emit",
    [
        # limit aligns exactly with a chunk boundary -> last admitted
        # chunk's max_emit equals its full num_rows (no real trim).
        (200, [100, 100], 2, 100),
        # limit lands mid-chunk -> last admitted chunk's max_emit is
        # the residual.
        (150, [100, 100], 2, 50),
        # limit equals first chunk's num_rows exactly -> 1 chunk
        # admitted, max_emit = full chunk.
        (100, [100, 100, 100], 1, 100),
        # limit < first chunk -> 1 chunk admitted, max_emit < num_rows.
        (1, [100, 100], 1, 1),
        # limit > total -> all admitted, no boundary; last chunk keeps
        # its default max_emit = num_rows.
        (10_000, [100, 100, 100], 3, 100),
    ],
)
def test_limit_boundary_arithmetic(
    limit_rows, rows_per_chunk, expected_admitted, expected_max_emit
):
    """Parametrized boundary arithmetic across critical alignments
    (exact-fit / mid-chunk / single-chunk / overshoot)."""
    inner = _CapturingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=limit_rows)
    wrapper.add_input(_make_manifest(rows_per_chunk))

    assert len(inner.received_mds) == expected_admitted
    last_md = inner.received_mds[-1]
    assert last_md["max_emit_rows"] == expected_max_emit


def test_no_stamp_when_total_below_limit():
    """When the cumulative row count never reaches ``limit_rows``, no
    boundary exists and no chunk gets stamped."""
    inner = _CapturingPartitioner()
    wrapper = LimitAwareFilePartitioner(inner=inner, limit_rows=1000)

    wrapper.add_input(_make_manifest([100, 100, 100]))
    wrapper.finalize()

    # Below-limit path forwards manifests unchanged: each chunk keeps its
    # default max_emit_rows = num_rows.
    assert all(
        md["max_emit_rows"] == md["num_rows"] for md in inner.received_mds
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
