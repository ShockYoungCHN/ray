"""Tests for limit pushdown into ``ListFiles.pushed_limit``.

Complements ``test_execution_optimizer_limit_pushdown.py`` (which covers
the broad rule semantics on V1 ``Read``) with V2-specific behavior: when
``LimitPushdownRule`` pushes a limit into a ``ReadFiles`` scanner that
implements ``SupportsLimitPushdown``, it must also mirror the limit onto
the upstream ``ListFiles`` so the indexer's lazy-enumeration path can
trim the manifest stream at listing time.

Avoids spinning up a Ray cluster: exercises the rule helper directly on
constructed logical-op trees.
"""

from dataclasses import dataclass, field, replace
from typing import List, Optional

import pyarrow as pa
import pytest

from ray.data._internal.datasource_v2.logical_optimizers import (
    SupportsLimitPushdown,
)
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import Limit
from ray.data._internal.logical.operators.read_operator import ListFiles, ReadFiles
from ray.data._internal.logical.rules.limit_pushdown import LimitPushdownRule


@dataclass(frozen=True)
class _StubScanner(SupportsLimitPushdown):
    """Bare scanner that records the pushed limit. Avoids needing a real
    arrow/parquet scanner (and its filesystem / pyarrow setup) for the
    pushdown unit test.

    Defaults to a small fixed-width schema (``int64`` column) so the
    CPU-aware bucket-size estimator has a meaningful ``bytes_per_row``
    to multiply by. Tests that don't care about CPU-aware sizing get the
    default; tests that need an empty schema can pass one explicitly.
    """

    limit: Optional[int] = None
    schema: pa.Schema = field(
        default_factory=lambda: pa.schema([pa.field("col0", pa.int64())])
    )

    def push_limit(self, limit: int) -> "_StubScanner":
        new_limit = limit if self.limit is None else min(self.limit, limit)
        return replace(self, limit=new_limit)

    def read_schema(self):
        return self.schema

    def pruned_column_names(self):
        return None


class _StubChunker:
    """Stub chunker that advertises row-count-limit support so the
    ``_can_drop_limit_after_pushdown`` gate evaluates True. The actual
    chunking logic is never invoked in these planning-time unit tests."""

    supports_row_count_limit = True


class _NonRowCountChunker:
    """Stub chunker that does NOT support row-count limit (mimics
    ``WholeFileChunker`` / ``LineDelimitedFileChunker``)."""

    supports_row_count_limit = False


class _StubIndexer:
    def __init__(self, chunker=None):
        self.file_chunker = chunker if chunker is not None else _StubChunker()


def _build_read_above_list(paths=None, *, indexer=None) -> ReadFiles:
    """Construct a ``ReadFiles -> ListFiles`` chain with a stub scanner."""
    list_files = ListFiles(
        paths=paths or ["s3://bucket/data/"],
        file_indexer=indexer if indexer is not None else _StubIndexer(),
        filesystem=None,
        source_paths=paths or ["s3://bucket/data/"],
    )
    read_files = ReadFiles(
        datasource_name="stub_parquet",
        scanner=_StubScanner(),
        schema=pa.schema([]),
        parallelism=-1,
        input_dependencies=[list_files],
    )
    return read_files


def test_pushed_limit_lands_on_list_files():
    """When the rule's per-block helper pushes a limit into a V2
    ``ReadFiles`` whose scanner supports ``SupportsLimitPushdown``, both
    the scanner limit and the upstream ``ListFiles.pushed_limit`` must be
    set."""
    read_op = _build_read_above_list()
    assert read_op.scanner.limit is None
    assert read_op.input_dependencies[0].pushed_limit is None

    rule = LimitPushdownRule()
    new_read = rule._apply_per_block_limit_if_supported(read_op, 1000)

    assert isinstance(new_read, ReadFiles)
    assert new_read.scanner.limit == 1000
    list_files = new_read.input_dependencies[0]
    assert isinstance(list_files, ListFiles)
    assert list_files.pushed_limit == 1000


def test_pushed_limit_is_idempotent_at_same_value():
    """Running the rule twice with the same limit must not produce a new
    object (fixed-point convergence guarantee)."""
    read_op = _build_read_above_list()
    rule = LimitPushdownRule()

    once = rule._apply_per_block_limit_if_supported(read_op, 500)
    list_files_once = once.input_dependencies[0]

    twice = rule._apply_per_block_limit_if_supported(once, 500)
    list_files_twice = twice.input_dependencies[0]

    # Same value → unchanged identity on the upstream listing.
    assert list_files_once is list_files_twice
    assert list_files_twice.pushed_limit == 500


def test_pushed_limit_tightens_to_smaller_value():
    """A subsequent push with a smaller limit replaces the cap. (A larger
    one would be redundant — covered by the scanner's own ``push_limit``
    semantics, which takes the min.)"""
    read_op = _build_read_above_list()
    rule = LimitPushdownRule()

    new_read = rule._apply_per_block_limit_if_supported(read_op, 1000)
    new_read2 = rule._apply_per_block_limit_if_supported(new_read, 100)

    list_files = new_read2.input_dependencies[0]
    assert list_files.pushed_limit == 100
    assert new_read2.scanner.limit == 100


def test_pushed_limit_no_op_when_upstream_not_list_files():
    """When ``ReadFiles`` sits over a non-``ListFiles`` dependency (e.g.,
    an in-memory source), the helper returns the read op unchanged on the
    list side — only the scanner gets the limit. Defensive: ensures the
    stamp helper never crashes on non-V2 upstreams."""

    @dataclass(frozen=True)
    class _OtherSource(LogicalOperator):
        _input_dependencies: List[LogicalOperator] = field(default_factory=list)

        def __post_init__(self):
            object.__setattr__(self, "_name", "OtherSource")

        @property
        def num_outputs(self):
            return None

    other = _OtherSource()
    read_op = ReadFiles(
        datasource_name="stub_parquet",
        scanner=_StubScanner(),
        schema=pa.schema([]),
        parallelism=-1,
        input_dependencies=[other],
    )

    rule = LimitPushdownRule()
    new_read = rule._apply_per_block_limit_if_supported(read_op, 500)

    # Scanner side still gets the limit pushed.
    assert new_read.scanner.limit == 500
    # No upstream ``ListFiles`` to stamp -> dependency identity preserved.
    assert new_read.input_dependencies[0] is other


def test_direct_read_limit_triggers_pushdown_via_apply():
    """The most common user pattern ``read_parquet().limit(N)`` puts the
    Limit op directly above ReadFiles (no num-rows-preserving ops in
    between). Verifies the fixed early-return path: ``_push_limit_down``
    must still push the limit into the scanner."""
    list_files = ListFiles(
        paths=["s3://bucket/data/"],
        file_indexer=None,
        filesystem=None,
        source_paths=["s3://bucket/data/"],
    )
    read_files = ReadFiles(
        datasource_name="stub_parquet",
        scanner=_StubScanner(),
        schema=pa.schema([]),
        parallelism=-1,
        input_dependencies=[list_files],
    )
    limit_op = Limit(1000, input_dependencies=[read_files])

    rule = LimitPushdownRule()
    result = rule._push_limit_down(limit_op)

    # Result should be either Limit(...) or ReadFiles directly (depending
    # on whether LimitOp deletion fires). Either way the scanner inside
    # the ReadFiles subtree must have absorbed the limit.
    def _find_read_files(op):
        if isinstance(op, ReadFiles):
            return op
        for dep in op.input_dependencies:
            found = _find_read_files(dep)
            if found is not None:
                return found
        return None

    rf = _find_read_files(result)
    assert rf is not None
    assert rf.scanner.limit == 1000
    assert rf.input_dependencies[0].pushed_limit == 1000


def test_limit_op_dropped_when_pushdown_safe():
    """When (a) Limit's input is ReadFiles, (b) scanner supports
    pushdown, (c) upstream is ListFiles, AND (d) the chunker advertises
    row-count-limit support, the Limit node becomes redundant: the
    indexer's lazy-enumeration path trims to exact limit at listing time
    and stamps ``max_emit_rows`` on the boundary chunk for per-task
    row-precise emit. Verify the Limit node is dropped from the plan
    tree."""
    list_files = ListFiles(
        paths=["s3://bucket/data/"],
        # ``_StubIndexer`` carries a chunker with
        # ``supports_row_count_limit=True`` — required by the chunker
        # capability gate in ``_can_drop_limit_after_pushdown``.
        file_indexer=_StubIndexer(),
        filesystem=None,
        source_paths=["s3://bucket/data/"],
    )
    read_files = ReadFiles(
        datasource_name="stub_parquet",
        scanner=_StubScanner(),
        schema=pa.schema([]),
        parallelism=-1,
        input_dependencies=[list_files],
    )
    limit_op = Limit(500, input_dependencies=[read_files])

    rule = LimitPushdownRule()
    result = rule._push_limit_down(limit_op)

    # Limit node should NOT appear in the result tree.
    def _has_limit(op):
        if isinstance(op, Limit):
            return True
        return any(_has_limit(dep) for dep in op.input_dependencies)

    assert not _has_limit(
        result
    ), "Limit node should be dropped when pushdown is row-precise"
    assert isinstance(result, ReadFiles)
    assert result.scanner.limit == 500


def test_pushdown_stamps_cpu_aware_row_cap(monkeypatch):
    """When pushdown fires AND the cluster CPU count is observable,
    ListFiles.pushed_max_rows_per_partition is set to
    limit_rows / (2 * avail_cpus). No bytes intermediate -- row count
    drives sizing directly.

    Mocks ray.cluster_resources so the test doesn't require a real
    Ray cluster.
    """
    import ray

    monkeypatch.setattr(ray, "cluster_resources", lambda: {"CPU": 64.0})

    read_op = _build_read_above_list()
    rule = LimitPushdownRule()
    # 100M rows / (2 * 64) = 781_250 rows per partition.
    new_read = rule._apply_per_block_limit_if_supported(read_op, 100_000_000)

    list_files = new_read.input_dependencies[0]
    assert list_files.pushed_limit == 100_000_000
    assert list_files.pushed_max_rows_per_partition == 100_000_000 // 128


def test_pushdown_falls_back_to_no_row_cap_when_cpus_unknown(monkeypatch):
    """If ray.cluster_resources raises (e.g. driver not joined to Ray),
    the row cap override silently degrades to None -- the partitioner
    keeps its default sizing. Limit-aware path still activates; we just
    lose the CPU-aware alignment."""
    import ray

    def _boom():
        raise RuntimeError("no ray here")

    monkeypatch.setattr(ray, "cluster_resources", _boom)

    read_op = _build_read_above_list()
    rule = LimitPushdownRule()
    new_read = rule._apply_per_block_limit_if_supported(read_op, 500)

    list_files = new_read.input_dependencies[0]
    assert list_files.pushed_limit == 500
    assert list_files.pushed_max_rows_per_partition is None


def test_limit_op_preserved_when_chunker_lacks_row_count_support():
    """Corner case: non-Parquet chunkers (WholeFile / LineDelimited /
    ByteEstimate) don't stamp ``num_rows`` on chunk metadata, so the
    indexer's lazy-enumeration path can't pick a boundary and
    ``max_emit_rows`` is never set. Deleting the Limit op in that state
    would silently allow multi-task over-emission (each task capped at
    per-task ``scanner.limit`` rather than at a chunk-derived budget).
    The gate must keep Limit alive."""
    list_files = ListFiles(
        paths=["s3://bucket/data/"],
        file_indexer=_StubIndexer(chunker=_NonRowCountChunker()),
        filesystem=None,
        source_paths=["s3://bucket/data/"],
    )
    read_files = ReadFiles(
        datasource_name="stub_arrow",
        scanner=_StubScanner(),
        schema=pa.schema([]),
        parallelism=-1,
        input_dependencies=[list_files],
    )
    limit_op = Limit(500, input_dependencies=[read_files])

    rule = LimitPushdownRule()
    result = rule._push_limit_down(limit_op)

    # Scanner pushdown still happens (per-task cap is good to have).
    def _find_read_files(op):
        if isinstance(op, ReadFiles):
            return op
        for dep in op.input_dependencies:
            found = _find_read_files(dep)
            if found is not None:
                return found
        return None

    rf = _find_read_files(result)
    assert rf is not None
    assert rf.scanner.limit == 500

    # But Limit MUST stay (otherwise per-task ≤ 500 across M tasks could
    # sum to > 500).
    def _has_limit(op):
        if isinstance(op, Limit):
            return True
        return any(_has_limit(dep) for dep in op.input_dependencies)

    assert _has_limit(result), (
        "Limit must be preserved when chunker can't enforce a per-task " "row budget"
    )


def test_limit_op_preserved_when_scanner_does_not_support_pushdown():
    """When the scanner doesn't implement ``SupportsLimitPushdown``, no
    pushdown happens and the Limit op must stay (otherwise rows are
    silently un-capped)."""

    @dataclass(frozen=True)
    class _NonPushdownScanner:
        """Scanner without SupportsLimitPushdown — push_limit not impl."""

        def read_schema(self):
            return pa.schema([])

        def pruned_column_names(self):
            return None

    list_files = ListFiles(
        paths=["s3://bucket/data/"],
        file_indexer=None,
        filesystem=None,
        source_paths=["s3://bucket/data/"],
    )
    read_files = ReadFiles(
        datasource_name="stub_no_pushdown",
        scanner=_NonPushdownScanner(),
        schema=pa.schema([]),
        parallelism=-1,
        input_dependencies=[list_files],
    )
    limit_op = Limit(500, input_dependencies=[read_files])

    rule = LimitPushdownRule()
    result = rule._push_limit_down(limit_op)

    # Should return the original limit_op (no pushdown, no deletion).
    assert isinstance(result, Limit)
    assert result.limit == 500


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
