import copy
import logging
from dataclasses import is_dataclass, replace
from typing import List, Optional

from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators import (
    AbstractMap,
    AbstractOneToOne,
    Download,
    Limit,
    Read,
    ReadFiles,
    Union,
)

__all__ = [
    "LimitPushdownRule",
]


logger = logging.getLogger(__name__)


class LimitPushdownRule(Rule):
    """Rule for pushing down the limit operator.

    When a limit operator is present, we apply the limit on the
    most upstream operator that supports it. We are conservative and only
    push through operators that we know for certain do not modify row counts:
    - Project operations (column selection)
    - MapRows operations (row-wise transformations that preserve row count)
    - Union operations (limits are prepended to each branch)

    We stop at:
    - Any operator that can modify the number of output rows (Sort, Shuffle, Aggregate, Read etc.)

    For per-block limiting, we also set per-block limits on Read operators to optimize
    I/O while keeping the Limit operator for exact row count control.

    In addition, we also fuse consecutive Limit operators into a single
    Limit operator, i.e. `Limit[n] -> Limit[m]` becomes `Limit[min(n, m)]`.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        # Snapshot cluster CPU count once per apply() so plan reproducibility
        # isn't subject to autoscale flicker mid-optimization (and so we
        # don't re-issue ray.cluster_resources() per Limit node).
        self._avail_cpus_snapshot = _try_get_avail_cpus()

        # The DAG's root is the most downstream operator.
        def transform(node: LogicalOperator) -> LogicalOperator:
            if isinstance(node, Limit):
                # First, try to fuse with upstream Limit if possible (reuse fusion logic)
                upstream_op = node.input_dependencies[0]
                if isinstance(upstream_op, Limit):
                    # Fuse consecutive Limits: Limit[n] -> Limit[m] becomes Limit[min(n,m)]
                    new_limit = min(node.limit, upstream_op.limit)
                    return Limit(
                        new_limit,
                        input_dependencies=[upstream_op.input_dependencies[0]],
                    )

                # If no fusion, apply pushdown logic
                if isinstance(upstream_op, Union):
                    return self._push_limit_into_union(node)
                else:
                    return self._push_limit_down(node)

            return node

        optimized_dag = plan.dag._apply_transform(transform)
        return LogicalPlan(dag=optimized_dag, context=plan.context)

    def _get_avail_cpus_cached(self) -> Optional[float]:
        """Return the apply()-time cluster CPU snapshot; lazy-init for unit
        tests that invoke helpers directly without going through apply()."""
        snap = getattr(self, "_avail_cpus_snapshot", None)
        if snap is None:
            snap = _try_get_avail_cpus()
            self._avail_cpus_snapshot = snap
        return snap

    def _push_limit_into_union(self, limit_op: Limit) -> Limit:
        """Push `limit_op` INTO every branch of its upstream Union
        and preserve the global limit.

        Existing topology:
            child₁ , child₂ , …  ->  Union  ->  Limit

        New topology:
            child₁ -> Limit ->│
                               │
            child₂ -> Limit ->┤ Union ──► Limit   (original)
                               │
            …    -> Limit  ->│

        Example (skip duplicate limit on a branch that already has it):
            before:
                child -> Limit(n) -> Union -> Limit(n)
            after:
                child -> Limit(n) -> Union -> Limit(n)   (no extra branch limit inserted)
        """
        union_op = limit_op.input_dependencies[0]
        assert isinstance(union_op, Union)

        def _branch_has_limit(op: LogicalOperator, limit: int) -> bool:
            current = op
            while (
                isinstance(current, AbstractOneToOne)
                and not current.can_modify_num_rows
                and current.input_dependencies
            ):
                if isinstance(current, Limit):
                    return current.limit == limit
                # Safe to use the first dependency: current is one-to-one here.
                current = current.input_dependencies[0]

            return isinstance(current, Limit) and current.limit == limit

        # Insert a branch-local Limit and push it further upstream.
        branch_tails: List[LogicalOperator] = []
        for child in union_op.input_dependencies:
            # Avoid inserting a duplicate Limit on a branch that already has the same
            # limit upstream of row-preserving ops.
            if _branch_has_limit(child, limit_op.limit):
                branch_tails.append(child)
                continue
            raw_limit = Limit(limit_op.limit, input_dependencies=[child])

            if isinstance(raw_limit.input_dependencies[0], Union):
                # This represents the limit operator appended after the union.
                pushed_tail = self._push_limit_into_union(raw_limit)
            else:
                # This represents the operator that takes place of the original limit position.
                pushed_tail = self._push_limit_down(raw_limit)
            branch_tails.append(pushed_tail)

        new_union = Union(branch_tails)
        return Limit(limit_op.limit, input_dependencies=[new_union])

    def _push_limit_down(self, limit_op: Limit) -> LogicalOperator:
        """Push a single Limit down through compatible operators conservatively.

        Two independent decisions:
          1. Walk through row-preserving ops above the source, collecting
             them for re-creation below the new Limit position.
          2. Ask the source to absorb the limit (per-block / scanner-level).

        Build the chain bottom-up: source -> (optional Limit) -> preserving.
        The Limit node is elided when the source guarantees row-precise
        emit (see ``_can_drop_limit_after_pushdown``).

        Returns ``limit_op`` unchanged only when neither decision did
        anything (no preserving ops AND the source didn't absorb).
        Creates new operator instances; never mutates existing ones.
        """
        source_op = limit_op.input_dependencies[0]
        preserving: List[LogicalOperator] = []
        while self._is_row_preserving(source_op, limit_op.limit):
            preserving.append(source_op)
            source_op = source_op.input_dependencies[0]

        pushed, absorbed, precise = self._apply_pushdown(source_op, limit_op.limit)

        if not preserving and not absorbed:
            return limit_op

        head: LogicalOperator = (
            pushed if precise else Limit(limit_op.limit, input_dependencies=[pushed])
        )
        for op in reversed(preserving):
            head = self._recreate_operator_with_new_input(op, head)
        return head

    def _is_row_preserving(self, op: LogicalOperator, limit: int) -> bool:
        """True when ``op`` definitely doesn't change row counts and is safe
        to walk past during pushdown.

        Stops on batch-based maps whose ``min_rows_per_bundled_input``
        exceeds the limit (those need a batch larger than the limit to
        produce stable outputs).
        """
        if not (isinstance(op, AbstractOneToOne) and not op.can_modify_num_rows):
            return False
        if isinstance(op, AbstractMap):
            min_rows = op.min_rows_per_bundled_input
            if min_rows is not None and min_rows > limit:
                logger.info(
                    f"Skipping push down of limit {limit} through map "
                    f"{op} because it requires {min_rows} rows to produce "
                    f"stable outputs"
                )
                return False
        return True

    def _apply_pushdown(
        self, op: LogicalOperator, limit: int
    ) -> "tuple[LogicalOperator, bool, bool]":
        """Apply per-block / scanner pushdown to ``op``.

        Returns ``(new_op, absorbed, precise)``:
          - ``absorbed``: the source took the limit (new_op is a new instance).
          - ``precise``: per-task emit is row-precise so the Limit node can
            be dropped from the plan (see ``_can_drop_limit_after_pushdown``).
        """
        new_op = self._apply_per_block_limit_if_supported(op, limit)
        if new_op is op:
            return op, False, False
        precise = _can_drop_limit_after_pushdown(
            original=op, pushed=new_op, limit=limit
        )
        return new_op, True, precise

    def _apply_per_block_limit_if_supported(
        self, op: LogicalOperator, limit: int
    ) -> LogicalOperator:
        """Apply per-block limit to operators that support it."""
        if isinstance(op, AbstractMap):
            if is_dataclass(op):
                if isinstance(op, Read):
                    return replace(
                        op,
                        per_block_limit=limit,
                        num_outputs=op.num_outputs,
                    )
                if isinstance(op, ReadFiles):
                    from ray.data._internal.datasource_v2.logical_optimizers import (
                        SupportsLimitPushdown,
                    )

                    if isinstance(op.scanner, SupportsLimitPushdown):
                        new_op = replace(
                            op,
                            scanner=op.scanner.push_limit(limit),
                        )
                        # Mirror the pushed limit onto the upstream
                        # ListFiles so its physical planner forwards it to
                        # the indexer's lazy-enumeration path; that stops
                        # reading footers once the running rows cover the
                        # limit, avoiding read-task dispatch for chunks
                        # whose rows the downstream Limit would discard.
                        # Compute a per-task row cap so each bucket lands
                        # ~1-2 tasks per CPU; when the read specifies an
                        # explicit parallelism, honor it instead of the
                        # CPU default.
                        explicit_partitions = (
                            new_op.parallelism if new_op.parallelism > 0 else None
                        )
                        if (
                            explicit_partitions is not None
                            and explicit_partitions > limit
                        ):
                            # row_cap floors to 1, so the partitioner
                            # produces at most `limit` partitions (and
                            # often fewer once Parquet row-group
                            # granularity collapses chunks into a single
                            # task). Surface the mismatch so users don't
                            # silently get fewer tasks than requested.
                            logger.info(
                                f"read parallelism={explicit_partitions} "
                                f"exceeds limit={limit}; effective read "
                                f"task count will be at most {limit} "
                                f"(possibly fewer due to Parquet row-"
                                f"group granularity)."
                            )
                        # Resolve the effective num_partitions: explicit
                        # parallelism takes priority; CPU-default falls back
                        # to 2 * avail_cpus. row_cap = ceil(limit /
                        # num_partitions). Both values get stamped so
                        # plan_list_files_op can wire the partitioner into
                        # adaptive cross-file mode (otherwise the file-count
                        # floor would cap the actual task count at the
                        # number of files touched).
                        target_num_partitions = _resolve_num_partitions(
                            explicit_partitions=explicit_partitions,
                            avail_cpus=self._get_avail_cpus_cached(),
                        )
                        target_row_cap = _compute_cpu_aware_row_cap(
                            limit_rows=limit,
                            num_partitions=target_num_partitions,
                        )
                        return _stamp_pushed_limit_on_list_files(
                            new_op,
                            limit=limit,
                            max_rows_per_partition=target_row_cap,
                            num_partitions=target_num_partitions,
                        )
                    return op
                assert len(op.input_dependencies) == 1, len(op.input_dependencies)
                return replace(
                    op,
                    input_dependencies=[op.input_dependencies[0]],
                    per_block_limit=limit,
                )
            new_op = copy.copy(op)
            new_op.set_per_block_limit(limit)
            return new_op
        return op

    def _recreate_operator_with_new_input(
        self, original_op: LogicalOperator, new_input: LogicalOperator
    ) -> LogicalOperator:
        """Create a new operator of the same type as original_op but with new_input as its input."""

        if isinstance(original_op, Limit):
            return Limit(original_op.limit, input_dependencies=[new_input])
        if isinstance(original_op, Download):
            return Download(
                uri_column_names=original_op.uri_column_names,
                output_bytes_column_names=original_op.output_bytes_column_names,
                input_dependencies=[new_input],
                ray_remote_args=original_op.ray_remote_args,
                filesystem=original_op.filesystem,
            )
        if isinstance(original_op, AbstractMap) and is_dataclass(original_op):
            return replace(original_op, input_dependencies=[new_input])

        # Use copy and replace input dependencies approach
        new_op = copy.copy(original_op)
        new_op.input_dependencies = [new_input]
        return new_op


def _resolve_num_partitions(
    *,
    explicit_partitions: Optional[int],
    avail_cpus: Optional[float],
) -> Optional[int]:
    """Pick the effective per-read num_partitions.

    Explicit (``ReadFiles.parallelism > 0`` from ``read_parquet(parallelism
    =...)``) wins. Falls back to ``2 * avail_cpus`` (the "2x CPU" floor for
    straggler tolerance). Returns None when neither source yields a
    positive value -- the partitioner keeps its default sizing.

    Callers pass ``avail_cpus`` from a snapshot taken once per apply() so
    plan reproducibility doesn't depend on autoscale timing.
    """
    if explicit_partitions is not None and explicit_partitions > 0:
        return explicit_partitions
    if avail_cpus is None or avail_cpus <= 0:
        return None
    return max(int(2 * avail_cpus), 1)


def _compute_cpu_aware_row_cap(
    *,
    limit_rows: int,
    num_partitions: Optional[int],
) -> Optional[int]:
    """Per-task row cap: ``limit_rows / num_partitions`` (floored at 1).

    Returns None when num_partitions is unset or non-positive -- the
    partitioner keeps its default sizing.
    """
    if num_partitions is None or num_partitions <= 0:
        return None
    return max(limit_rows // num_partitions, 1)


def _try_get_avail_cpus() -> Optional[float]:
    """Return ``ray.cluster_resources()['CPU']`` if Ray is reachable."""
    try:
        import ray

        return ray.cluster_resources().get("CPU")
    except Exception:
        return None


def _can_drop_limit_after_pushdown(
    *,
    original: LogicalOperator,
    pushed: LogicalOperator,
    limit: int,
) -> bool:
    """Return True iff the per-task emit row count is row-precise after
    pushdown — so the ``Limit`` node can be deleted from the plan.

    Conditions:

    1. The scanner absorbed the pushdown (``pushed`` is a new instance with
       the limit set on its scanner).
    2. The upstream ``ListFiles`` accepted the same pushed limit, which
       triggers the indexer's lazy-enumeration path -- footers are read
       only until the running row count covers the limit, the boundary
       chunk has ``max_emit_rows`` stamped, and chunks past the boundary
       are dropped before reaching the partitioner.
    3. The ``ListFiles.file_indexer.file_chunker`` advertises
       ``supports_row_count_limit = True``. Only chunkers that stamp
       ``num_rows`` + ``max_emit_rows`` on chunk metadata make the
       boundary trim work. Without this, ``Limit`` deletion would silently
       allow multi-task over-emission (each task capped at per-task
       ``scanner.limit`` rather than at a chunk-derived budget).

    Dropping ``Limit`` removes a fusion barrier between ``Read`` and any
    downstream ``ShuffleMap`` / ``Map`` / ``Write``, enabling the standard
    ``OperatorFusionRule`` to fuse across what used to be a pipeline cut.
    """
    from ray.data._internal.logical.operators.read_operator import ListFiles

    if pushed is original:
        return False
    deps = getattr(pushed, "input_dependencies", None)
    if not deps or not isinstance(deps[0], ListFiles):
        return False
    list_files = deps[0]
    # pushed_limit may be tighter than the current `limit` if the rule
    # re-entered with a different value (_stamp_pushed_limit_on_list_files
    # keeps `min(existing, new)`). Tighter pushdown is safe to drop the
    # Limit op against -- per-task emit caps at pushed_limit <= limit, so
    # total output stays within the user-requested bound. Looser
    # (pushed_limit > limit) or absent is not safe; keep the Limit op.
    if list_files.pushed_limit is None or list_files.pushed_limit > limit:
        return False
    indexer = getattr(list_files, "file_indexer", None)
    chunker = getattr(indexer, "file_chunker", None)
    if not getattr(chunker, "supports_row_count_limit", False):
        return False
    return True


def _stamp_pushed_limit_on_list_files(
    read_op: LogicalOperator,
    *,
    limit: int,
    max_rows_per_partition: Optional[int] = None,
    num_partitions: Optional[int] = None,
) -> LogicalOperator:
    """Stamp the pushed row limit + row cap + target partition count onto
    a ReadFiles' upstream ListFiles.

    Re-entrant: keeps the tighter of (existing, new) limit via min(...),
    and short-circuits when nothing would change. The fixed-point
    optimizer can call us multiple times.
    """
    from ray.data._internal.logical.operators.read_operator import ListFiles

    deps = read_op.input_dependencies
    if not deps or not isinstance(deps[0], ListFiles):
        return read_op
    list_files = deps[0]
    existing = list_files.pushed_limit
    # min(existing, limit) keeps the tighter cap if the rule re-enters
    # with a smaller limit on a later pass.
    new_limit = limit if existing is None else min(existing, limit)
    existing_cap = list_files.pushed_max_rows_per_partition
    existing_np = list_files.pushed_num_partitions
    # When the limit tightens we always recompute; when it stays the same
    # we keep whatever was already stamped.
    if existing != new_limit:
        new_cap = max_rows_per_partition
        new_np = num_partitions
    else:
        new_cap = existing_cap
        new_np = existing_np
    if existing == new_limit and existing_cap == new_cap and existing_np == new_np:
        return read_op
    new_list_files = replace(
        list_files,
        pushed_limit=new_limit,
        pushed_max_rows_per_partition=new_cap,
        pushed_num_partitions=new_np,
    )
    return replace(read_op, input_dependencies=[new_list_files])
