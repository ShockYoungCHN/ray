from typing import List

from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.logical.operators import (
    AbstractAllToAll,
    Aggregate,
    RandomizeBlocks,
    RandomShuffle,
    Repartition,
    Sort,
)
from ray.data._internal.planner.aggregate import generate_aggregate_fn
from ray.data._internal.planner.random_shuffle import generate_random_shuffle_fn
from ray.data._internal.planner.randomize_blocks import generate_randomize_blocks_fn
from ray.data._internal.planner.repartition import generate_repartition_fn
from ray.data._internal.planner.sort import generate_sort_fn
from ray.data.context import DataContext, ShuffleStrategy


def _plan_gpu_shuffle_repartition(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.gpu_shuffle.hash_shuffle import GPUShuffleOperator
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.keys).get_columns()

    schema = logical_op.infer_schema()
    columns = list(schema.names) if schema is not None else None

    return GPUShuffleOperator(
        input_physical_op,
        data_context,
        key_columns=tuple(normalized_key_columns),
        columns=columns,
        num_partitions=logical_op.num_outputs,
        should_sort=logical_op.sort,
    )


def _plan_hash_shuffle_repartition(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.execution.operators.hash_shuffle import (
        HashShuffleOperator,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.keys).get_columns()

    return HashShuffleOperator(
        input_physical_op,
        data_context,
        key_columns=tuple(normalized_key_columns),  # noqa: type
        # NOTE: In case number of partitions is not specified, we fall back to
        #       default min parallelism configured
        num_partitions=logical_op.num_outputs,
        should_sort=logical_op.sort,
        # TODO wire in aggregator args overrides
    )


def _plan_hash_shuffle_repartition_v3(
    data_context: DataContext,
    logical_op: Repartition,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    """Plan ``Repartition through the v3 file-transport hash shuffle.

    Returns the ``ShuffleReduceOpV3`` (the downstream root) which wraps the
    ``ShuffleMapOpV3`` as its single input dependency. ``concat_reduce`` is
    used as the reduce_fn — appropriate for repartition (no sort/aggregate
    semantics). ``logical_op.sort=True`` is rejected: a sort-aware reduce
    path is future work.
    """
    from ray.data._internal.arrow_ops.transform_pyarrow import hash_partition
    from ray.data._internal.execution.operators.hash_shuffle_v3 import (
        concat_reduce,
    )
    from ray.data._internal.execution.operators.shuffle_operators.shuffle_map_operator_v3 import (  # noqa: E501
        ShuffleMapOpV3,
    )
    from ray.data._internal.execution.operators.shuffle_operators.shuffle_reduce_operator_v3 import (  # noqa: E501
        ShuffleReduceOpV3,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    if logical_op.sort:
        raise NotImplementedError(
            "use_hash_shuffle_v3=True does not yet support sorted "
            "repartition (logical_op.sort=True). Disable the flag or use "
            "ds.sort() separately."
        )

    num_partitions = logical_op.num_outputs
    if num_partitions is None or num_partitions <= 0:
        num_partitions = data_context.default_hash_shuffle_parallelism

    # When the user supplies hash keys, partition on those; otherwise hash
    # on the full row (consistent with v2's HashShuffleOperator behavior
    # for keyed repartition, and a stable fallback for keyless).
    key_cols = tuple(SortKey(logical_op.keys).get_columns()) if logical_op.keys else ()

    def _partition_fn(table):
        cols = list(key_cols) if key_cols else list(table.column_names)
        return hash_partition(
            table, hash_cols=cols, num_partitions=num_partitions
        )

    map_op = ShuffleMapOpV3(
        input_physical_op,
        data_context,
        num_partitions=num_partitions,
        partition_fn=_partition_fn,
    )
    reduce_op = ShuffleReduceOpV3(
        map_op,
        data_context,
        num_partitions=num_partitions,
        reduce_fn=concat_reduce,
    )
    return reduce_op


def _plan_hash_shuffle_aggregate(
    data_context: DataContext,
    logical_op: Aggregate,
    input_physical_op: PhysicalOperator,
) -> PhysicalOperator:
    from ray.data._internal.execution.operators.hash_aggregate import (
        HashAggregateOperator,
    )
    from ray.data._internal.planner.exchange.sort_task_spec import SortKey

    normalized_key_columns = SortKey(logical_op.key).get_columns()

    return HashAggregateOperator(
        data_context,
        input_physical_op,
        key_columns=tuple(normalized_key_columns),  # noqa: type
        aggregation_fns=tuple(logical_op.aggs),  # noqa: type
        # NOTE: In case number of partitions is not specified, we fall back to
        #       default min parallelism configured
        num_partitions=logical_op.num_partitions,
        # TODO wire in aggregator args overrides
    )


def plan_all_to_all_op(
    op: AbstractAllToAll,
    physical_children: List[PhysicalOperator],
    data_context: DataContext,
) -> PhysicalOperator:
    """Get the corresponding physical operators DAG for AbstractAllToAll operators.

    Note this method only converts the given `op`, but not its input dependencies.
    See Planner.plan() for more details.
    """
    assert len(physical_children) == 1
    input_physical_dag = physical_children[0]

    if isinstance(op, RandomizeBlocks):
        fn = generate_randomize_blocks_fn(op, data_context)
        # Randomize block order does not actually compute anything, so we
        # want to inherit the upstream op's target max block size.

    elif isinstance(op, RandomShuffle):
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_random_shuffle_fn(
            data_context,
            op.seed_config,
            op.num_outputs,
            op.ray_remote_args,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Repartition):
        # v3 file-transport hash shuffle takes precedence over v2/GPU paths
        # when the user opts in via DataContext.use_hash_shuffle_v3. Handles
        # both keyed and keyless repartition (keyless = hash on all columns).
        if data_context.use_hash_shuffle_v3:
            return _plan_hash_shuffle_repartition_v3(
                data_context, op, input_physical_dag
            )

        if op.keys:
            if data_context.shuffle_strategy == ShuffleStrategy.GPU_SHUFFLE:
                return _plan_gpu_shuffle_repartition(
                    data_context, op, input_physical_dag
                )
            elif data_context.shuffle_strategy == ShuffleStrategy.HASH_SHUFFLE:
                return _plan_hash_shuffle_repartition(
                    data_context, op, input_physical_dag
                )
            else:
                raise ValueError(
                    "Key-based repartitioning only supported for "
                    f"`DataContext.shuffle_strategy=HASH_SHUFFLE` or "
                    f"`DataContext.shuffle_strategy=GPU_SHUFFLE` "
                    f"(got {data_context.shuffle_strategy})"
                )

        elif op.shuffle:
            debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
                "debug_limit_shuffle_execution_to_num_blocks", None
            )
        else:
            debug_limit_shuffle_execution_to_num_blocks = None

        fn = generate_repartition_fn(
            op.num_outputs,
            op.shuffle,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Sort):
        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_sort_fn(
            op.sort_key,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )

    elif isinstance(op, Aggregate):
        if data_context.shuffle_strategy in (
            ShuffleStrategy.HASH_SHUFFLE,
            ShuffleStrategy.GPU_SHUFFLE,
        ):
            return _plan_hash_shuffle_aggregate(data_context, op, input_physical_dag)

        debug_limit_shuffle_execution_to_num_blocks = data_context.get_config(
            "debug_limit_shuffle_execution_to_num_blocks", None
        )
        fn = generate_aggregate_fn(
            op.key,
            op.aggs,
            data_context,
            debug_limit_shuffle_execution_to_num_blocks,
        )
    else:
        raise ValueError(f"Found unknown logical operator during planning: {op}")

    return AllToAllOperator(
        fn,
        input_physical_dag,
        data_context,
        num_outputs=op.num_outputs,
        sub_progress_bar_names=op.sub_progress_bar_names,
        name=op.name,
    )
