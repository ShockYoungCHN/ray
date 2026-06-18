"""ShuffleMapOpV3 — map phase of the v3 file-transport hash shuffle.

Drives the per-mapper ``v3_map_task`` and emits ONE output ``RefBundle`` per
completed mapper, each carrying the returned ``ShuffleHandle`` ref. Unlike
the v2 ``ShuffleMapOp``, this op does NOT transpose per-mapper output into
per-partition bundles: the v3 ``ShuffleHandle`` already contains the
per-partition byte index, and the v3 reducer reads its partition's bytes
directly from the source node's ``ShuffleManager`` via the file-transport
side-channel. Net effect:

  * No ``_partition_staging[pid]`` FIFO queues on the driver.
  * Each completed map task contributes ONE handle bundle.
  * The downstream ``ShuffleReduceOpV3`` collects all handle bundles and,
    once the map phase finishes, dispatches ``num_partitions`` reduce
    tasks each given the full handle list + a ``partition_id``.

Lifecycle: ``start()`` is the natural place to spawn one ``ShuffleManager``
actor per cluster node that map tasks may run on (managers serve their own
node's shuffle files over a per-node socket endpoint). ``_do_shutdown()``
kills those actors and removes the on-disk shuffle directory.

This is the operator-layer skeleton for the MVP "ds.repartition() goes
through v3" path; planner glue and join/multi-seq are follow-up work.
"""

import functools
import logging
import secrets
import shutil
import tempfile
import typing
from typing import Any, Dict, List, Optional

import ray
from ray.data._internal.execution.bundle_queue import (
    BaseBundleQueue,
    FIFOBundleQueue,
)
from ray.data._internal.execution.interfaces import (
    BlockEntry,
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    MetadataOpTask,
    ObjectStoreUsage,
    OpTask,
    estimate_total_num_of_blocks,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    InternalQueueOperatorMixin,
)
from ray.data._internal.execution.operators.hash_shuffle_v3 import (
    PartitionFn,
    ShuffleCompression,
    ShuffleManager,
    v3_map_task,
)
from ray.data._internal.execution.operators.sub_progress import (
    SubProgressBarMixin,
)
from ray.data.block import BlockMetadata, BlockStats
from ray.data.context import DataContext
from ray.types import ObjectRef
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

if typing.TYPE_CHECKING:
    from ray.data._internal.progress.base_progress import BaseProgressBar

logger = logging.getLogger(__name__)


# Per-mapper sentinel stamped on the emitted handle bundle's metadata.
# Lets the reducer recover "which mapper produced this handle" if it ever
# needs to (current reducer doesn't, since it just collects every handle
# it sees — but keeping it parallels the v2 ``__partition__N`` convention
# and helps debugging).
_MAPPER_ID_SENTINEL = "__v3_mapper__"


def _make_mapper_sentinel(mapper_id: int) -> List[str]:
    return [f"{_MAPPER_ID_SENTINEL}{mapper_id}"]


class ShuffleMapOpV3(
    InternalQueueOperatorMixin, PhysicalOperator, SubProgressBarMixin
):
    """V3 map operator. See module docstring."""

    _DEFAULT_MAP_NUM_CPUS = 1.0
    _DEFAULT_POOL_BUDGET_BYTES = 16 * 1024 * 1024  # 16 MiB

    def __init__(
        self,
        input_op: PhysicalOperator,
        data_context: DataContext,
        *,
        num_partitions: int,
        partition_fn: PartitionFn,
        compression: ShuffleCompression = None,
        pool_budget_bytes: int = _DEFAULT_POOL_BUDGET_BYTES,
        fsync_on_close: bool = True,
        map_cpus: float = _DEFAULT_MAP_NUM_CPUS,
        base_dir: Optional[str] = None,
        name: str = "ShuffleMapV3",
    ):
        super().__init__(
            name=name,
            input_dependencies=[input_op],
            data_context=data_context,
        )

        self._num_partitions: int = num_partitions
        self._partition_fn: PartitionFn = partition_fn
        self._compression: ShuffleCompression = compression
        self._pool_budget_bytes: int = pool_budget_bytes
        self._fsync_on_close: bool = fsync_on_close

        # -- Map task config --
        self._map_num_cpus: float = map_cpus

        # -- On-disk staging --
        # One ``base_dir`` shared by all mappers + the per-node ShuffleManager;
        # we tear it down in ``_do_shutdown``. Caller can override (e.g. to
        # point at a fast local SSD / tmpfs) — otherwise a per-op tempdir.
        self._owns_base_dir = base_dir is None
        self._base_dir: str = base_dir or tempfile.mkdtemp(
            prefix="ray_shuffle_v3_"
        )
        # Per-shuffle auth token; ShuffleManager rejects requests with any
        # other token. Cheap defense against accidental cross-shuffle reads
        # by misrouted reducers in a shared cluster.
        self._token: str = secrets.token_hex(16)

        # -- Per-node ShuffleManager registry --
        # node_id → ShuffleManager actor handle (lazily populated)
        self._managers: Dict[str, "ray.actor.ActorHandle"] = {}
        # node_id → (host, port) cached endpoint
        self._endpoints: Dict[str, "typing.Tuple[str, int]"] = {}

        # -- Map task tracking --
        self._next_map_idx: int = 0
        self._shuffle_map_tasks: Dict[int, MetadataOpTask] = {}
        self._map_resource_usage = ExecutionResources.zero()

        # -- Output queue --
        # Each item is a 1-block RefBundle whose ref points at the
        # ShuffleHandle dict returned by ``v3_map_task``.
        self._output_queue: FIFOBundleQueue = FIFOBundleQueue()

        # -- Stats --
        self._total_input_rows: int = 0
        self._total_input_bytes: int = 0
        self._map_blocks_stats: List[BlockStats] = []

        # -- Sub-progress bar --
        self._map_bar: Optional["BaseProgressBar"] = None

    # ──────────────────────────── Queue plumbing ────────────────────────────
    @property
    def _input_queues(self) -> List[BaseBundleQueue]:
        return []

    @property
    def _output_queues(self) -> List[BaseBundleQueue]:
        return [self._output_queue]

    # ──────────────────────── ShuffleManager bootstrap ──────────────────────
    def _ensure_manager(self, node_id: str) -> "ray.actor.ActorHandle":
        """Spawn a ``ShuffleManager`` actor pinned to ``node_id`` if missing.

        Per-node managers are mandatory: each map task writes its
        ``map_{i}.shf`` to LOCAL disk on whatever node Ray schedules it on,
        and the handle's endpoint must point at that node's manager so the
        reducer can fetch over the side-channel.
        """
        if node_id in self._managers:
            return self._managers[node_id]
        actor = ShuffleManager.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id, soft=False
            ),
            num_cpus=0,
        ).remote(self._base_dir, self._token)
        self._managers[node_id] = actor
        self._endpoints[node_id] = ray.get(actor.endpoint.remote())
        return actor

    def _pick_target_node(self, refs: RefBundle) -> str:
        """Choose a node to run the map task on, preferring input-block
        locality. Falls back to this driver's node if upstream metadata has
        no location preference."""
        prefer_locs = refs.get_preferred_object_locations()
        if prefer_locs:
            return max(prefer_locs, key=lambda n: prefer_locs[n])
        # No upstream locality info — schedule on the driver's node so the
        # ShuffleManager (which we spawn there) has local access.
        return ray.get_runtime_context().get_node_id()

    # ─────────────────────────── PhysicalOperator API ───────────────────────
    def _add_input_inner(self, refs: RefBundle, input_index: int) -> None:
        assert input_index == 0
        if not refs.block_refs:
            refs.destroy_if_owned()
            return
        node_id = self._pick_target_node(refs)
        self._ensure_manager(node_id)
        self._submit_map_task(refs, target_node_id=node_id)

    def _submit_map_task(
        self,
        input_bundle: RefBundle,
        *,
        target_node_id: str,
    ) -> None:
        """Submit one ``v3_map_task`` for ``input_bundle`` on ``target_node_id``."""
        map_id = self._next_map_idx
        self._next_map_idx += 1

        estimated_bytes = sum(
            (m.size_bytes or 0) for m in input_bundle.metadata
        )

        # 2× input-size memory hint mirrors v2's heuristic: input block in
        # memory + transient partition spike (capped further by
        # pool_budget_bytes inside v3_map_task).
        resources: Dict[str, Any] = {"num_cpus": self._map_num_cpus}
        if estimated_bytes > 0:
            resources["memory"] = estimated_bytes * 2

        endpoint = self._endpoints[target_node_id]
        ray_options: Dict[str, Any] = {
            **resources,
            "scheduling_strategy": NodeAffinitySchedulingStrategy(
                target_node_id, soft=False
            ),
        }

        handle_ref = v3_map_task.options(**ray_options).remote(
            *input_bundle.block_refs,
            partition_fn=self._partition_fn,
            num_partitions=self._num_partitions,
            out_dir=self._base_dir,
            map_id=map_id,
            endpoint=endpoint,
            token=self._token,
            pool_budget_bytes=self._pool_budget_bytes,
            compression=self._compression,
            fsync_on_close=self._fsync_on_close,
        )

        task = MetadataOpTask(
            task_index=map_id,
            object_ref=handle_ref,
            task_done_callback=functools.partial(
                self._handle_map_done, map_id, handle_ref, input_bundle
            ),
            task_resource_bundle=ExecutionResources.from_resource_dict(resources),
        )
        self._shuffle_map_tasks[map_id] = task
        requested = task.get_requested_resource_bundle()
        assert requested is not None
        self._map_resource_usage = self._map_resource_usage.add(requested)

        all_blocks_meta = tuple(
            BlockEntry(ref=ref, metadata=meta)
            for ref, meta in zip(
                input_bundle.block_refs, input_bundle.metadata
            )
        )
        self._metrics.on_task_submitted(
            map_id,
            RefBundle(all_blocks_meta, schema=None, owns_blocks=False),
            task_id=task.get_task_id(),
        )

        if self._map_bar is not None:
            _, _, num_rows = estimate_total_num_of_blocks(
                map_id + 1,
                self.upstream_op_num_outputs(),
                self._metrics,
                total_num_tasks=None,
            )
            self._map_bar.update(total=num_rows)

    def _handle_map_done(
        self,
        map_id: int,
        handle_ref: "ObjectRef",
        input_bundle: RefBundle,
    ) -> None:
        """``MetadataOpTask`` callback: handle is materialized.

        We don't need to actually deserialize the handle on the driver (the
        reducer can do that), but we do want to record stats from the
        on-disk file size and free input bundles. We emit a 1-block bundle
        whose ref IS the handle ref, schema=None (handles aren't tables).
        """
        task = self._shuffle_map_tasks.pop(map_id)
        requested = task.get_requested_resource_bundle()
        assert requested is not None
        self._map_resource_usage = self._map_resource_usage.subtract(requested)

        # Read the handle once on the driver so we can grab num_rows /
        # size_bytes for metrics. The dict is small (KBs).
        handle = ray.get(handle_ref)
        total_bytes = handle.get("total_bytes", 0)

        out_meta = BlockMetadata(
            num_rows=None,  # handle isn't itself a Block
            size_bytes=total_bytes,
            exec_stats=None,
            input_files=_make_mapper_sentinel(map_id),
        )
        out_bundle = RefBundle(
            (BlockEntry(ref=handle_ref, metadata=out_meta),),
            schema=None,
            owns_blocks=True,
        )
        self._output_queue.add(out_bundle)
        self._metrics.on_output_queued(out_bundle)

        # Free input bundles (they've been consumed by the map task).
        input_bundle.destroy_if_owned()

        # Roll up stats from input metadata.
        input_rows = sum((m.num_rows or 0) for m in input_bundle.metadata)
        input_bytes = sum((m.size_bytes or 0) for m in input_bundle.metadata)
        self._total_input_rows += input_rows
        self._total_input_bytes += input_bytes
        # No exec_stats here (we'd need v3_map_task to surface them); MVP
        # leaves block stats minimal.

        self._metrics.on_task_finished(
            map_id, None, task_exec_stats=None, task_exec_driver_stats=None
        )
        self._metrics.on_task_output_generated(
            task_index=map_id, output=out_bundle
        )

        if self._map_bar is not None:
            self._map_bar.update(increment=input_rows)

    def has_next(self) -> bool:
        return self._output_queue.has_next()

    def _get_next_inner(self) -> RefBundle:
        bundle: RefBundle = self._output_queue.get_next()
        self._metrics.on_output_dequeued(bundle)
        return bundle

    def get_active_tasks(self) -> List[OpTask]:
        return list(self._shuffle_map_tasks.values())

    def has_execution_finished(self) -> bool:
        if self._shuffle_map_tasks or self._output_queue.has_next():
            return False
        return super().has_execution_finished()

    def has_completed(self) -> bool:
        return (
            not self._shuffle_map_tasks
            and not self._output_queue.has_next()
            and super().has_completed()
        )

    def _do_shutdown(self, force: bool = False) -> None:
        super()._do_shutdown(force)
        self._shuffle_map_tasks.clear()
        self._output_queue.clear()
        # Tear down per-node ShuffleManager actors. Map files live under
        # base_dir on whatever nodes they were written to; the directory is
        # cleaned up below (single-node MVP — multi-node will need per-node
        # cleanup tasks).
        for node_id, actor in list(self._managers.items()):
            try:
                ray.kill(actor)
            except Exception:
                logger.exception(
                    "Failed to kill ShuffleManager on node %s", node_id
                )
        self._managers.clear()
        self._endpoints.clear()
        if self._owns_base_dir:
            shutil.rmtree(self._base_dir, ignore_errors=True)

    # ───────────────────────────── Stats / progress ─────────────────────────
    def get_stats(self) -> Dict[str, List[BlockStats]]:
        return {self._name: self._map_blocks_stats}

    def num_output_rows_total(self) -> Optional[int]:
        return self._total_input_rows if self._total_input_rows > 0 else None

    def current_logical_usage(self) -> ExecutionResources:
        return ExecutionResources(
            cpu=self._map_resource_usage.cpu,
            memory=self._map_resource_usage.memory,
        )

    def estimate_object_store_usage(self, state) -> ObjectStoreUsage:
        # Bulk shuffle data lives on disk, not in Plasma. Handles are
        # KB-sized dicts, so their object-store footprint is negligible.
        return ObjectStoreUsage(internal=0, outputs=0)

    def incremental_resource_usage(self) -> ExecutionResources:
        # Mirror v2's heuristic: 2× per-task input-bytes memory hint.
        avg_input = self._metrics.average_bytes_inputs_per_task
        memory = int(avg_input * 2) if avg_input else 0
        return ExecutionResources(cpu=self._map_num_cpus, memory=memory)

    def min_scheduling_resources(self) -> ExecutionResources:
        return self.incremental_resource_usage()

    def progress_str(self) -> str:
        maps_done = self._next_map_idx - len(self._shuffle_map_tasks)
        return f"map: {maps_done}/{self._next_map_idx}"

    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        return ["Map"]

    def set_sub_progress_bar(self, name: str, pg: "BaseProgressBar") -> None:
        if name == "Map":
            self._map_bar = pg

    # ─────────────────────── V3-specific accessors ──────────────────────────
    @property
    def num_partitions(self) -> int:
        return self._num_partitions

    @property
    def base_dir(self) -> str:
        return self._base_dir

    @property
    def token(self) -> str:
        return self._token

    def get_endpoints(self) -> Dict[str, "typing.Tuple[str, int]"]:
        """Snapshot of node_id → (host, port). Used by the reducer op to
        sanity-check that all handles' endpoints map to known managers."""
        return dict(self._endpoints)
