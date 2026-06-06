"""Microbenchmark: decompose the reducer ``ray.get`` cost by fetch path.

The OOC-shuffle reducer wraps its whole shard fetch in a single
``reduce.ray_get_s`` timer (``_shuffle_tasks.py:425``).  That number is a
black box mixing four very different physical operations:

    1. local  + in-memory  : plasma hit on the reducer's own node (cheap)
    2. local  + spilled     : the reducer's own raylet restores from local disk
    3. remote + in-memory   : pull a plasma copy over the network
    4. remote + spilled     : the *remote* node restores from ITS disk, then
                              ships it over the network  <-- user's path #2

This probe measures the *per-object* cost of each path in isolation on the
real cluster, with NO modification to Ray internals.  It:

  * creates a pile of ~SHARD-sized objects on one "producer" node, large
    enough that the node's object store overflows and a chunk of them spill;
  * classifies every object via ``get_object_locations`` (``did_spill`` +
    whether the producer node is the holder) into in-mem vs spilled;
  * splits each class into two disjoint halves so a local measurement can't
    warm the cache for the remote one;
  * times batched ``ray.get`` (batch=16, mirroring the reducer) from a
    consumer task pinned to the producer node (local) and to a different
    node (remote), for spilled and in-memory subsets.

Output: per-path mean per-object latency + effective throughput, plus the
spill/network deltas that say which part of the reducer get to optimize.
"""

import argparse
import time
from collections import defaultdict

import numpy as np

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

_REDUCE_BATCH_SIZE = 16  # mirror _shuffle_tasks._REDUCE_BATCH_SIZE


@ray.remote(num_cpus=1)
def _produce(n: int, size: int):
    """Create ``n`` separate objects of ``size`` bytes, owned by the driver,
    living in THIS node's plasma.  Dynamic generator => one object per yield."""
    for _ in range(n):
        # Real bytes (not zero-filled lazily) so plasma actually holds size.
        yield np.frombuffer(bytes(size), dtype=np.uint8).copy()


@ray.remote(num_cpus=1)
def _consume(refs, batch_size: int):
    """Fetch ``refs`` in batches (mirrors the reducer), timing each batch.

    Returns (total_get_s, n_objs, total_bytes, per_batch_s list)."""
    total_s = 0.0
    total_bytes = 0
    n = 0
    per_batch = []
    for start in range(0, len(refs), batch_size):
        batch = refs[start : start + batch_size]
        t0 = time.perf_counter()
        got = ray.get(batch)
        dt = time.perf_counter() - t0
        total_s += dt
        per_batch.append(dt)
        for g in got:
            total_bytes += g.nbytes
            n += 1
        del got
    return total_s, n, total_bytes, per_batch


def _node_ids():
    return [n["NodeID"] for n in ray.nodes()
            if n.get("Alive") and n.get("Resources", {}).get("CPU", 0) >= 1]


def _classify(refs, producer_node):
    """Split refs into (local_mem, local_spill) by did_spill, as seen now.

    All refs live on producer_node, so 'local' = producer_node is holder.
    Returns dict path -> list[ref]."""
    locs = {}
    for i in range(0, len(refs), 1000):
        chunk = refs[i : i + 1000]
        locs.update(
            ray.experimental.get_object_locations(chunk, timeout_ms=60000)
        )
    out = defaultdict(list)
    sizes = {}
    for r in refs:
        info = locs.get(r)
        if info is None:
            out["unknown"].append(r)
            continue
        did_spill = info.get("did_spill", False)
        node_ids = info.get("node_ids") or []
        sizes[r] = info.get("object_size") or 0
        on_producer = producer_node in node_ids
        # producer is the only holder; classify purely by spill state.
        out["spill" if did_spill else "mem"].append(r)
    return out, sizes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--object-size-kib", type=int, default=1024,
                    help="Per-object size (default 1 MiB ~ avg shuffle shard).")
    ap.add_argument("--total-gib", type=float, default=14.0,
                    help="Total bytes to create on the producer node; must "
                         "exceed its object store to force spill.")
    args = ap.parse_args()

    ray.init(address="auto")
    size = args.object_size_kib * 1024
    n_obj = int(args.total_gib * 1024**3 / size)

    nodes = _node_ids()
    obj_store_per_node = ray.cluster_resources().get("object_store_memory", 0) / len(nodes)
    print(f"Cluster: {len(nodes)} CPU nodes, "
          f"~{obj_store_per_node/1e9:.1f} GB object store/node")
    producer = nodes[0]
    remote_node = nodes[1]
    print(f"Producer node: {producer[:8]}  remote consumer node: {remote_node[:8]}")

    local_sched = NodeAffinitySchedulingStrategy(node_id=producer, soft=False)
    remote_sched = NodeAffinitySchedulingStrategy(node_id=remote_node, soft=False)

    def produce(total_bytes):
        """Create ~total_bytes of `size`-objects on the producer node."""
        n = int(total_bytes / size)
        per_task = max(1, n // 64)
        gens = [
            _produce.options(scheduling_strategy=local_sched, num_cpus=0).remote(
                per_task, size
            )
            for _ in range(64)
        ]
        out = []
        for g in gens:
            out.extend(list(g))
        return out

    def measure(label, subset, sched_):
        if len(subset) < 8:
            print(f"  {label}: SKIP (only {len(subset)} objs)")
            return None
        total_s, n, total_bytes, _ = ray.get(
            _consume.options(scheduling_strategy=sched_, num_cpus=0).remote(
                subset, _REDUCE_BATCH_SIZE
            )
        )
        per_obj_ms = total_s / n * 1000
        thr = total_bytes / total_s / 1e6
        print(f"  {label:22s} n={n:5d}  get={total_s:7.3f}s  "
              f"per_obj={per_obj_ms:6.3f}ms  thr={thr:7.1f} MB/s", flush=True)
        return {"label": label, "per_obj_ms": per_obj_ms, "thr_mbps": thr}

    r = {}

    # --- Phase A: in-memory baselines (small pile that fits in store) ---
    fit = obj_store_per_node * 0.45  # well under the store -> nothing spills
    print(f"\n[Phase A] in-memory: creating {fit/1e9:.1f} GB on producer "
          "(no spill) ...", flush=True)
    mem_refs = produce(fit)
    time.sleep(5)
    classed, _ = _classify(mem_refs, producer)
    print(f"  classified: mem={len(classed['mem'])} spill={len(classed['spill'])}",
          flush=True)
    mem = classed["mem"]
    half = len(mem) // 2
    # disjoint subsets per path; remote measured first (it drops a copy on the
    # consumer but the originals on the producer stay in-memory regardless).
    print("=== in-memory paths (batch=16) ===")
    r["remote_mem"] = measure("remote+in-memory(#3)", mem[:half], remote_sched)
    r["local_mem"] = measure("local+in-memory (#1)", mem[half:], local_sched)
    del mem_refs, mem, classed

    # --- Phase B: spilled paths (large pile that overflows the store) ---
    print(f"\n[Phase B] spilled: creating {args.total_gib:.1f} GB on producer "
          "(forces spill) ...", flush=True)
    spill_refs = produce(args.total_gib * 1024**3)
    time.sleep(10)
    classed, _ = _classify(spill_refs, producer)
    n_spill = len(classed["spill"])
    print(f"  classified: mem={len(classed['mem'])} spill={n_spill}", flush=True)
    spill = classed["spill"]
    half = len(spill) // 2
    print("=== spilled paths (batch=16) ===")
    # remote-spill FIRST: a remote pull restores on producer + copies to
    # consumer, so doing local first would convert spilled->in-memory.
    r["remote_spill"] = measure("remote+spilled (#4)", spill[:half], remote_sched)
    r["local_spill"] = measure("local+spilled  (#2)", spill[half:], local_sched)

    print("\n=== Deltas (the cost of each physical operation) ===")
    def perobj(key):
        return r[key]["per_obj_ms"] if r.get(key) else None
    lm, ls = perobj("local_mem"), perobj("local_spill")
    rm, rs = perobj("remote_mem"), perobj("remote_spill")
    if lm is not None and ls is not None:
        print(f"  local-disk restore  (#1 path)  = local_spill - local_mem "
              f"= {ls - lm:.3f} ms/obj")
    if lm is not None and rm is not None:
        print(f"  network transfer    (in-mem)   = remote_mem - local_mem  "
              f"= {rm - lm:.3f} ms/obj")
    if rm is not None and rs is not None:
        print(f"  remote restore+net  (#2 extra) = remote_spill - remote_mem "
              f"= {rs - rm:.3f} ms/obj")
    if rs is not None and ls is not None:
        print(f"  remote_spill vs local_spill    = {rs - ls:.3f} ms/obj "
              f"(network on top of a restore)")

    print("\nDONE")


if __name__ == "__main__":
    main()
