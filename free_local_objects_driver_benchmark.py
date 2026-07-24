"""Driver-side workload benchmark for owner-driven FreeLocalObjects batching.

Owner-driven freeing is push-from-the-owner: when the driver's refcount for an
object hits zero it sends a FreeLocalObjects RPC to each raylet holding a copy and
handles the reply. This measures only the *driver's* workload for a big deletion
wave -- the RPCs it sends and the reply callbacks it processes -- which is exactly
what Nagle-style batching coalesces.

Objects are forced into plasma (max_direct_call_object_size=0) so they actually go
through the owner-driven FreeLocalObjects path (small inlined objects never hit a
raylet).

Driver-workload metrics (from RAY_event_stats in the driver log):
  - FreeLocalObjects RPCs sent           (main-thread send path)
  - FreeLocalObjects.OnReplyReceived     (io_context reply callbacks)
Optionally a py-spy --native flamegraph of the driver during the wave [--flame],
which shows the CoreWorker::FreeObjectOnNodesAsync CPU share on the main thread.

Usage:
  python3.10 free_local_objects_driver_benchmark.py --n 1000000 --batched 1 --flame
"""

import argparse
import glob
import os
import re
import subprocess
import time


def driver_free_rpc_counts(session_dir):
    """Read the driver's FreeLocalObjects send / reply-callback counts from its
    RAY_event_stats log (last snapshot)."""
    sends = replies = None
    for path in glob.glob(os.path.join(session_dir, "logs", "python-core-driver-*.log")):
        with open(path) as f:
            for line in f:
                m = re.search(r"grpc_client\.FreeLocalObjects\.OnReplyReceived - (\d+) total", line)
                if m:
                    replies = int(m.group(1))
                    continue
                m = re.search(r"grpc_client\.FreeLocalObjects - (\d+) total", line)
                if m:
                    sends = int(m.group(1))
    return sends, replies


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=1_000_000)
    ap.add_argument("--flame", action="store_true", help="capture a py-spy flamegraph")
    ap.add_argument("--object-store-gb", type=float, default=8.0)
    ap.add_argument("--batch-size", type=int, default=256,
                    help="max_free_local_objects_batch_size (1 ~= no coalescing)")
    ap.add_argument("--batched", type=int, default=1,
                    help="1 = coalesced batcher, 0 = legacy one-RPC-per-object")
    ap.add_argument("--settle", type=float, default=10.0,
                    help="seconds to let the free wave drain + event_stats flush")
    args = ap.parse_args()

    os.environ.setdefault("RAY_event_stats", "1")
    os.environ.setdefault("RAY_event_stats_print_interval_ms", "1000")

    import ray

    ray.init(
        num_cpus=4,
        object_store_memory=int(args.object_store_gb * 1024**3),
        _system_config={
            "max_direct_call_object_size": 0,
            "max_free_local_objects_batch_size": args.batch_size,
            "batch_free_local_objects": bool(args.batched),
        },
        logging_level="info",
    )
    driver_pid = os.getpid()
    session_dir = ray._private.worker._global_node.get_session_dir_path()
    mode = "batched" if args.batched else "baseline"
    print(f"[{mode}] driver pid={driver_pid} n={args.n} session={session_dir}")

    t0 = time.time()
    refs = [ray.put(i) for i in range(args.n)]
    print(f"[{mode}] created {args.n} plasma objects in {time.time() - t0:.1f}s")

    flame = None
    if args.flame:
        flame = subprocess.Popen(
            ["py-spy", "record", "-o", f"driver_{mode}.svg", "--native", "--pid",
             str(driver_pid), "--duration", str(int(args.settle)), "--rate", "500",
             "--threads"]
        )
        time.sleep(1)  # let py-spy attach before the wave

    # The deletion wave: drop every reference -> owner-driven FreeLocalObjects.
    del refs

    # Let the driver drain the wave and event_stats flush.
    time.sleep(args.settle)

    sends, replies = driver_free_rpc_counts(session_dir)
    print(f"[{mode}] driver FreeLocalObjects RPCs sent : {sends}")
    print(f"[{mode}] driver reply callbacks handled    : {replies}")

    if flame is not None:
        flame.wait()
        print(f"[{mode}] flamegraph -> driver_{mode}.svg")

    ray.shutdown()


if __name__ == "__main__":
    main()
