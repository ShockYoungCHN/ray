#!/usr/bin/env python3
"""Real 2-node cross-node pull benchmark with per-node raylet CPU.

Attaches to a running cluster (--address auto). Producer = driver node A
(ray.put lands objects here = the SOURCE/server). W consumer tasks pinned to
node B (the PULLER/sender) each pull K objects from A. Because A and B are
separate hosts, we sample each raylet's /proc on its OWN node via a tiny task
pinned there, giving a real server-vs-sender CPU split.

  python pull_bench_2node.py --pullers 8 --per-puller 128 --obj-size-kb 4 --emit
"""
import argparse, glob, json, os, statistics, time
import numpy as np, ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

KIB, MIB, CLK = 1024, 1024 * 1024, os.sysconf("SC_CLK_TCK")


@ray.remote(num_cpus=0)
def local_raylet_cpu():
    # Runs on whatever node it's pinned to; reads that host's raylet /proc.
    for cl in glob.glob("/proc/[0-9]*/cmdline"):
        try:
            cmd = open(cl, "rb").read().replace(b"\0", b" ").decode()
        except Exception:
            continue
        if "--raylet_socket_name=" not in cmd:
            continue
        pid = cl.split("/")[2]
        try:
            s = open(f"/proc/{pid}/stat").read()
            a = s[s.rindex(")") + 2:].split()
            return (int(a[11]) + int(a[12])) / CLK
        except Exception:
            return 0.0
    return 0.0


@ray.remote
def pull(*refs):
    return sum(int(a[0]) for a in refs) if refs else 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pullers", type=int, default=8)
    p.add_argument("--per-puller", type=int, default=128)
    p.add_argument("--obj-size-kb", type=float, default=4.0)
    p.add_argument("--rounds", type=int, default=6)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--emit", action="store_true")
    args = p.parse_args()

    size = int(args.obj_size_kb * KIB)
    total = args.pullers * args.per_puller
    total_mb = total * size / MIB

    ray.init(address="auto")
    a_node = ray.get_runtime_context().get_node_id()
    b_node = sorted(n["NodeID"] for n in ray.nodes()
                    if n["Alive"] and n["NodeID"] != a_node)[0]
    on_a = NodeAffinitySchedulingStrategy(a_node, soft=False)
    on_b = NodeAffinitySchedulingStrategy(b_node, soft=False)

    def cpu(node_strat):
        return ray.get(local_raylet_cpu.options(scheduling_strategy=node_strat).remote())

    def wave():
        pool = [[ray.put(np.ones(size, dtype=np.uint8)) for _ in range(args.per_puller)]
                for _ in range(args.pullers)]
        a0, b0 = cpu(on_a), cpu(on_b)
        t0 = time.perf_counter()
        ray.get([pull.options(scheduling_strategy=on_b).remote(*ch) for ch in pool])
        dt = time.perf_counter() - t0
        a1, b1 = cpu(on_a), cpu(on_b)
        del pool
        return dt, a1 - a0, b1 - b0

    for _ in range(args.warmup):
        wave()
    walls, srv, snd = [], [], []
    for _ in range(args.rounds):
        dt, a, b = wave()
        walls.append(dt); srv.append(a); snd.append(b)

    row = {
        "pullers": args.pullers, "per_puller": args.per_puller,
        "obj_size_kb": args.obj_size_kb, "total_obj": total, "total_mb": total_mb,
        "wall_s": statistics.median(walls),
        "server_cpu_s": statistics.median(srv),   # node A (source / receives Pull RPCs)
        "sender_cpu_s": statistics.median(snd),   # node B (puller / sends Pull RPCs)
    }
    row["raylet_cpu_s"] = row["server_cpu_s"] + row["sender_cpu_s"]
    print(f"{total} objs  wall {row['wall_s']*1000:.0f}ms  "
          f"server(A) {row['server_cpu_s']*1000:.0f}ms  sender(B) {row['sender_cpu_s']*1000:.0f}ms")
    if args.emit:
        print("BENCHROW " + json.dumps(row))
    ray.shutdown()


if __name__ == "__main__":
    main()
