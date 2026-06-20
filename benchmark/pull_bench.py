#!/usr/bin/env python3
"""Cross-node object-pull benchmark core (one image).

W tasks on node B each pull K objects from node A. Reports raylet control-plane
CPU (the metric batch-rpc actually moves), plus wall time and throughput. Run
two of these (batch vs nobatch) and diff them with ab.py.

  python pull_bench.py --local --pullers 8 --per-puller 128 --obj-size-kb 4 --emit
  python pull_bench.py --local --pullers 1 --per-puller 1024            # single task
"""
import argparse, glob, json, os, re, statistics, time
import numpy as np, ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

KIB, MIB, CLK = 1024, 1024 * 1024, os.sysconf("SC_CLK_TCK")


@ray.remote
def pull(*refs):
    return sum(int(a[0]) for a in refs) if refs else 0


def raylet_cpu():
    # {socket_basename: utime+stime seconds} for every raylet process.
    out = {}
    for cl in glob.glob("/proc/[0-9]*/cmdline"):
        try:
            cmd = open(cl, "rb").read().replace(b"\0", b" ").decode()
        except Exception:
            continue
        m = re.search(r"--raylet_socket_name=(\S+)", cmd)
        if not m or "raylet" not in cmd:
            continue
        pid = cl.split("/")[2]
        try:
            s = open(f"/proc/{pid}/stat").read()
            a = s[s.rindex(")") + 2:].split()
            out[m.group(1).split("/")[-1]] = (int(a[11]) + int(a[12])) / CLK
        except Exception:
            pass
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--pullers", type=int, default=8)
    p.add_argument("--per-puller", type=int, default=128)
    p.add_argument("--obj-size-kb", type=float, default=4.0)
    p.add_argument("--rounds", type=int, default=6)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--local", action="store_true")
    p.add_argument("--local-store-mb", type=int, default=3000)
    p.add_argument("--emit", action="store_true")
    args = p.parse_args()

    size = int(args.obj_size_kb * KIB)
    total = args.pullers * args.per_puller
    total_mb = total * size / MIB

    if args.local:
        from ray.cluster_utils import Cluster
        c = Cluster()
        for _ in range(2):
            c.add_node(num_cpus=args.pullers + 2,
                       object_store_memory=args.local_store_mb * MIB)
        ray.init(address=c.address)
    else:
        ray.init(address="auto")

    a_node = ray.get_runtime_context().get_node_id()
    b_node = sorted(n["NodeID"] for n in ray.nodes()
                    if n["Alive"] and n["NodeID"] != a_node)[0]
    strat = NodeAffinitySchedulingStrategy(b_node, soft=False)

    def wave():
        pool = [[ray.put(np.ones(size, dtype=np.uint8)) for _ in range(args.per_puller)]
                for _ in range(args.pullers)]
        c0 = raylet_cpu()
        t0 = time.perf_counter()
        ray.get([pull.options(scheduling_strategy=strat).remote(*ch) for ch in pool])
        dt = time.perf_counter() - t0
        c1 = raylet_cpu()
        del pool
        return dt, {k: c1.get(k, 0) - c0.get(k, 0) for k in c1}

    for _ in range(args.warmup):
        wave()
    walls, cpus = [], []
    for _ in range(args.rounds):
        dt, cpu = wave()
        walls.append(dt)
        cpus.append(cpu)

    wall = statistics.median(walls)
    cpu_total = statistics.median(sum(c.values()) for c in cpus)
    # node A (driver/head) = source/server socket "raylet"; B = puller "raylet.1"
    server = statistics.median(c.get("raylet", 0) for c in cpus)
    sender = statistics.median(c.get("raylet.1", 0) for c in cpus)
    row = {
        "pullers": args.pullers, "per_puller": args.per_puller,
        "obj_size_kb": args.obj_size_kb, "total_obj": total, "total_mb": total_mb,
        "wall_s": wall, "obj_per_s": total / wall, "mb_per_s": total_mb / wall,
        "raylet_cpu_s": cpu_total, "server_cpu_s": server, "sender_cpu_s": sender,
    }
    print(f"{total} objs/round  wall {wall * 1000:.0f} ms  {row['obj_per_s']:.0f} obj/s  "
          f"raylet CPU {cpu_total * 1000:.0f} ms (server {server * 1000:.0f}, "
          f"sender {sender * 1000:.0f})")
    if args.emit:
        print("BENCHROW " + json.dumps(row))
    ray.shutdown()


if __name__ == "__main__":
    main()
