#!/usr/bin/env python3
"""A/B driver: run pull_bench.py for batch vs nobatch and print the comparison.

Two modes:
  two images:   --batch-image IMG --nobatch-image IMG
  one base + mounted raylet (fairest, only the raylet binary differs):
                --base-image IMG --batch-raylet /path/to/bazel-bin/.../raylet

Sweep configs as 'pullers:per_puller' pairs:
  python ab.py --base-image .../nobatch-rpc_fix \
      --batch-raylet $(readlink -f ../bazel-bin/src/ray/raylet/raylet) \
      --configs 1:1024,8:128,32:128
"""
import argparse, json, os, subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
IMG_RAYLET = "/home/ray/anaconda3/lib/python3.10/site-packages/ray/core/src/ray/raylet/raylet"


def run(image, raylet, cfg, args):
    pullers, per = cfg
    cmd = ["docker", "run", "--rm", "--shm-size=8gb", "-v", f"{HERE}:/bench:ro"]
    if raylet:
        cmd += ["-v", f"{raylet}:{IMG_RAYLET}:ro"]
    cmd += [image, "python", "/bench/pull_bench.py", "--local", "--emit",
            "--pullers", str(pullers), "--per-puller", str(per),
            "--obj-size-kb", str(args.obj_size_kb),
            "--rounds", str(args.rounds), "--warmup", str(args.warmup)]
    r = subprocess.run(cmd, capture_output=True, text=True)
    for line in r.stdout.splitlines():
        if line.startswith("BENCHROW "):
            return json.loads(line[len("BENCHROW "):])
    print(r.stdout[-1500:]); print(r.stderr[-1500:])
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--configs", default="1:1024,8:128,32:128")
    p.add_argument("--obj-size-kb", type=float, default=4.0)
    p.add_argument("--rounds", type=int, default=6)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--base-image")
    p.add_argument("--batch-raylet")
    p.add_argument("--batch-image")
    p.add_argument("--nobatch-image")
    args = p.parse_args()

    configs = [tuple(int(x) for x in c.split(":")) for c in args.configs.split(",")]
    if args.batch_raylet:
        batch, nobatch = (args.base_image, args.batch_raylet), (args.base_image, None)
    else:
        batch, nobatch = (args.batch_image, None), (args.nobatch_image, None)

    print(f"{'config':>10} {'objs':>5} {'batch CPUms':>11} {'nobatch CPUms':>13} "
          f"{'CPU saved':>9} {'wall b/nb':>9}")
    for cfg in configs:
        b = run(batch[0], batch[1], cfg, args)
        nb = run(nobatch[0], nobatch[1], cfg, args)
        if not b or not nb:
            continue
        bc, nbc = b["raylet_cpu_s"] * 1000, nb["raylet_cpu_s"] * 1000
        saved = f"{(1 - bc / nbc) * 100:.0f}%" if nbc else "-"
        print(f"{f'{cfg[0]}x{cfg[1]}':>10} {b['total_obj']:>5} {bc:>11.0f} {nbc:>13.0f} "
              f"{saved:>9} {nb['wall_s'] / b['wall_s']:>8.2f}x")


if __name__ == "__main__":
    main()
