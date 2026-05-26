"""Single-size spill microbenchmark.

Puts many fixed-size objects into a small object store to force automatic
spilling to the local filesystem, then reads some back to exercise restore.
Timing CSVs are written by the IO worker (Python) and raylet (C++) via the
RAY_SPILL_TIMING* env vars, which Ray child processes inherit.

Usage: python spill_run.py <obj_size_bytes> [store_mb] [fill_multiplier]
"""
import json
import os
import sys
import time

import numpy as np

import ray

obj_size = int(sys.argv[1])
store_mb = int(sys.argv[2]) if len(sys.argv) > 2 else 300
fill_mult = float(sys.argv[3]) if len(sys.argv) > 3 else 4.0

store_bytes = store_mb * 1024 * 1024
spill_dir = os.environ.get("SPILL_DIR", "/tmp/ray_spill")
os.makedirs(spill_dir, exist_ok=True)

ray.init(
    object_store_memory=store_bytes,
    num_cpus=4,
    _system_config={
        "min_spilling_size": 0,
        "automatic_object_spilling_enabled": True,
        "max_io_workers": 4,
        "object_spilling_config": json.dumps(
            {"type": "filesystem", "params": {"directory_path": [spill_dir]}}
        ),
    },
)

# Number of objects so that total >> store, forcing heavy spilling.
n = max(8, int(store_bytes * fill_mult // obj_size))
# Cap to keep wall time reasonable for tiny objects.
n = min(n, 40000)

print(f"obj_size={obj_size} store_mb={store_mb} n_objects={n}", flush=True)

payload = np.ones(obj_size, dtype=np.uint8)
refs = []
t0 = time.perf_counter()
for i in range(n):
    # Distinct buffer per object so they are not deduplicated.
    a = payload.copy()
    a[0] = i & 0xFF
    refs.append(ray.put(a))
put_s = time.perf_counter() - t0
print(f"put done in {put_s:.1f}s, waiting for spill to drain", flush=True)

# Give the spill manager time to flush pinned objects to disk.
time.sleep(5)

# Exercise restore: pull a sample back into plasma.
sample = refs[:: max(1, n // 200)]
t0 = time.perf_counter()
for r in sample:
    ray.get(r)
print(f"restored {len(sample)} objects in {time.perf_counter()-t0:.1f}s", flush=True)

ray.shutdown()
print("DONE", flush=True)
