"""Sweep object size and collect spill timing CSVs.

For each object size it launches a fresh `spill_run.py` subprocess with the
RAY_SPILL_TIMING* env vars set (so the size becomes the CSV `tag`). Python and
C++ timing rows are appended to two shared CSV files.

Usage: python spill_sweep.py <out_dir> [store_mb]
"""
import os
import subprocess
import sys

out_dir = sys.argv[1] if len(sys.argv) > 1 else "/tmp/spillbench/out"
store_mb = sys.argv[2] if len(sys.argv) > 2 else "300"
os.makedirs(out_dir, exist_ok=True)

py_csv = os.path.join(out_dir, "spill_py.csv")
cpp_csv = os.path.join(out_dir, "spill_cpp.csv")
# Start fresh.
for p in (py_csv, cpp_csv):
    if os.path.exists(p):
        os.remove(p)

KB = 1024
MB = 1024 * 1024
sizes = [
    64 * KB, 128 * KB, 256 * KB, 512 * KB,
    1 * MB, 2 * MB, 4 * MB, 8 * MB, 16 * MB, 32 * MB, 64 * MB, 128 * MB,
]

here = os.path.dirname(os.path.abspath(__file__))
run_py = os.path.join(here, "spill_run.py")

for size in sizes:
    env = dict(os.environ)
    env["RAY_SPILL_TIMING"] = py_csv
    env["RAY_SPILL_TIMING_CPP"] = cpp_csv
    env["RAY_SPILL_TIMING_TAG"] = str(size)
    env["SPILL_DIR"] = "/tmp/ray_spill"
    print(f"\n===== object size {size} bytes =====", flush=True)
    rc = subprocess.call(
        [sys.executable, run_py, str(size), store_mb], env=env
    )
    print(f"size {size} exit {rc}", flush=True)

print("\nSWEEP COMPLETE")
print("python csv:", py_csv)
print("cpp csv:", cpp_csv)
