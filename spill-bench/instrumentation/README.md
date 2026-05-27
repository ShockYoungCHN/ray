# Spill timing instrumentation

Env-gated timing used to produce the spill breakdown. It is **not** part of the
fix PR — it exists only to reproduce the numbers, and everything is inert unless
the `RAY_SPILL_TIMING*` env vars below are set.

On this branch (`reform-spill`) the instrumentation lives **directly in the real
source files**, so the default reproduction (checkout → build → run the sweep)
measures the **after-fix** path with no copy step:

- `src/ray/raylet/local_object_manager.cc`
  - `T_total` — `steady_clock` around the `SpillObjects` RPC, recorded in the
    reply callback. Writes a CSV row per spill request:
    `op,tag,num_objects,total_bytes,dur_ns`.
- `python/ray/_private/external_storage.py`
  - `T_python` — duration of the whole `spill_objects` / `restore_spilled_objects` call.
  - `T_io` — time inside `f.write` / `f.flush` (and the restore read).
  - Writes a CSV row per call: `wall_ts,op,backend,tag,num_objects,total_bytes,t_python_ns,t_io_ns`.

Derived in `analyze_spill.py`: `T_pylogic = T_python - T_io`, `T_rpcgil = T_total - T_python`.

## The before/after control

The only file kept here is the **before-fix** Python variant:

- `external_storage.instrumented.py` — identical timing, but with the original
  `payload = header + memoryview(buf)` concatenation (the avoidable memcpy) instead
  of the two-write fix. The C++ side is unaffected by the fix, so there is no C++
  control copy.

To measure the **before** numbers, drop this file in over the real one (no rebuild
needed for the Python change), run the sweep, then restore the real file for the
**after** numbers:

```sh
cp python/ray/_private/external_storage.py /tmp/external_storage.after.py
cp spill-bench/instrumentation/external_storage.instrumented.py \
   python/ray/_private/external_storage.py
# ... run the sweep (below) into a "before" out_dir ...
cp /tmp/external_storage.after.py python/ray/_private/external_storage.py
```

## Use

Build the C++ (raylet) once, then run the sweep:

```sh
python spill-bench/spill_sweep.py <out_dir> [store_mb]
```

Timing is gated by environment variables (the sweep sets them per object size):

- `RAY_SPILL_TIMING=<python_csv_path>`
- `RAY_SPILL_TIMING_CPP=<cpp_csv_path>`
- `RAY_SPILL_TIMING_TAG=<tag>` (the sweep sets this to the object size)
