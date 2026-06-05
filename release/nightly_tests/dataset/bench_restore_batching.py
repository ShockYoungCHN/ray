"""Microbenchmark: Python restore_spilled_objects file-IO batching.

Compares the per-object "open + seek + read" pattern (pre-change) against
the new "group by base_url + sort by offset + open once + sequential read"
pattern (post-change).  Bypasses plasma — measures only the file IO half
that the C++ LOM batching enables.

Usage on devbox:
    python3 bench_restore_batching.py                      # warm cache, 64 KiB / obj
    python3 bench_restore_batching.py --cold               # drop page cache between runs
    python3 bench_restore_batching.py --obj-size 1048576   # 1 MiB / obj
"""

import argparse
import os
import statistics
import sys
import tempfile
import time
from collections import defaultdict
from typing import List, Tuple


# ---------------------------------------------------------------------------
# Two implementations: "unbatched" (one open per object) and "batched"
# (group by file, sort by offset, open once + sequential read).
# ---------------------------------------------------------------------------


def restore_unbatched(items: List[Tuple[str, int, int]]) -> int:
    """Old behavior: open file once per object, in caller order.

    Each item is (base_url, offset, header+payload bytes).  This is what
    external_storage.py:337 did before this change.
    """
    total = 0
    for base_url, offset, _size in items:
        with open(base_url, "rb") as f:
            f.seek(offset)
            address_len = int.from_bytes(f.read(8), byteorder="little")
            metadata_len = int.from_bytes(f.read(8), byteorder="little")
            buf_len = int.from_bytes(f.read(8), byteorder="little")
            _owner = f.read(address_len)
            _metadata = f.read(metadata_len)
            _payload = f.read(buf_len)
            total += buf_len
    return total


def restore_batched(items: List[Tuple[str, int, int]]) -> int:
    """New behavior: group by base_url, sort offsets ascending, open once,
    posix_fadvise SEQUENTIAL so kernel prefetches.  Mirrors the production
    external_storage.py rewrite."""
    by_file: dict = defaultdict(list)
    for i, (base_url, offset, size) in enumerate(items):
        by_file[base_url].append((i, offset, size))

    total = 0
    for base_url, group in by_file.items():
        group.sort(key=lambda t: t[1])
        with open(base_url, "rb") as f:
            try:
                os.posix_fadvise(f.fileno(), 0, 0, os.POSIX_FADV_SEQUENTIAL)
            except (AttributeError, OSError):
                pass
            for _i, offset, _size in group:
                f.seek(offset)
                address_len = int.from_bytes(f.read(8), byteorder="little")
                metadata_len = int.from_bytes(f.read(8), byteorder="little")
                buf_len = int.from_bytes(f.read(8), byteorder="little")
                _owner = f.read(address_len)
                _metadata = f.read(metadata_len)
                _payload = f.read(buf_len)
                total += buf_len
    return total


# ---------------------------------------------------------------------------
# Synthetic spill file.  Format matches external_storage.py's
# _write_multiple_objects layout: <address_len><metadata_len><buf_len>
# <owner_address><metadata><payload>.
# ---------------------------------------------------------------------------


def make_spill_file(
    path: str, n_objects: int, obj_size_bytes: int
) -> List[Tuple[str, int, int]]:
    """Create one fused spill file holding ``n_objects`` of size
    ``obj_size_bytes`` each.  Returns the list of (path, offset, total_size)
    tuples in original write order (mimics how the C++ side produces
    fused URLs)."""
    address_len = 16
    metadata_len = 8
    items: List[Tuple[str, int, int]] = []
    with open(path, "wb") as f:
        for _ in range(n_objects):
            offset = f.tell()
            f.write(address_len.to_bytes(8, "little"))
            f.write(metadata_len.to_bytes(8, "little"))
            f.write(obj_size_bytes.to_bytes(8, "little"))
            f.write(b"\x00" * address_len)
            f.write(b"\x00" * metadata_len)
            # Use os.urandom so the kernel can't dedup pages — keeps the
            # benchmark honest under disk-cache pressure.
            f.write(os.urandom(obj_size_bytes))
            total = 24 + address_len + metadata_len + obj_size_bytes
            items.append((path, offset, total))
    # fsync so the file is durably on disk for the cold-cache run.
    with open(path, "rb") as f:
        os.fsync(f.fileno())
    return items


def drop_page_cache() -> bool:
    """Best-effort drop of OS page cache.  Returns True on success.  No-op
    on macOS / non-root Linux."""
    try:
        with open("/proc/sys/vm/drop_caches", "w") as f:
            f.write("3\n")
        return True
    except (PermissionError, FileNotFoundError, OSError):
        return False


# ---------------------------------------------------------------------------
# Bench driver
# ---------------------------------------------------------------------------


def time_one(
    fn, items: List[Tuple[str, int, int]], cold: bool
) -> Tuple[float, int]:
    """Run ``fn(items)`` once, optionally with cold page cache.  Returns
    (wall_seconds, bytes_restored)."""
    if cold:
        if not drop_page_cache():
            print(
                "WARNING: could not drop page cache (need sudo or /proc); "
                "results are warm-cache.",
                file=sys.stderr,
            )
    start = time.perf_counter_ns()
    total = fn(items)
    elapsed_s = (time.perf_counter_ns() - start) / 1e9
    return elapsed_s, total


def bench_one_case(
    n_objects: int,
    obj_size_bytes: int,
    repeats: int,
    cold: bool,
    different_files: bool,
) -> None:
    """Run repeats × {unbatched, batched} for the given config and print
    summary."""
    tmpdir = tempfile.mkdtemp(prefix="bench_restore_")
    try:
        # Build items.  When different_files=True, every object lives in
        # its own file (the worst case for batching — nothing to group).
        # When False, all objects share one fused file (the best case).
        if different_files:
            items: List[Tuple[str, int, int]] = []
            for i in range(n_objects):
                path = os.path.join(tmpdir, f"spill_{i:05d}.bin")
                items += make_spill_file(path, 1, obj_size_bytes)
        else:
            path = os.path.join(tmpdir, "spill_fused.bin")
            items = make_spill_file(path, n_objects, obj_size_bytes)

        unbatched_times = []
        batched_times = []
        for _ in range(repeats):
            t_u, b_u = time_one(restore_unbatched, items, cold)
            t_b, b_b = time_one(restore_batched, items, cold)
            assert b_u == b_b, f"Different bytes restored {b_u} vs {b_b}"
            unbatched_times.append(t_u)
            batched_times.append(t_b)

        u_med = statistics.median(unbatched_times) * 1000
        b_med = statistics.median(batched_times) * 1000
        speedup = u_med / b_med if b_med > 0 else float("inf")
        layout = "1-file-per-obj" if different_files else "single-fused-file"
        cache = "cold" if cold else "warm"
        print(
            f"  n_objects={n_objects:5d}  "
            f"obj_size={obj_size_bytes // 1024:>5d} KiB  "
            f"layout={layout:>17s}  cache={cache:>4s}  "
            f"unbatched_median={u_med:7.2f} ms  "
            f"batched_median={b_med:7.2f} ms  "
            f"speedup={speedup:5.2f}x"
        )
    finally:
        import shutil
        shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--obj-size", type=int, default=64 * 1024,
        help="Per-object payload size in bytes (default 64 KiB).",
    )
    parser.add_argument(
        "--cold", action="store_true",
        help="Drop /proc/sys/vm/drop_caches between runs.  Needs root.",
    )
    parser.add_argument(
        "--include-1-file-per-obj", action="store_true",
        help="Also run the worst-case layout where each object lives in "
             "its own file.",
    )
    args = parser.parse_args()

    n_objects_grid = [1, 5, 20, 100, 500]

    print(
        f"Bench restore_spilled_objects: "
        f"obj_size={args.obj_size // 1024} KiB, "
        f"repeats={args.repeats}, "
        f"cache={'cold' if args.cold else 'warm'}\n"
    )

    # Case A: fused single-file (best case — what real spill produces).
    print("Layout: single-fused-file (every object in same spill file)")
    print("-" * 100)
    for n in n_objects_grid:
        bench_one_case(
            n_objects=n,
            obj_size_bytes=args.obj_size,
            repeats=args.repeats,
            cold=args.cold,
            different_files=False,
        )
    print()

    if args.include_1_file_per_obj:
        # Case B: pathological worst case for batching — every object is
        # in its own file, so by_file groups have size 1 each, no
        # sequential-read benefit, only the dict-grouping overhead.
        print("Layout: 1-file-per-obj (worst case for batching)")
        print("-" * 100)
        for n in n_objects_grid:
            bench_one_case(
                n_objects=n,
                obj_size_bytes=args.obj_size,
                repeats=args.repeats,
                cold=args.cold,
                different_files=True,
            )
        print()


if __name__ == "__main__":
    main()
