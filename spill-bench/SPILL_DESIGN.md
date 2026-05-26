# Ray Object Spilling: Current Design & Proposed Changes

Status: draft for discussion. Branch context: `reform-spill` / `data/base-shuffle-operator`.
Audience: Ray core devs evaluating moving spill off the Python IO-worker path.

---

## 1. Current design

### 1.1 Component split (who decides what)

Spilling is a **C++ control plane + Python data plane** design.

- **C++ decides WHEN and WHAT to spill.**
  - Plasma store detects memory pressure on object creation and fires
    `spill_objects_callback_` (`src/ray/object_manager/plasma/create_request_queue.cc`).
  - `LocalObjectManager` (`src/ray/raylet/local_object_manager.cc`) selects objects,
    enforces batch/fusion limits, and manages a pool of IO workers.
- **Python executes the actual storage I/O.**
  - `python/ray/_private/external_storage.py` implements the storage backends and the
    serialization/fusion file format.

### 1.2 The boundary (control path)

```
PlasmaStore (C++ thread)
  └─ spill_objects_callback_  ──post──▶ NodeManager event loop
       └─ LocalObjectManager::SpillObjectUptoMaxThroughput
            └─ PopSpillWorker  (IO worker pool, raylet, C++)
                 └─ gRPC: SpillObjects(request)  ─────────────▶ IO worker process
                                                                  CoreWorker::HandleSpillObjects (C++)
                                                                    └─ options_.spill_objects(...)   (std::function callback)
                                                                         └─ spill_objects_handler  (Cython, _raylet.pyx, takes GIL)
                                                                              └─ external_storage.spill_objects (Python)
```

Key facts:
- raylet and the IO worker are **separate processes** on the same host; the hop between
  them is **real gRPC over loopback** (`SpillObjectsRequest` carries only object refs,
  not data).
- Inside the IO worker process, C++ (`CoreWorker`, a gRPC server) and Python coexist in
  one address space. The C++ handler invokes Python via a registered `std::function`
  callback (`CoreWorkerOptions.spill_objects`, set in `_raylet.pyx`). This is an
  in-process call guarded by the **GIL**, not gRPC.

### 1.3 Data path (where bytes actually move)

The object data is **never put on the gRPC wire**. The IO worker reads it directly from
the local plasma shared-memory segment:

- **Spill:** `get_if_local` returns a zero-copy `Buffer` wrapping the plasma mmap pointer
  (`_raylet.pyx`); `_write_multiple_objects` builds a header and calls
  `f.write(memoryview(buf))`. **One copy: plasma → file.** This copy is intrinsic I/O —
  C++ would pay it too.
- **Restore:** plasma allocates a destination buffer, then `file.readinto(memoryview)`
  reads bytes straight into it (`_raylet.pyx::put_file_like_object`). **One copy: file →
  plasma** (+ a negligible metadata copy).

Conclusion: the Python path adds **no extra data copy**. The only avoidable overheads are
the **gRPC round trip**, the **GIL acquisition**, and **Python interpreter time** for the
serialization/fusion logic.

### 1.4 IO worker pool

`IOWorkerPoolInterface` (`src/ray/raylet/worker_pool.h`) maintains pre-started,
**reused** worker processes for spill / restore / delete (not started per I/O). Workers
are popped before a request and pushed back after. Lazily grows up to a cap
(`max_io_workers`). So the per-spill cost is *pop worker + one gRPC call*, not process
startup.

### 1.5 Storage backends (Python)

- `FileSystemStorage` — local/NFS disk; round-robins over directories; fuses many objects
  into one file (`file://path?offset=&size=`).
- `ExternalStorageSmartOpenImpl` — remote via `smart_open` (S3/GCS/HDFS/Azure). S3 path
  reuses a boto3 session and defers seek.
- These are pluggable in **pure Python** via `object_spilling_config` — a real extension
  point for users.

### 1.6 Cross-node reads of spilled objects

`spilled_node_id` decides the read path:
- **Local FS spill** (`is_external_storage_type_fs=true`): `spilled_node_id` = spilling
  node. A remote puller sends a pull request to that node, which **reads the spill file
  and streams chunks** (`ObjectManager::PushFromFilesystem`) — it does *not* re-load into
  its own plasma first.
- **Distributed storage** (S3/HDFS): `spilled_node_id` = Nil. Any node **restores
  directly** from the URL into its own plasma (`PullManager` → `AsyncRestoreSpilledObject`).

The owner only publishes location metadata; it is never in the data path.

---

## 2. Bottleneck analysis (measured)

Instrumentation added (durations, monotonic clocks; no cross-process timestamp alignment):
- C++ `T_total` = `steady_clock` from just before `rpc_client()->SpillObjects()` to the
  reply callback (`local_object_manager.cc`).
- Python `T_python` = duration of `external_storage.spill_objects`; `T_io` = time inside
  `f.write`/`f.flush` (`external_storage.py`, gated by `RAY_SPILL_TIMING`).
- Derived: `T_pylogic = T_python − T_io`; `T_rpc_gil = T_total − T_python`.

See `RESULTS.md` (generated by `analyze_spill.py`) for the size sweep on the filesystem
backend. Headline: for ~1 MB objects the **gRPC+GIL fixed overhead dominates** while disk
I/O is a minority of the time; as object size grows, I/O takes over and the fixed overhead
becomes negligible. This matches the shuffle workload regime (0.5–1 MB intermediate
objects, produced in the hundreds of thousands to millions).

---

## 3. Proposed changes

Ordered by effort/risk vs payoff.

### Option A — Tune batching/fusion (cheapest, do first)
Increase the spill batch size / fusion so each `SpillObjects` RPC amortizes the fixed
gRPC+GIL cost over more objects. Zero architectural change; likely meaningful for the
small-object regime. Validate against the measured `T_rpc_gil` per batch.

### Option B — C++ fast path for local filesystem spill (targeted, recommended)
For `type=filesystem` only, perform spill/restore **inside the raylet in C++**, bypassing
the IO-worker gRPC hop and the Python interpreter entirely:
- raylet reads object data from plasma (already mmap'd in-process on the raylet side via
  the object manager) and writes the fusion format to disk directly.
- Reuse the existing C++ reader (`SpilledObjectReader`) which already parses this exact
  format for cross-node reads — so only the **writer** is new.
- S3/HDFS and user-defined Python backends keep the current path (fallback by backend
  type).

Payoff: removes `T_rpc_gil` for the dominant shuffle case; no new data copy. Scope is
bounded to the fs backend + the write format. Risk: must preserve the on-disk format and
the `spilled_node_id` / object-directory reporting semantics.

### Option C — Full migration of all backends to C++ (not recommended now)
Would require a C++ unified storage abstraction (e.g. `arrow::fs`, which Ray already
depends on and provides Local/S3/GCS/HDFS) and would break the pure-Python custom-backend
extension point. High effort, and for remote backends the win is ~0 because network I/O
dominates. Revisit only if the Python extension point can be preserved (e.g. keep Python
backends as an opt-in slow path).

### Non-goals / things NOT worth doing
- Replacing the gRPC transport layer (UDS/shared-memory queue): large blast radius across
  all worker RPCs for a ~100 µs saving that is dwarfed by I/O for non-tiny objects.
- Eliminating data copies: the path is already zero-extra-copy.

---

## 4. Open questions
- What is the real fusion batch size under the shuffle workload, and how close is it to the
  point of diminishing returns for Option A?
- For Option B, how to keep delete/GC (`url_ref_count_`, `ProcessSpilledObjectsDeleteQueue`)
  consistent when the writer moves into the raylet.
- Does moving fs spill into the raylet event loop risk blocking it on slow disks? (Likely
  need a dedicated C++ thread pool for the blocking writes, mirroring the IO-worker
  isolation that exists today.)
