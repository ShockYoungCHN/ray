# FreeLocalObjects batching — driver-workload benchmark results

Owner-driven object freeing is a push from the owner: when the driver's refcount
for an object hits zero it sends a `FreeLocalObjects` RPC to each raylet holding a
copy and handles the reply. This benchmark measures **only the driver's workload**
for a large deletion wave — the RPCs it sends and the reply callbacks it handles —
which is exactly what the per-node Nagle-style batching coalesces.

Run with `free_local_objects_driver_benchmark.py` (single node, objects forced into
plasma via `max_direct_call_object_size=0`; `--batched 0` = legacy one-RPC-per-
object, `--batched 1` = coalesced). Counts are read from the driver's
`RAY_event_stats` log.

## Driver workload vs #objects freed (single node, **no injected latency**)

| objects freed | baseline: RPCs sent = reply callbacks | batched: RPCs sent = reply callbacks | reduction |
|--------------:|:-------------------------------------:|:------------------------------------:|:---------:|
|             1 |                    1                  |                   1                  |    1×     |
|           100 |                  100                  |                   3                  |   ~33×    |
|        10,000 |               10,000                  |                  41                  |  ~244×    |
|        50,000 |               50,000                  |                 197                  |  ~254×    |

- Batching is **never worse** than baseline (N=1 is exactly 1 == 1), so there is no
  regression when there is nothing to coalesce.
- At 50k objects the driver sends **197** RPCs / handles **197** reply callbacks
  instead of **50,000** — a **~254×** reduction. (197 ≈ 50000 / 256, the batch cap;
  the per-node backlog peaks at ~41,000 buffered ids before the first reply.)
- No artificial latency is needed: `del refs` generates frees far faster than the
  local RPC round-trip drains them, so a backlog builds and coalesces on its own.

## CPU: driver main-thread flamegraph (N=200,000)

`py-spy --native` of the driver during the wave (SVGs in this directory):

| flamegraph | `CoreWorker::FreeObjectOnNodesAsync` share of driver samples |
|------------|:-----------------------------------------------------------:|
| `driver_baseline.svg` (batched=0) | **~59%** (main thread busy sending one RPC per object) |
| `driver_batched.svg`  (batched=1) | **negligible** (drops out of the top frames) |

The frees run synchronously on the driver's main thread during `del`; batching turns
the per-object RPC send into a cheap buffer append, so the ~59% hotspot disappears.

## Reproduce

```bash
# A/B driver workload
python free_local_objects_driver_benchmark.py --n 50000 --batched 0   # baseline
python free_local_objects_driver_benchmark.py --n 50000 --batched 1   # batched

# flamegraphs (needs py-spy + ptrace_scope=0)
python free_local_objects_driver_benchmark.py --n 200000 --batched 0 --flame
python free_local_objects_driver_benchmark.py --n 200000 --batched 1 --flame

# to also exercise the coalescer under simulated RTT (amplifies the baseline cost):
RAY_testing_asio_delay_us="NodeManagerService.grpc_client.FreeLocalObjects.OnReplyReceived=5000:5000" \
  python free_local_objects_driver_benchmark.py --n 200000 --batched 0
```
