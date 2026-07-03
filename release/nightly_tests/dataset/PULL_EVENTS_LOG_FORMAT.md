# `raylet_pull_events.out` — File Format Reference

Spec for the dedicated pull-event log file written by raylet (C++) from
`PullManager`. Sibling of `raylet_spill_events.out` — same grammar, same
analysis tooling pattern, different phases. Read this first if you want to
write an analysis script against the log.

The events here measure the **fetch path** for object pulls — from "worker
asked for these refs" to "every object is sealed in local plasma". For OOC
shuffle this is the reduce-side fetch latency.

---

## 1. Where the file lives

One file **per node**. Path resolution priority (set in
`PullManager::InitPullEventLogger`, `src/ray/object_manager/pull_manager.cc`):

1. `RAY_PULL_EVENTS_LOG_PATH` env var, if set on that node's raylet process.
2. Fallback: `/tmp/raylet_pull_events.out`.

`benchmark_ooc_shuffle.py` forwards `RAY_PULL_EVENTS_LOG_PATH` to all
workers via `runtime_env` and truncates the file on every node before each
run (pass `--no-truncate-spill-log` to disable; the flag covers both spill
and pull event logs).

**The file is append-only across raylet restarts** (`spdlog::basic_logger_st(..., truncate=false)`),
so a single file can span multiple Ray sessions — timestamps are absolute
and disambiguate runs. Truncation is the Python driver's responsibility,
not spdlog's.

---

## 2. Line format

Identical grammar to `raylet_spill_events.out`. Every line is plain ASCII,
terminated with `\n`. Strict grammar:

```
<unix_seconds>.<microseconds> <token>(\s+<token>)*
```

- **Timestamp**: spdlog pattern `%E.%f` — integer seconds since epoch, dot,
  6-digit microseconds. Example: `1780900000.123456`.
- **Tokens**: space-separated `key=value` pairs. **Token values never
  contain spaces** (parsers can split on whitespace without quoting). The
  one exception today is `task=<name>` where `<name>` may contain symbols
  but never whitespace (Ray Data shuffle task names are like
  `ShuffleMap[ReadParquet]->Repartition`); if you write your own caller
  that emits whitespace in task names, sanitize on the raylet side.
- `task=-` (literal dash) is emitted when the bundle has no associated
  task name (e.g. `ray.get` from the driver).

Canonical parser:

```python
import re
LINE_RE = re.compile(r"^(\d+\.\d+)\s+(.*)$")
KV_RE = re.compile(r"(\w+)=(\S+)")

def parse_line(line):
    m = LINE_RE.match(line.strip())
    if not m:
        return None
    ts, rest = float(m.group(1)), m.group(2)
    return {"ts": ts, **dict(KV_RE.findall(rest))}
```

`aggregate_pull_events.py` uses exactly this.

---

## 3. Event phases

Every event has `phase=...`. Two kinds of events share this file:

**Per-bundle (receiver-side, PullManager)**:

```
T0 = subscribe_start_time   (bundle constructed in PullManager::Pull)
T1 = pullable_time          ← emitted as `phase=bundle_locate`
T2 = active_time            ← emitted as `phase=bundle_active`   (first activation only)
T3 = complete_time          ← emitted as `phase=bundle_complete` (iff fully sealed)
T_cancel                    ← emitted as `phase=bundle_terminal` (always, at CancelPull)
```

`bundle_terminal` is the only phase guaranteed to fire — `bundle_complete`
is absent for bundles that get cancelled before all objects become local.
Use `bundle_terminal.outcome` to identify those.

Join all per-bundle phases for one bundle by `req_id` (unique per raylet
process; not globally unique across nodes). To get a cluster-wide unique
key, prefix with `node_ip` (the aggregator does this).

**Per-object (sender-side, ObjectManager + PushManager)**:

```
phase=push_started          ← HandlePull -> first StartPush (gap = unfulfilled_push_requests_ wait)
phase=push_queued           ← PushManager dequeued this push for first send
phase=push_dispatched       ← all chunks handed to chunk_send_fn_ (in flight)
phase=chunk_read            ← one line per chunk: chunk_reader->GetChunk (spill disk / plasma read)
phase=chunk_sent            ← one line per chunk: gRPC Push RPC RTT (send + receiver ACK)
phase=object_pushed         ← one line per object after all chunks ACKed
```

Emitted by the node that *served* an object to a remote requester.

`phase=push_started` closes the gap between "receiver's PullManager
fired SendPullRequest" and "sender started actually pushing". If the
object was not local on the sender when Pull arrived, it sits in
`unfulfilled_push_requests_` until `HandleObjectAdded` fires — that
wait is entirely inside `pull_to_start_ms`. See §3.8.

`phase=object_pushed` carries the wall-clock for the sender side of the
cross-node transfer and a `from_disk` flag separating spill-served
(`PushFromFilesystem`) from plasma-served (`PushLocalObject`). The byte
sum of `from_disk=1` lines is the *only* place in the entire telemetry
surface where cross-node spill IO is measurable — `restored_*` in
`raylet_spill_events.out` does NOT cover this path. See §3.5.

`phase=push_queued` and `phase=push_dispatched` decompose `push_wall_ms`
into PushManager-internal phases — see §3.9. Join by
`(node_ip, object_id, dest_node)` to derive
`drain_ms = push_wall_ms - queue_ms - dispatch_ms` (the network ack tail).

`phase=chunk_read` and `phase=chunk_sent` further decompose that drain
into per-chunk distributions (§3.10 / §3.11) — critical when you need
to distinguish "one chunk stalled" from "all chunks uniformly slow".

**Per-object (receiver-side, ObjectManager)**:

```
phase=object_first_byte     ← Pull RPC sent -> first chunk arrived
phase=chunk_received        ← one line per chunk arrival, includes gap-from-prev-chunk
phase=plasma_create_wait    ← per-chunk plasma buffer_pool_.CreateChunk wait (all chunks, not just 0)
phase=object_sealed         ← last chunk arrival -> plasma seal notification
```

Emitted on the *receiving* node. `object_first_byte` is
`send_pull_request -> first chunk received`, covering remote dispatch +
remote first IO + network one-way + local dispatch. `chunk_received`
per-chunk arrival timing exposes inter-chunk gaps (sender stall vs
uniform slow). `plasma_create_wait` is the time `buffer_pool_.CreateChunk`
blocks for the receive buffer — includes synchronous plasma eviction /
spill. Fires for every chunk (up from chunk_0 only in earlier builds),
so multi-chunk objects with late-chunk admission stalls are now visible.
`object_sealed` is `last_chunk_arrival -> plasma seal notification`,
catching finalize / metadata-registration cost downstream of the last
chunk. See §3.6–§3.13.

All receiver events fire for any inbound chunk regardless of which
PullManager priority triggered it (GET / WAIT / TASK_ARGS — there is
no other path that lands chunks in ObjectManager).

**Per-node global gauge**:

```
phase=raylet_snapshot       ← 100 ms tick, sender-side backlog counters
```

Snapshots small counters that provide context for per-object stalls —
e.g. a `push_started.pull_to_start_ms=3000` is much easier to explain
against a `raylet_snapshot.unfulfilled_push_count=147` line at the same
timestamp. See §3.14.

### 3.1 `phase=bundle_locate` (T0 → T1)

Time from "PullManager subscribed to object locations" to "every object's
size + location is known and none is pending creation".

```
1780900000.123456 phase=bundle_locate req_id=42 bundle_size=8 latency_ms=15.2 warm=0 task=ShuffleReduce
```

| Field | Type | Notes |
|---|---|---|
| `req_id` | int | PullManager-assigned bundle request id |
| `bundle_size` | int | Number of distinct object refs in the bundle |
| `latency_ms` | float | T1 − T0 in milliseconds |
| `warm` | 0/1 | `1` if all objects were already locally known at bundle construction (fast path: `AddBundlePullRequest` short-circuit, never went through `OnLocationChange`); `0` otherwise |
| `task` | string | Task name if a task-arg request; `-` for raw `ray.get`/`ray.wait` |

A `warm=1` line has `latency_ms=0.0` by definition. Cold lines are the
ones whose distribution you care about.

### 3.2 `phase=bundle_active` (T1 → T2)

Time the bundle spent in `inactive_requests` waiting for plasma quota
before `ActivateNextBundlePullRequest` actually fired the pull.

```
1780900000.138900 phase=bundle_active req_id=42 bundle_size=8 wait_ms=3.4 task=ShuffleReduce
```

| Field | Type | Notes |
|---|---|---|
| `req_id` | int | Matches the bundle_locate line |
| `bundle_size` | int | Same as bundle_locate |
| `wait_ms` | float | T2 − T1 in milliseconds. Large = plasma backpressure |
| `task` | string | Same convention as bundle_locate |

A bundle that's pullable + has plasma headroom flips to active in
microseconds; non-trivial `wait_ms` means scheduler is starving it. Cross-
check `phase=spill_manager_summary.pending_spill_bytes` in
`raylet_spill_events.out` to see whether spill is the cause.

### 3.3 `phase=bundle_complete` (T0 → T3, T1 → T3)

Time from bundle creation to "every object is sealed in local plasma" —
the actual end-to-end fetch wall time as far as raylet sees it.

```
1780900000.642300 phase=bundle_complete req_id=42 bundle_size=8 transfer_ms=487.1 total_ms=502.3 task=ShuffleReduce
```

| Field | Type | Notes |
|---|---|---|
| `transfer_ms` | float | T3 − T1: pure transfer time (network pull / spill restore), excludes location discovery |
| `total_ms` | float | T3 − T0: full end-to-end raylet-side fetch time |

`transfer_ms ≈ total_ms − bundle_locate.latency_ms`. The split is useful
when location discovery is slow (owner pubsub lag) and you want to
attribute it separately from actual transfer.

A `warm=1`-equivalent bundle (all objects already local at construction)
emits `transfer_ms=0.0 total_ms=0.0`.

### 3.4 `phase=bundle_terminal` (per-bundle summary at CancelPull)

Emitted exactly once per bundle at `PullManager::CancelPull`, the only
universal teardown hook. **This is the line you join against to count the
"never completed" population** — bundles that never reach
`phase=bundle_complete` still emit a `bundle_terminal` line with
`outcome=no_complete`.

It also surfaces the **deactivate/reactivate churn** that
`bundle_active.wait_ms` cannot see. `bundle_active` only fires on the
*first* activation, which is structurally near-zero (admission is
opportunistic + preemptive — `ActivateNextBundlePullRequest` kicks lower-
priority bundles out instead of blocking). Real plasma backpressure
manifests *after* T2 as repeated deactivate→reactivate cycles, and shows
up here in `dx_count` / `dx_total_ms`.

```
1780900000.999000 phase=bundle_terminal req_id=42 bundle_size=8 outcome=complete dx_count=3 reax_count=3 dx_memory=2 dx_unpullable=1 dx_total_ms=145.2 lifetime_ms=512.4 task=ShuffleReduce
```

| Field | Type | Notes |
|---|---|---|
| `outcome` | `complete` \| `no_complete` | `complete` iff `bundle_complete` ever fired. `no_complete` = worker called CancelPull before all objects became local (timeout / cancelled / errored worker) |
| `dx_count` | int | Total deactivations excluding the final teardown-cancel (= `dx_memory + dx_unpullable`) |
| `reax_count` | int | Number of re-activations (≤ `dx_count`; equal under normal completion) |
| `dx_memory` | int | Deactivations forced by `DeactivateUntilMarginAvailable` (plasma quota margin) |
| `dx_unpullable` | int | Deactivations because an object lost size/loc info (spill/reconstruct in flight) |
| `dx_total_ms` | float | Cumulative wall-clock spent in `inactive_requests` between activations. Real plasma-backpressure signal |
| `lifetime_ms` | float | `T_cancel − T0`. Always present, even for `no_complete` outcomes |

A `dx_count == 0` bundle had a clean fast path (admit once, never bounced).
`dx_count > 0` with `outcome=complete` shows survivable churn — the
distribution of `dx_total_ms` for these is the most useful single
percentile for "how much did plasma pressure cost me". `outcome=no_complete`
with high `dx_count` shows bundles that got starved out and never
recovered.

### 3.5 `phase=object_pushed` (sender-side, per-object push completion)

Emitted by `ObjectManager::PushObjectInternal` once all chunks of an
object's outbound push have been acknowledged. Captures the wall-clock
duration that the sender node spent serving one object to a remote node
— including local disk read time (for `from_disk=1`, i.e.
`PushFromFilesystem`) and chunked gRPC send time.

```
1780900100.456789 phase=object_pushed object_id=<hex32> bytes=<int> push_wall_ms=<float> get_chunk_ms_total=<float> bounce_lag_ms_total=<float> from_disk=<0|1> dest_node=<hex>
```

| Field | Type | Notes |
|---|---|---|
| `object_id` | hex32 | Ray ObjectID being pushed |
| `bytes` | int | Total object size |
| `push_wall_ms` | float | Wall clock from `StartPush` to the last chunk's send-completion callback fires on rpc_service_. Frozen on rpc_service_ before the emit is posted to main_service_, so it reflects "data on the wire" and is NOT inflated by emit queue latency |
| `get_chunk_ms_total` | float | Sum across all chunks of the time spent inside `chunk_reader->GetChunk`, i.e. reading the payload out of plasma (from_disk=0) or off the spill file (from_disk=1). `push_wall_ms - get_chunk_ms_total` is the network + receiver share (plus any rpc_service_ scheduling slack) |
| `bounce_lag_ms_total` | float | Sum across all chunks of the wait time between "rpc_service_ posted `OnChunkComplete` to main_service_" and "main_service_ actually ran that lambda". Measures contention on the single main_service_ thread that serializes all PushManager state updates. **Authoritative** — the emit is itself posted onto main_service_ after every chunk's bounce lambda (asio io_context is FIFO), so all fetch_add's have completed by the time it reads this value |
| `from_disk` | 0/1 | `1` = `PushFromFilesystem` (read from this node's spill file), `0` = `PushLocalObject` (read from local plasma) |
| `dest_node` | hex | NodeID of the requester. Always different from self_node_id |

**Reading `bounce_lag_ms_total`**: `bounce_lag_ms_total / push_wall_ms`
is the fraction of push wall time attributable to main_service_
scheduling contention (not network, not disk). Elevated values —
especially co-occurring with high `bundle_terminal.dx_total_ms` or
long `plasma_create_wait.wait_ms` on the receiver — point at the
single-threaded main_service_ as the raylet-wide bottleneck.

**Why this lives in pull_events (and not spill_events)**:

1. **Cause attribution: this push is exclusively pull-triggered.**
   `PushObjectInternal` is reached only via two `Push(obj, node)`
   callsites in object_manager.cc — both of which are downstream of a
   remote node's Pull RPC (either immediate `HandlePull`, or queued
   `HandleObjectAdded` for an earlier-unfulfilled Pull). Ray has no
   proactive replication / prefetch / background-eviction-push path
   that hits PushObjectInternal. So every line here corresponds to
   exactly one remote PullManager that decided to fetch this object.
   It's pull-driven traffic by construction; the spill events file is
   about spill *decisions* (what to spill, when, by whom),
   not about pull-driven *reads* of spill files.

2. **Cross-module reference cost: putting it in spill_events would
   require object_manager.cc to depend on local_object_manager's spill
   logger** (currently file-local in `local_object_manager.cc`).
   That's a larger refactor across the raylet ↔ object_manager
   boundary. By contrast, `EmitPullEvent` was already exposed in
   `pull_manager.h` so object_manager.cc can emit with no new
   cross-module API — pull_manager is already an object_manager dep.

3. **Analysis pairing**: a sender-side `phase=object_pushed` line
   directly pairs with a receiver-side `phase=bundle_complete` /
   `bundle_terminal` line on the requesting node. Keeping both in the
   same file format makes cluster-wide join trivial (parse one schema,
   one aggregator) and matches how cross-node fetch latency is
   reasoned about (sender wall time + network + receiver plasma).

**Important: this is the only signal for cross-node spill IO**. The
`restored_*` counters in `raylet_spill_events.out` ONLY cover
`AsyncRestoreSpilledObject` (same-node spill → same-node plasma).
Cross-node spill reads served via `PushFromFilesystem` do not touch any
`restored_*` counter — they only show up here. If you want to know
"how much disk did this cluster read serving shuffle reduces", aggregate
`bytes` over `from_disk=1` lines.

### 3.6 `phase=object_first_byte` (receiver-side, send-Pull → first-chunk)

Emitted by `ObjectManager::HandlePush` the *first* time a chunk of an
object arrives on this node after the local raylet's PullManager fired
the Pull RPC. Chunks may not arrive in order, so "first" is detected via
the presence of a per-object pull-sent timestamp (not `chunk_index == 0`).

```
1780900100.234567 phase=object_first_byte object_id=<hex32> bytes=<int> first_byte_ms=<float> attempt=<int> from_node=<hex>
```

| Field | Type | Notes |
|---|---|---|
| `object_id` | hex32 | Ray ObjectID |
| `bytes` | int | Total object size (from the chunk's `data_size`) |
| `first_byte_ms` | float | Wall clock from `SendPullRequest` to first chunk arrival |
| `attempt` | int | Cumulative count of Pull RPCs ever sent for this object_id on this node. `attempt=1` is first pull; `attempt>1` means **the object was Pull-ed more than once** — either an in-session retry, OR the object once landed in our plasma, was later evicted, and is now being re-fetched (the "evict + re-pull" cycle that drains OOC shuffle throughput) |
| `from_node` | hex | NodeID that sent the chunk (the holder we Pull-ed from) |

**`attempt > 1` interpretation**: if you previously saw an
`object_first_byte` event for the same `object_id` on this node with
`attempt = N-1`, then attempt=N is a re-pull. The aggregator joins on
`(node_ip, object_id)` to compute per-object attempt counts and a
"re-pull byte ratio" — total bytes received from `attempt > 1` events
divided by total received bytes. High ratios indicate wasted disk /
network spent re-fetching evicted objects.

This decomposes naturally into:
`first_byte_ms` ≈ (Pull RPC one-way) + (remote dispatch + remote first
IO, which for `from_disk` paths is `SpilledObjectReader::CreateSpilledObjectReader`
open + first pread) + (first chunk one-way) + (local HandlePush dispatch).

For OOC reduce(`ray.get` GET path), this is the **lower bound on actual
fetch wait** — everything before this is wire / dispatch, after this is
chunked transfer + plasma create.

**When this fires**: any inbound chunk that follows a local Pull RPC.
Covers all three PullManager priorities (GET / WAIT / TASK_ARGS) since
those are the only sources of Pull RPCs — no proactive prefetch in Ray.

**Cleanup behavior**: pull-sent timestamps are erased on first chunk
receive. If a pull is cancelled before any chunk arrives, the entry
leaks until the next pull for the same object overwrites it. Bounded by
in-flight pulls without a first chunk; typically small (sub-thousand).

### 3.7 `phase=plasma_create_wait` (receiver-side, plasma admission)

Emitted by `ObjectManager::ReceiveObjectChunk` for **every** inbound
chunk, timing the wall-clock spent inside `buffer_pool_.CreateChunk`.
That call allocates the plasma buffer slot for that chunk index — and
if plasma is full, it must drive eviction / spill synchronously to
free room. Previously emitted only on `chunk_index == 0`; the current
build emits on every chunk to catch late-chunk stalls in multi-chunk
objects.

```
1780900100.345678 phase=plasma_create_wait object_id=<hex32> chunk_id=<int> bytes=<int> wait_ms=<float> ok=<0|1>
```

| Field | Type | Notes |
|---|---|---|
| `object_id` | hex32 | Ray ObjectID |
| `chunk_id` | int | Chunk index within this object |
| `bytes` | int | Total object size being allocated (not chunk size) |
| `wait_ms` | float | Wall clock inside `buffer_pool_.CreateChunk` for this chunk |
| `ok` | 0/1 | `1` = allocation succeeded; `0` = failed (plasma full or out of disk after spill) |

**Why this matters for OOC GET path**: reduce-task `ray.get` bundles are
GET_REQUEST priority, which is *never* deactivated for memory pressure
(see §3.4 — `bundle_terminal.dx_memory` is structurally 0 for GET). So
the actual plasma-pressure wait does NOT show up in PullManager-side
metrics; it shows up here. A high `plasma_create_wait.wait_ms` p99 with
`ok=1` means "plasma backpressure is forcing receive-side stalls
inside chunked transfer" — exactly the invisible component of
`bundle_complete.transfer_ms` that bundle-level churn metrics miss.

`ok=0` lines indicate plasma rejected the allocation (out of disk after
spill attempt, or duplicate / cancelled — see the loops in
`ReceiveObjectChunk`); the PushManager on the remote will retry,
producing duplicate `plasma_create_wait` lines for the same object_id on
subsequent attempts.

### 3.8 `phase=push_started` (sender-side, HandlePull → first StartPush)

Emitted by `ObjectManager::PushObjectInternal` right before it calls
`push_manager_->StartPush`. `pull_to_start_ms` is the elapsed time
between the sender's `HandlePull` receiving the Pull RPC and the sender
actually starting to push chunks — a gap that is **invisible to every
other metric** (`push_wall_ms` starts at `StartPush`, `push_queued`
starts inside PushManager after StartPush, `bundle_locate` is on the
receiver side entirely).

```
1780900100.111000 phase=push_started object_id=<hex32> bytes=<int> pull_to_start_ms=<float> from_disk=<0|1> dest_node=<hex>
```

| Field | Type | Notes |
|---|---|---|
| `object_id` | hex32 | Object being served |
| `bytes` | int | Total object size |
| `pull_to_start_ms` | float | HandlePull wall-clock → first StartPush call for this (object, dest) |
| `from_disk` | 0/1 | `1` = eventually served via `PushFromFilesystem`; `0` = plasma-local at StartPush time |
| `dest_node` | hex | Requesting NodeID |

**Why this exists**: if the sender receives a Pull RPC for an object it
does not yet have locally (neither in plasma nor spilled), the request
sits in `unfulfilled_push_requests_` until `HandleObjectAdded` fires
for that object. That wait can be arbitrarily long — in the 512 GB
run investigation, 74 % of top-1% tail objects showed
`pull_to_start_ms ≈ 3000` while `push_wall_ms` was under 15 ms,
localizing the tail to this gap and pointing at owner-side location
staleness / syncer broadcast delay as the likely root cause.

**Not emitted** for synthetic re-Push callsites (e.g. resend paths that
don't originate from a fresh Pull RPC and thus don't carry an origin
timestamp).

### 3.9 `phase=push_queued` / `phase=push_dispatched` (sender-side, PushManager internals)

Emitted by `PushManager` directly (not `ObjectManager`). Together with
`phase=object_pushed` they decompose `push_wall_ms` into three phases per
sender-side push:

```
T0 = StartPush()            push enters push_requests_with_chunks_to_send_
T1 = first SendOneChunk()  ← phase=push_queued       queue_ms = T1 - T0
T2 = last  SendOneChunk()  ← phase=push_dispatched   dispatch_ms = T2 - T1
T3 = last  OnChunkComplete ← phase=object_pushed     push_wall_ms ≈ T3 - T0
                              drain_ms = push_wall_ms - queue_ms - dispatch_ms
```

```
1780900100.111111 phase=push_queued     object_id=<hex32> dest_node=<hex> chunks=<int> queue_ms=<float>
1780900100.222222 phase=push_dispatched object_id=<hex32> dest_node=<hex> chunks=<int> dispatch_ms=<float>
```

| Field | Type | Notes |
|---|---|---|
| `object_id` | hex32 | Object being served |
| `dest_node` | hex | Receiving NodeID. Same object can have multiple lines, one per destination |
| `chunks` | int | Total chunks for this push (constant across the two phases) |
| `queue_ms` | float | Wait inside `push_requests_with_chunks_to_send_` for the `chunks_in_flight` window to open. Fires once per push (resends don't re-emit) |
| `dispatch_ms` | float | Time from first to last chunk dispatched. If `dispatch_ms / chunks` ≫ chunk RTT, the throttle is binding |

**Join key**: `(node_ip, object_id, dest_node)`. Same key as `object_pushed`.

**Interpretation matrix**:

| Component | Bottleneck if large |
|---|---|
| `queue_ms` | HOL blocking — other pushes on this sender are saturating `max_chunks_in_flight`, or destination is congested |
| `dispatch_ms` | Window throttle — sender is window-bound, not CPU/disk bound |
| `drain_ms` | Network / receiver — last in-flight chunks slow to ack; check `plasma_create_wait` on the receiver |

**Caveats**:

1. **No emit when logger disabled**: like all `EmitPullEvent` lines, these
   are no-ops unless `RAY_pull_manager_event_log_enabled=1` was set when
   the raylet started.
2. **Resend behavior**: if `StartPush` is called twice for the same
   `(dest, object)` (duplicate Pull from peer), the second call invokes
   `ResendAllChunks` which does NOT reset `first_sent_at_`. So
   `queue_ms` reflects original-enqueue → first-send only; a resend that
   completes earlier than the first attempt will appear as a clean
   single-push timeline.
3. **`HandleNodeRemoved`**: pushes cancelled by node removal are erased
   from `push_requests_with_chunks_to_send_` without going through the
   "num_chunks_to_send_ == 0" path, so `push_dispatched` is NOT emitted
   for them. The aggregator counts these as `queue_only_count`.

### 3.10 `phase=chunk_read` (sender-side, per-chunk read)

Emitted by `ObjectManager::SendObjectChunk` — one line per chunk,
timing the wall-clock inside `chunk_reader->GetChunk(chunk_index)`.
This is the disk-read latency for `from_disk=1` pushes and the
plasma-read latency for `from_disk=0`. `object_pushed.get_chunk_ms_total`
is the sum of these values; this line exposes the per-chunk distribution
so a single slow chunk vs uniform slow can be told apart.

```
1780900100.121000 phase=chunk_read object_id=<hex32> chunk_id=<int> read_ms=<float> from_disk=<0|1> ok=<0|1>
```

| Field | Type | Notes |
|---|---|---|
| `chunk_id` | int | Chunk index within this object |
| `read_ms` | float | Wall clock inside `chunk_reader->GetChunk` |
| `from_disk` | 0/1 | `1` = spill file (`SpilledObjectReader`); `0` = plasma (`MemoryObjectReader`) |
| `ok` | 0/1 | `0` = chunk was evicted between StartPush and this attempt (spill file gone / plasma object freed). Rare but useful to spot |

### 3.11 `phase=chunk_sent` (sender-side, per-chunk gRPC RTT)

Emitted by `SendObjectChunk`'s gRPC completion callback — one line per
chunk, timing wall-clock between `rpc_client->Push()` call and the
callback firing. Covers gRPC serialize + socket write + network one-way
+ receiver `HandlePush` + `send_reply_callback` + return network + gRPC
deserialize. Does NOT include the subsequent `main_service_` bounce
(that's `bounce_lag_ms_total` in `object_pushed`).

```
1780900100.131000 phase=chunk_sent object_id=<hex32> chunk_id=<int> send_ms=<float> bytes=<int> ok=<0|1> dest_node=<hex>
```

| Field | Type | Notes |
|---|---|---|
| `chunk_id` | int | Chunk index |
| `send_ms` | float | gRPC RTT for this chunk |
| `bytes` | int | Chunk payload size |
| `ok` | 0/1 | gRPC-level status |
| `dest_node` | hex | Requesting NodeID |

**Correlation with receiver**: `chunk_sent.send_ms` on sender should
approximate `chunk_received.recv_ms` on receiver (for the same
object_id / chunk_id pair) — cross-node clock skew aside. Large
divergence points at gRPC framing / TCP retransmission.

### 3.12 `phase=chunk_received` (receiver-side, per-chunk arrival)

Emitted by `ObjectManager::HandlePush` — one line per inbound chunk.
`gap_from_prev_ms` is the interval since the last chunk of the same
object arrived on this node. Large gaps at random chunk indices (not
just chunk 0) indicate mid-push stalls: sender was interrupted, or
the network had a hiccup between chunks.

```
1780900100.132000 phase=chunk_received object_id=<hex32> chunk_id=<int> bytes=<int> gap_from_prev_ms=<float> from_node=<hex>
```

| Field | Type | Notes |
|---|---|---|
| `chunk_id` | int | Chunk index |
| `bytes` | int | Chunk payload size |
| `gap_from_prev_ms` | float | Time since the previous chunk of this object arrived; `-1.0` for the first chunk (no previous) |
| `from_node` | hex | Sender NodeID |

### 3.13 `phase=object_sealed` (receiver-side, plasma seal notification)

Emitted by `ObjectManager::HandleObjectAdded` — the moment plasma
finished sealing the object and told us it's now local.
`last_chunk_to_seal_ms` measures the tail: from the last observed
chunk arrival to plasma sending the added-object notification. Large
values indicate plasma finalize / metadata registration cost.

```
1780900100.145000 phase=object_sealed object_id=<hex32> bytes=<int> last_chunk_to_seal_ms=<float>
```

| Field | Type | Notes |
|---|---|---|
| `bytes` | int | Total object size (data + metadata) |
| `last_chunk_to_seal_ms` | float | `HandleObjectAdded` wall-clock − last `chunk_received` wall-clock for this object |

Only emitted for objects that were remotely pulled (locally-produced
objects have no `chunk_received` timeline, so `last_chunk_recv_ns_` is
absent and the line is skipped).

### 3.14 `phase=raylet_snapshot` (per-node global gauge, ~100 ms tick)

Emitted by `ObjectManager::Tick` — one line per raylet per timer tick
(default `object_manager_timer_freq_ms = 100`). Provides context for
per-object stall events, so `push_started.pull_to_start_ms = 3000` can
be correlated with sustained backlog by looking at the snapshot lines
straddling that timestamp.

```
1780900100.100000 phase=raylet_snapshot node=<hex> unfulfilled_push_count=<int> local_objects=<int> used_memory_bytes=<int>
```

| Field | Type | Notes |
|---|---|---|
| `node` | hex | This raylet's NodeID |
| `unfulfilled_push_count` | int | `unfulfilled_push_requests_.size()` — count of (object_id, dest) pairs waiting for `HandleObjectAdded` |
| `local_objects` | int | Objects currently in this node's plasma |
| `used_memory_bytes` | int | Sum of data+metadata bytes for local objects |

Intentionally minimal. Fields to add later if we still need more
context: PullManager active/inactive by priority, PushManager in-flight
chunks, `main_service_` queue length. Those all require plumbing new
accessors across modules.

### 3.15 What's NOT in this file (yet)

These would be useful additions but are not implemented:

- **Sender-side syncer / object_directory broadcast timing** (`phase=syncer_broadcast`
  / `phase=object_directory_notify`): the ~3s tail investigation
  strongly implicates owner-side location broadcast, but capturing it
  requires cross-module instrumentation into `src/ray/ray_syncer` and
  `src/ray/object_manager/ownership_object_directory.cc`. Held until
  data from the current instrumentation set justifies the reach.
- **`phase=spill_restore`**: per-object disk read timing on the
  *local-restore* path (`AsyncRestoreSpilledObject`). Currently only
  cumulative bytes/objects via the `restored_*` counters in spill
  events. Per-object timing would help separate disk IO from RPC
  scheduling latency in the local restore path.
- **Worker-side `phase=ray_get`**: end-to-end consumer view of `ray.get`,
  emitted from `CoreWorkerPlasmaStoreProvider::Get`. Lives in a separate
  per-worker file (workers and raylet are different processes; sharing
  one spdlog file is unsafe).
- **Per-event `phase=bundle_deactivate` / `phase=bundle_reactivate`**:
  individual churn events with their own timestamp. `bundle_terminal`
  aggregates the counts and durations, which is enough for most
  analyses; per-event lines would let you correlate churn timing with
  external events (e.g. spill_manager_summary).

---

## 4. Aggregator-side handling

`aggregate_pull_events.py` (sibling of `aggregate_observ_spill.py`) does
per-node fan-out via NodeAffinity tasks and produces a summary:

- Per-phase histograms (min / p50 / p90 / p99 / max) of `latency_ms` /
  `wait_ms` / `transfer_ms` / `total_ms`
- Per-task-name breakdown (so you can see ShuffleMap vs ShuffleReduce
  bundles separately)
- `warm` ratio: % of bundles that hit the construction fast path
- Bundle size distribution (small bundles are typical for OOC reduce;
  large bundles for fused map-side fetch)
- **`bundle_terminal` outcome counters + churn distribution** per task
  (completion_ratio, dx_count, dx_total_ms — see §3.4)
- **`object_pushed` sender-side aggregation** at the cluster level: total
  bytes / wall_ms split by `from_disk={0,1}`, effective bandwidth, and
  the `from_disk_byte_share` ratio that quantifies "how much of the
  cross-node transfer was served from spill files" (the OOC severity
  signal `restored_*` cannot give)
- **PushManager-internal decomposition** of `push_wall_ms`:
  `queue_ms_per_object` / `dispatch_ms_per_object` / `drain_ms_per_object`
  distributions inside both `from_disk={0,1}` buckets. Joins
  `phase=push_queued` + `phase=push_dispatched` + `phase=object_pushed`
  by `(node_ip, object_id, dest_node)`; orphan counts
  (`queue_only_count` / `dispatch_only_count`) flag pushes that never
  completed (e.g. node removal mid-push)

Run:

```bash
python aggregate_pull_events.py --output pull_events_summary.json
```

---

## 5. Worked example

```
1780900000.100000 phase=bundle_locate   req_id=42 bundle_size=8 latency_ms=15.2 warm=0 task=ShuffleReduce
1780900000.115400 phase=bundle_active   req_id=42 bundle_size=8 wait_ms=3.4 task=ShuffleReduce
1780900000.602500 phase=bundle_complete req_id=42 bundle_size=8 transfer_ms=487.1 total_ms=502.3 task=ShuffleReduce
1780900000.605000 phase=bundle_terminal req_id=42 bundle_size=8 outcome=complete dx_count=2 reax_count=2 dx_memory=2 dx_unpullable=0 dx_total_ms=87.4 lifetime_ms=504.9 task=ShuffleReduce

1780900000.200000 phase=bundle_locate   req_id=43 bundle_size=1 latency_ms=0.0 warm=1 task=-
1780900000.200001 phase=bundle_complete req_id=43 bundle_size=1 transfer_ms=0.0 total_ms=0.0 task=-
1780900000.200500 phase=bundle_terminal req_id=43 bundle_size=1 outcome=complete dx_count=0 reax_count=0 dx_memory=0 dx_unpullable=0 dx_total_ms=0.0 lifetime_ms=0.5 task=-

1780900001.000000 phase=bundle_locate   req_id=44 bundle_size=12 latency_ms=8.1 warm=0 task=ShuffleReduce
1780900001.008200 phase=bundle_active   req_id=44 bundle_size=12 wait_ms=0.1 task=ShuffleReduce
1780900001.510000 phase=bundle_terminal req_id=44 bundle_size=12 outcome=no_complete dx_count=5 reax_count=5 dx_memory=4 dx_unpullable=1 dx_total_ms=341.0 lifetime_ms=510.0 task=ShuffleReduce

1780900002.001000 phase=push_queued     object_id=abc123 dest_node=node-B-hex chunks=64 queue_ms=12.0
1780900002.008000 phase=push_dispatched object_id=abc123 dest_node=node-B-hex chunks=64 dispatch_ms=7.0
1780900002.012300 phase=object_pushed   object_id=abc123 bytes=33554432 push_wall_ms=128.4 get_chunk_ms_total=48.2 bounce_lag_ms_total=6.1 from_disk=1 dest_node=node-B-hex
1780900002.011500 phase=push_queued     object_id=def456 dest_node=node-B-hex chunks=64 queue_ms=0.3
1780900002.012100 phase=push_dispatched object_id=def456 dest_node=node-B-hex chunks=64 dispatch_ms=0.6
1780900002.012800 phase=object_pushed   object_id=def456 bytes=33554432 push_wall_ms=4.2 get_chunk_ms_total=0.5 bounce_lag_ms_total=0.3 from_disk=0 dest_node=node-B-hex
```

Interpretation:

- `req_id=42` typical reduce-side cold pull that completed: 15 ms
  location resolve, 3 ms first-activation wait, 487 ms total transfer.
  `bundle_terminal` shows the transfer time actually hid 2 deactivate→
  reactivate cycles costing 87 ms (`dx_total_ms`, both `dx_memory`); the
  remaining 400 ms was real network/disk work.
- `req_id=43` driver-side `ray.get` on already-local object — clean warm
  path, zero churn.
- `req_id=44` `outcome=no_complete` — bundle was cancelled by the worker
  before all 12 objects became local. 5 churn cycles (4 memory-driven,
  1 from spill), 341 ms of cumulative deactivated time, **never reached
  `bundle_complete`**. This is the class of bundles that's invisible
  without `bundle_terminal`.
- The two object pushes (sender-side from a third node serving objects
  to node-B) decompose cleanly:
  - `abc123` (`from_disk=1`, 32 MiB in 128.4 ms ≈ 250 MB/s effective): 12 ms
    of queue, 7 ms to dispatch all 64 chunks, **109 ms of drain**; of that
    drain, 48 ms is `get_chunk_ms_total` (spill-file read pulling payload
    off disk) and 6 ms is `bounce_lag_ms_total` (main_service_ contention
    on OnChunkComplete). The remaining ~55 ms is network + receiver
    plasma admission (cross-check with `plasma_create_wait.wait_ms` on
    node-B).
  - `def456` (`from_disk=0`, 32 MiB in 4.2 ms ≈ 8 GB/s): 0.3 ms queue,
    0.6 ms dispatch, **3.3 ms drain** — 0.5 ms `get_chunk_ms_total` +
    0.3 ms `bounce_lag_ms_total`, remainder pure network. Clean
    plasma→network path, orders of magnitude faster, illustrating
    exactly why mixing these paths into `bundle_complete.transfer_ms`
    is so opaque.

---

## 6. Gotchas for analysis scripts

1. **`req_id` is per-raylet, not globally unique.** Two nodes will both
   start counting from 0. The aggregator scopes joins by `(node_ip, req_id)`.
2. **Missing `bundle_active` is normal.** Bundles can be cancelled before
   activation, or be activated more than once (deactivate-during-spill
   cycle). The current implementation only emits the first activation.
   A bundle with `bundle_locate` + `bundle_complete` but no
   `bundle_active` typically means it was warm-on-construct or was
   cancelled after locate but before active. Don't error on this — count
   it as "missing data".
3. **Missing `bundle_complete` is expected for failed/cancelled bundles**:
   those never reach T3. To track the failure population, count
   `bundle_terminal.outcome=no_complete` (see §3.4). Completion ratio is
   `count(outcome=complete) / count(bundle_terminal)`. The previous
   workaround of `count(bundle_complete) / count(bundle_locate)` was
   wrong when bundles are still in flight at scan time — `bundle_terminal`
   fires only at CancelPull, so its absence means "still pulling", not
   "failed".
4. **Timestamps may interleave** across multiple phases and across
   multiple bundles. Always sort by ts before computing per-bundle
   timelines, or group by `req_id` first.
5. **`warm=1` bundles bias latency distributions toward zero.** Filter
   them out (`warm=0`) when reporting "real" cold-fetch percentiles for
   the OOC shuffle path.
6. **Append-only across raylet restarts** (same caveat as spill events):
   use timestamps to scope a query. The benchmark driver truncates the
   file before each run, so under normal use one file ≈ one run.
7. **`bundle_active.wait_ms` is not the plasma-pressure signal you want.**
   First-activation latency is structurally near-zero because
   `ActivateNextBundlePullRequest` is opportunistic (passes if quota
   allows or if it's the first active bundle, regardless of quota) and
   preemptive (kicks lower-priority bundles out instead of blocking). To
   measure plasma backpressure use `bundle_terminal.dx_total_ms` and
   `bundle_terminal.dx_memory`, which capture the post-T2 deactivate/
   reactivate churn that admission speed hides.
8. **`object_pushed` lives on the sender, not the receiver.** The node
   that emits this line is the one *serving* the object — typically the
   map worker's node when reduce pulls a shard. To compute "how much
   cross-node spill IO did the cluster do", sum `bytes` over
   `from_disk=1` lines across *all* nodes; do NOT correlate per-node
   `object_pushed` with that same node's `bundle_complete` (they're
   different sides of the wire). `restored_*` in `raylet_spill_events.out`
   does not cover this — it only counts self-served (local spill →
   local plasma) restores. **`object_pushed` is the only telemetry
   surface that quantifies cross-node spill traffic.**
