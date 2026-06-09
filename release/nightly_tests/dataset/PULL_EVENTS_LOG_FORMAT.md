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

Every event has `phase=...`. The three phases form a per-bundle timeline:

```
T0 = subscribe_start_time   (bundle constructed in PullManager::Pull)
T1 = pullable_time          ← emitted as `phase=bundle_locate`
T2 = active_time            ← emitted as `phase=bundle_active`
T3 = complete_time          ← emitted as `phase=bundle_complete`
```

Join the three lines for one bundle by `req_id` (unique per raylet
process; not globally unique across nodes). To get a cluster-wide unique
key, prefix with `node_ip` (the aggregator does this).

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

### 3.4 What's NOT in this file (yet)

These would be useful additions but are not implemented:

- **`phase=bundle_failed`**: bundle timed out / OBJECT_FETCH_TIMED_OUT
  path. Today these never emit a `bundle_complete`, they just disappear
  from telemetry. Tracking the failure population requires a hook in
  `fail_pull_request_` / `mark_as_failed_`.
- **`phase=spill_restore`**: per-object disk read timing, separating spill
  IO from network IO inside `transfer_ms`. Belongs in
  `LocalObjectManager`/`SpilledObjectReader`, not PullManager.
- **Worker-side `phase=ray_get`**: end-to-end consumer view of `ray.get`,
  emitted from `CoreWorkerPlasmaStoreProvider::Get`. Lives in a separate
  per-worker file (workers and raylet are different processes; sharing
  one spdlog file is unsafe).

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

Run:

```bash
python aggregate_pull_events.py --output pull_events_summary.json
```

---

## 5. Worked example

```
1780900000.100000 phase=bundle_locate req_id=42 bundle_size=8 latency_ms=15.2 warm=0 task=ShuffleReduce
1780900000.115400 phase=bundle_active req_id=42 bundle_size=8 wait_ms=3.4 task=ShuffleReduce
1780900000.602500 phase=bundle_complete req_id=42 bundle_size=8 transfer_ms=487.1 total_ms=502.3 task=ShuffleReduce

1780900000.200000 phase=bundle_locate req_id=43 bundle_size=1 latency_ms=0.0 warm=1 task=-
1780900000.200001 phase=bundle_complete req_id=43 bundle_size=1 transfer_ms=0.0 total_ms=0.0 task=-
```

Interpretation:

- `req_id=42` is a typical reduce-side cold pull: 15 ms to discover where
  each shard lives, 3 ms waiting for plasma quota, 487 ms actual transfer
  (8 shards across the network from various map nodes / spill files).
- `req_id=43` is a driver-side `ray.get` on an object already local — no
  bundle_active line because it was instantly active; the bundle_complete
  fires from the construct-time warm path.

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
3. **Missing `bundle_complete` is also normal**: failed/cancelled bundles
   never reach T3. See §3.4 — we have no `bundle_failed` event yet, so
   these are silently absent. Compute completion ratio as
   `count(bundle_complete) / count(bundle_locate)` to track this.
4. **Timestamps may interleave** across multiple phases and across
   multiple bundles. Always sort by ts before computing per-bundle
   timelines, or group by `req_id` first.
5. **`warm=1` bundles bias latency distributions toward zero.** Filter
   them out (`warm=0`) when reporting "real" cold-fetch percentiles for
   the OOC shuffle path.
6. **Append-only across raylet restarts** (same caveat as spill events):
   use timestamps to scope a query. The benchmark driver truncates the
   file before each run, so under normal use one file ≈ one run.
