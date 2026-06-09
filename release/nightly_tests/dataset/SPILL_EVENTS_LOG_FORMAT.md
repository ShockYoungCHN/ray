# `raylet_spill_events.out` — File Format Reference

Spec for the dedicated spill-event log file written by raylet (C++) and
augmented by benchmark drivers (Python). Read this first if you want to
write an analysis script against the log.

---

## 1. Where the file lives

One file **per node**. Path resolution priority:

1. `RAY_SPILL_EVENTS_LOG_PATH` env var, if set on that node's raylet process.
2. Fallback: `/tmp/raylet_spill_events.out`.

The benchmark drivers in this directory propagate `RAY_SPILL_EVENTS_LOG_PATH`
to every Ray worker via `runtime_env` so all nodes converge on the same path.
**The file is append-only across raylet restarts** (`spdlog::basic_logger_st(..., truncate=false)`),
so a single file can span multiple Ray sessions — timestamps are absolute and
disambiguate runs. `benchmark_ooc_shuffle.py` truncates the file on every node
before its workload starts (fan-out via NodeAffinity Ray task), so each run's
analysis sees only that run's events; pass `--no-truncate-spill-log` to keep
the append-across-sessions behavior.

`aggregate_observ_spill.py` fans out a Ray task to every node, reads the local
file, and aggregates centrally. To gather logs externally use SSH or
`anyscale workspace_v2 ssh ... -- cat /tmp/raylet_spill_events.out`.

---

## 2. Line format

Every line is plain ASCII, terminated with `\n`. Strict grammar:

```
<unix_seconds>.<microseconds> <token>(\s+<token>)*
```

- **Timestamp**: spdlog pattern `%E.%f` — integer seconds since epoch, dot,
  6-digit microseconds. Example: `1717428000.123456`.
- **Tokens**: space-separated, each token is **either** `key=value` **or** a
  bare word (rare; only `BENCHMARK_FINALIZE` used to appear bare but the
  current code prefixes it with `phase=`).
- **Token values** never contain spaces. JSON envelopes were rejected
  deliberately so the file survives Ray's stdout dedup canonicalizer
  (`_canonicalise_log_line` strips any token containing a number, which
  would mangle JSON) and stays grep-friendly.

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

This is exactly what `aggregate_observ_spill.py:44` does, with one nit:
the existing impl drops the timestamp by using `LINE_RE.search` and
discarding group(1). Re-implement if you need timing.

---

## 3. Event types

Every event has either a `fn=` field (entry-point breadcrumb) **or** a
`phase=` field (state transition). The two are mutually exclusive on a
single line. Below is the full catalogue.

### 3.1 Entry-point breadcrumbs (`fn=...`)

Emitted at the start of LOM control-flow functions. Useful for
correlating spilled-object events to the trigger path that initiated
them.

| `fn` value | Other fields | When emitted |
|---|---|---|
| `SpillObjectUptoMaxThroughput` | `trigger=<EvictionOnCreate\|ThresholdMonitor\|ExplicitApi>` | `LocalObjectManager::SpillObjectUptoMaxThroughput` entry (`local_object_manager.cc:227`). Every spill cascade begins with one of these. |
| `TryToSpillObjects` | `trigger=<…>` `pinned=<int>` `pending=<int>` | `LocalObjectManager::TryToSpillObjects` entry (`local_object_manager.cc:246`). `pinned` = current pinned object count, `pending` = currently in-flight spill count. |
| `OnObjectSpilled` | `n_ids=<int>` `reply_urls=<int>` `trigger=<…>` | Spill IO worker has reported back to LOM (`local_object_manager.cc:487`). `n_ids` = batch size, `reply_urls` = URL count returned. |
| `ProcessSpilledObjectsDeleteQueue` | `queue_size=<int>` `max_batch=<int>` | Periodic deletion sweep entry (`local_object_manager.cc:635`). |
| `SpillObjectsInternal` | `bytes=<int>` `duration=<float_ms>` `objCnt=<int>` `type=<EvictionOnCreate\|ThresholdMonitor\|ExplicitApi>` | Emitted from `LocalObjectManager::SpillObjectsInternal` after the per-batch spill submission. `bytes` is the total bytes selected for this batch, `duration` is the wall-clock spent inside `SpillObjectsInternal` so far in milliseconds (`(now - start_time) / 1e6`), `objCnt` is the number of objects in the batch, and `type` is the originating spill trigger. **Format quirks** — this is the only line where (a) the literal word `spill` appears between `fn=...` and the first kv (no `=`, so the canonical `KV_RE` silently drops it), and (b) numeric values carry trailing commas (`bytes=12345,`). The aggregator's `_parse_kvs` now strips trailing commas; any new parser must do the same before casting. |

Use these as "session boundaries" — anything between two
`fn=SpillObjectUptoMaxThroughput` lines belongs to one logical spill
batch.

### 3.2 `phase=spilled` (per-object completion)

One line **per object** when its bytes have been confirmed on the spill
storage.

```
1717428000.123456 phase=spilled object_id=<hex32> size=<bytes> trigger=<…> creator=<Unknown|TaskReturn|RayPut>
```

| Field | Type | Notes |
|---|---|---|
| `object_id` | 32-char hex | Ray ObjectID |
| `size` | int (bytes) | Object size in plasma |
| `trigger` | enum string | One of `EvictionOnCreate`, `ThresholdMonitor`, `ExplicitApi`. See `SpillTriggerToString` in `local_object_manager_interface.h:46`. |
| `creator` | enum string | One of `Unknown`, `TaskReturn`, `RayPut`. Currently always `Unknown` until CoreWorker plumbs the value through (`object_manager/common.h:217`). |

### 3.3 `phase=deleted` (per-object delete completion)

One line **per object** when LOM has removed its spill file (the object
left plasma, refcount hit zero, delete queue drained it).

```
1717428005.654321 phase=deleted object_id=<hex32> duration_ms=<float> size=<bytes> trigger=<…> creator=<…>
```

| Field | Type | Notes |
|---|---|---|
| `duration_ms` | float | Wall-clock between this object's `phase=spilled` and `phase=deleted`. Use this for spill-residency-time analysis. |
| Others | — | Same as `phase=spilled`. `trigger`/`creator` are propagated from the original spill, not from the delete trigger. |

The aggregator at `aggregate_observ_spill.py:71` buckets `phase=spilled`
and `phase=deleted` by `(trigger, creator)` pair.

### 3.4 `phase=spill_manager_summary` (raylet self-report, periodic)

Written by `LocalObjectManager::LogSpillManagerSummary` at every
`RecordMetrics` tick (default cadence: `metrics_report_interval_ms`,
~5s). Carries cumulative + instantaneous counters straight from LOM
internal state. **Independent data source** from Prometheus — cross-
check both to detect scraping pipeline bugs.

```
1717428010.000000 phase=spill_manager_summary source=raylet_cpp \
  spilled_bytes_total=<int> spilled_objects_total=<int> spill_time_total_s=<float> \
  spill_throughput_mb=<float> \
  restored_bytes_total=<int> restored_objects_total=<int> \
  restore_time_total_s=<float> restore_throughput_mb=<float> \
  pinned_size_bytes=<int> pinned_count=<int> \
  pending_spill_bytes=<int> pending_spill_count=<int> \
  pending_restore_bytes=<int> pending_restore_count=<int> \
  failed_deletions=<int>
```

| Field | Cumulative? | Source variable |
|---|---|---|
| `spilled_bytes_total` | ✅ | `spilled_bytes_total_` |
| `spilled_objects_total` | ✅ | `spilled_objects_total_` |
| `spill_time_total_s` | ✅ | `spill_time_total_s_` (concurrency-adjusted; **not raw disk time** — see `local_object_manager.cc:310` formula) |
| `spill_throughput_mb` | derived | `spilled_bytes_total_ / spill_time_total_s_ / 1MB`. **This is page-cache completion rate, not disk-write rate** — reasonable values can exceed physical disk bandwidth. |
| `restored_bytes_total` | ✅ | `restored_bytes_total_` |
| `restored_objects_total` | ✅ | `restored_objects_total_` |
| `restore_time_total_s` | ✅ | `restore_time_total_s_` |
| `restore_throughput_mb` | derived | Restore equivalent. |
| `pinned_size_bytes` | instantaneous | `pinned_objects_size_` |
| `pinned_count` | instantaneous | `pinned_objects_.size()` |
| `pending_spill_bytes` | instantaneous | `num_bytes_pending_spill_` |
| `pending_spill_count` | instantaneous | `objects_pending_spill_.size()` |
| `pending_restore_bytes` | instantaneous | `num_bytes_pending_restore_` |
| `pending_restore_count` | instantaneous | `objects_pending_restore_.size()` |
| `failed_deletions` | ✅ | `num_failed_deletion_requests_` |

> Cumulative counters **never reset within a raylet process lifetime**.
> To get per-benchmark deltas, snapshot two summary lines that bracket
> your workload and subtract.

### 3.5 `phase=benchmark_finalize` (Python-written, one per benchmark run)

Written by `spill_metrics_dump.write_benchmark_summary_to_raylet_logs`
from the driver, fanned out to every node via a NodeAffinity Ray task.
Format:

```
1717428100.000000 phase=benchmark_finalize tag=<run_id> endpoint=<ip>:<port> \
  delta.<metric_short>.<label>=<float> ... \
  after.<metric_short>.<label>=<float> ...
```

Where:

- `tag` = the benchmark driver's run identifier (e.g. timestamp `20260603_143000`).
- `endpoint` = the node's raylet metrics export endpoint (`ip:port`).
- `delta.*` fields = (post-benchmark Prometheus snapshot) − (pre-benchmark snapshot), computed by `spill_metrics_dump.compute_metrics_delta`. **Only on cumulative metrics**; instantaneous gauges (e.g. `objects.Pinned`) pass through the post value.
- `after.*` fields = raw cumulative Prometheus snapshot at benchmark end (matches whatever Grafana sees).

The `<metric_short>` is the Prometheus metric name with the
`ray_spill_manager_` prefix stripped. The `<label>` is the dominant
label value (`State` for `objects` / `objects_bytes`, `Type` for
`request_total` / `throughput_mb`), or `all` if unlabeled. Concrete
examples:

```
delta.throughput_mb.Spilled=234.5
delta.objects_bytes.Spilled=8456789012
after.throughput_mb.Spilled=637.7
after.objects_bytes.Pinned=35485729245
after.objects_count.Pinned=61521
```

Use these lines to recover per-benchmark deltas without re-scraping
Prometheus. They are the Python-side dual of `phase=spill_manager_summary` —
**cross-checking the two** is the primary use case (raylet's flat
counters vs. Prometheus's gauge pipeline; divergence flags a scraper bug).

---

## 4. Aggregator-side handling

`aggregate_observ_spill.py` already implements a node-fan-out + summary:

```python
@ray.remote(num_cpus=0)
def _scan_node(log_glob: str) -> Dict[str, Any]:
    events = []
    for p in glob.glob(log_glob):
        with open(p) as f:
            for line in f:
                m = LINE_RE.search(line)
                if not m:
                    continue
                events.append(_parse_kvs(m.group(1)))
    return {"node_ip": ray.util.get_node_ip_address(), "events": events}
```

It buckets events by `phase` and treats `phase=benchmark_finalize` and
`phase=spill_manager_summary` as separate counters (not in the
spilled/deleted histogram bins). Replicate that policy in any new
analysis script.

---

## 5. Worked example

```
1717428000.100000 fn=SpillObjectUptoMaxThroughput trigger=EvictionOnCreate
1717428000.250000 fn=TryToSpillObjects trigger=EvictionOnCreate pinned=1024 pending=0
1717428000.510000 fn=OnObjectSpilled n_ids=16 reply_urls=16 trigger=EvictionOnCreate
1717428000.510100 phase=spilled object_id=ffffffffffffffff0e8b25c302000000 size=33554432 trigger=EvictionOnCreate creator=Unknown
1717428000.510120 phase=spilled object_id=ffffffffffffffff0e8b25c303000000 size=33554432 trigger=EvictionOnCreate creator=Unknown
...
1717428005.000000 phase=spill_manager_summary source=raylet_cpp spilled_bytes_total=536870912 spilled_objects_total=16 spill_time_total_s=0.42 spill_throughput_mb=1219.5 restored_bytes_total=0 restored_objects_total=0 restore_time_total_s=0.0 restore_throughput_mb=0.0 pinned_size_bytes=2147483648 pinned_count=64 pending_spill_bytes=0 pending_spill_count=0 pending_restore_bytes=0 pending_restore_count=0 failed_deletions=0
...
1717428060.000000 fn=ProcessSpilledObjectsDeleteQueue queue_size=16 max_batch=64
1717428060.025000 phase=deleted object_id=ffffffffffffffff0e8b25c302000000 duration_ms=59515.0 size=33554432 trigger=EvictionOnCreate creator=Unknown
...
1717428120.000000 phase=benchmark_finalize tag=20260603_142800 endpoint=10.0.31.125:8085 delta.throughput_mb.Spilled=634.2 delta.objects_bytes.Spilled=4294967296 after.throughput_mb.Spilled=1219.5 after.objects_bytes.Pinned=2147483648 after.objects_count.Pinned=64
```

Interpretation:

- A spill cascade started at t=100ms (EvictionOnCreate trigger), spilled 16 objects of 32 MiB each (~512 MiB total).
- At t=5s the raylet's self-reported summary records 16 cumulative objects and 512 MiB spilled, with `spill_throughput_mb=1219.5` page-cache rate.
- At t=60s the delete queue ran, each object had been on disk for ~59.5s (`duration_ms`).
- At t=120s a Python benchmark driver fanned out a finalize line on this node. The `delta` fields are this run's contribution; the `after` fields match the raylet's own summary at t=5s (modulo any further activity).

---

## 6. Gotchas for analysis scripts

1. **Append-only across sessions.** The file is not truncated on raylet restart. Use timestamps to scope a query. Don't assume the first line is from the current session.
2. **Per-node files.** Always fan out (Ray task or SSH) before aggregating. Bringing one file back gives you a single node's view.
3. **Logs may interleave between raylet and benchmark writes.** Both processes hold their own spdlog/std-fileio handles to the same path. Lines are atomic (single `write()`), but order across writers is not guaranteed within the same millisecond.
4. **`spill_throughput_mb` is NOT raw disk throughput.** See section 3.4 caveat. It's `spilled_bytes / (concurrency-adjusted wall-time)`, and the underlying I/O writes go to the OS page cache, not synced disk. Real disk throughput is below this; use `iostat -dx` on each node for the physical rate.
5. **Cumulative counters never reset within a raylet lifetime.** `spilled_bytes_total` keeps growing. For per-benchmark numbers, either (a) compute deltas between two `phase=spill_manager_summary` lines bracketing the run, or (b) use the `phase=benchmark_finalize` line's `delta.*` fields if the run wrote one.
6. **Stdout dedup is separate.** Ray's `RAY_DEDUP_LOGS` only affects what reaches the driver's stdout from `RAY_LOG(INFO)` lines (e.g. the `Spilled X MiB ... write throughput Y` line). It **does not** affect this dedicated log file — every event is written verbatim, every time.
7. **`object_id` is hex but length-32 only when serialized as the canonical 16-byte ObjectID. Don't substring-match on it.**
8. **Empty-line tolerance.** spdlog occasionally rotates with extra newlines if the process crashes mid-write. Parsers should skip lines that don't match the `LINE_RE` regex (the aggregator already does).
