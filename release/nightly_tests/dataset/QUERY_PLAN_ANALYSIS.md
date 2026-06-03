# TPC-H Query Plan Analysis

Per-query breakdown of the physical plan, join types, shuffle volume per
barrier, and where the bottleneck is. Reference column for sizing
spill / benchmark experiments.

Defaults assumed throughout:
- `--num-partitions 500`
- `--strategy hash_shuffle`
- `--enable-v2-join` → joins go through `ShuffleMapOp + ShuffleReduceOp`. Without the flag, joins go through v1 `HashShufflingOperatorBase` (actor-pool, `HashShuffleAggregator`); plan tree shape stays the same, only the operator class differs.
- `DataContext.shuffle_strategy=HASH_SHUFFLE` → final `Aggregate` always goes through the v2 `HashAggregateMap/Reduce` path with map-side pre-agg.

TPC-H row counts (sf=1 baseline; multiply by sf):

| Table | rows | typical column-subset size |
|---|---|---|
| region | 5 | < 1 KB |
| nation | 25 | < 2 KB |
| supplier | 10,000 | ~1 MB |
| customer | 150,000 | ~25 MB |
| orders | 1,500,000 | ~170 MB |
| lineitem | ~6,000,000 | ~750 MB |

---

## Q1 — Pricing Summary Report

**Intent:** Aggregate lineitem by `(l_returnflag, l_linestatus)`, 4 distinct
groups in the whole dataset.

**Plan:**
```
Read lineitem (7 cols) → Filter l_shipdate ≤ 1998-09-02
  → MapBatches add_column disc_price
  → MapBatches add_column charge
  → ShuffleMapOp[HashAggregateMap(keys=(l_returnflag,l_linestatus), partitions=500)]
  → ShuffleReduceOp[HashAggregateReduce]
```

**Joins:** none.

**Shuffle barriers:** 1 (final aggregate).

**Per-stage shuffle volume (sf=1 baseline; ×sf at scale):**

| Stage | Input | Map-side pre-agg collapses? | Bytes through plasma |
|---|---|---|---|
| HashAggregateMap | filtered lineitem ~98% of 750 MB | **Yes — 4 groups → ~4 rows per map task** | tiny: `N_map_tasks × 4 rows × ~80 bytes` ≈ KB, **independent of sf** |
| HashAggregateReduce | tiny | n/a | tiny |

**Bottleneck:** the **scan + filter pipe** (Read → MapBatches), not the shuffle.
Lineitem is read in full, decompressed, filter-pushdown applies but most of the
data still flows through MapBatches. CPU bound on Parquet decode + Arrow compute.
This is the only query in the suite where shuffle is **not** the dominant cost.

**Workload pattern:** extreme fan-in (millions of rows → 4 groups). Great test
for map-side pre-aggregation correctness, useless for spill stress (shuffle
bytes ≈ 0).

---

## Q3 — Shipping Priority

**Intent:** Top-N customer orders by `revenue = l_extendedprice * (1 - l_discount)`,
grouped by `(l_orderkey, o_orderdate, o_shippriority)`.

**Plan:**
```
Read customer (2 cols) ─ Filter c_mktsegment="BUILDING" ─┐
                                                         ├→ Join (on c_custkey = o_custkey)
Read orders (4 cols) ─ Filter o_orderdate < 1995-03-15 ──┘   │
                                                             ▼
Read lineitem ── Filter l_shipdate > 1995-03-15 ── add_column revenue
                                                             │
                                                             ▼
                                  Join (on o_orderkey = l_orderkey)
                                                             │
                                                             ▼
                              ShuffleMapOp[HashAggregateMap(
                                keys=(o_orderkey, o_orderdate, o_shippriority))]
                                                             │
                                                             ▼
                              ShuffleReduceOp[HashAggregateReduce]
```

**Joins:** 2× INNER PK/FK.

| Join | Left side rows (sf=1) | Right side rows | Output rows |
|---|---|---|---|
| `customer ⋈ orders` | ~30K (BUILDING ~20%) | ~750K (date filter ~50%) | ~150K |
| `(co) ⋈ lineitem` | ~150K | ~3M (date filter ~50%) | ~600K |

**Shuffle barriers:** 3 (2 joins + final aggregate).

**Per-stage shuffle volume (per sf=1):**

| Stage | Bytes through plasma |
|---|---|
| Join 1 (`c_custkey`) | ~5 MB customer + ~85 MB orders = ~90 MB |
| Join 2 (`o_orderkey`) | ~20 MB co + ~370 MB lineitem (filtered) = ~390 MB |
| Final groupby | ~600K rows × 30 B ≈ 18 MB (high cardinality, **pre-agg can't collapse much** — each (orderkey, date, priority) tuple has 1-4 rows from join, so pre-agg shrinks by ~3× at best) |

**Bottleneck:** Join 2 (co ⋈ lineitem) — the lineitem-scale shuffle. Final
aggregate is the only high-cardinality groupby in the whole suite (~150K-600K
unique keys × sf, not collapsible).

**Workload pattern:** chained PK/FK joins + **high-cardinality groupby** —
exercises the path where map-side pre-agg fails to collapse rows. This is the
one query where final reduce is non-trivial work.

---

## Q4 — Order Priority Checking (semi-join)

**Intent:** Count orders in a 3-month window that have ≥1 lineitem with
`l_commitdate < l_receiptdate` (late-shipped), grouped by priority.

**Plan:**
```
Read orders (3 cols) ─ Filter o_orderdate ∈ [1993-07, 1993-10) ─┐
                                                                ├→ Join (LEFT_SEMI)
Read lineitem (3 cols) ─ Filter l_commitdate < l_receiptdate ───┘
                                                                  │
                                                                  ▼
                                ShuffleMapOp[HashAggregateMap(keys=(o_orderpriority))]
                                                                  │
                                                                  ▼
                                ShuffleReduceOp[HashAggregateReduce]
```

**Joins:** 1× **LEFT_SEMI** (`on=("o_orderkey",), right_on=("l_orderkey",)`).

| Join | Left side rows (sf=1) | Right side rows | Output rows |
|---|---|---|---|
| `orders LEFT_SEMI lineitem` | ~75K (date filter ~5%) | ~3M (commit<receipt ~63%) | ~70K (orders with ≥1 late ship) |

**Shuffle barriers:** 2 (semi-join + final aggregate).

**Per-stage shuffle volume (per sf=1):**

| Stage | Bytes through plasma |
|---|---|
| LEFT_SEMI shuffle | ~5 MB orders + ~150 MB lineitem (filtered) = ~155 MB. **Note:** LEFT_SEMI shuffle still moves all matching right-side rows through plasma even though they're dropped from the output. Same shuffle cost as INNER, only the reducer drops the right columns. |
| Final groupby | 5 distinct priorities → pre-agg collapses to ~5 rows per map task → tiny |

**Bottleneck:** the semi-join, dominated by **filtered lineitem ~150 MB × sf**.

**Workload pattern:** **only query in the suite that exercises the LEFT_SEMI
code path**. q3/q5/q12 are all INNER. Important coverage for the V2 join
implementation since `JoiningAggregation.finalize` has dedicated semi-join
logic that q3/q5/q12 leave untested.

---

## Q5 — Local Supplier Volume (5 joins, low-card groupby)

**Intent:** Per-nation revenue from supplier-customer matched pairs within
ASIA region, for orders placed in 1994.

**Plan (after the `16434a4566` join-order fix):**
```
Read customer ─┐
               ├→ Join INNER (c_custkey = o_custkey)            ── co
Read orders ───┘  (orders filtered to 1994)                      │
                                                                 ▼
                              Read lineitem ── add_column revenue
                                                                 │
                              Join INNER (o_orderkey = l_orderkey) ── col
                                                                 │
                                                                 ▼
                  Read supplier
                              Join INNER (composite key:
                                (l_suppkey, c_nationkey) =
                                (s_suppkey, s_nationkey))         ── cols
                                                                 │
                                                                 ▼
                  Read nation
                              Join INNER (c_nationkey = n_nationkey) ── colsn
                                                                 │
                                                                 ▼
                  Read region ── Filter r_name = "ASIA"
                              Join INNER (n_regionkey = r_regionkey) ── colsnr
                                                                 │
                                                                 ▼
                              ShuffleMapOp[HashAggregateMap(keys=(n_name))]
                                                                 │
                                                                 ▼
                              ShuffleReduceOp[HashAggregateReduce]
```

**Joins:** 5× INNER.

| Stage | Left rows (sf=1) | Right rows | Output rows | Notes |
|---|---|---|---|---|
| customer ⋈ orders | 150K | ~210K (orders filtered to 1994 ~14%) | ~210K | PK/FK |
| co ⋈ lineitem | ~210K | ~6M | ~840K | Each order has ~4 lineitems |
| col ⋈ supplier | ~840K | 10K | ~33K | **Composite key** — supplier unique on `s_suppkey`, must match `c_nationkey` too. Selectivity ~4% (1/25 nations) |
| cols ⋈ nation | ~33K | 25 | ~33K | natural join, pulls in `n_name` |
| colsn ⋈ region | ~33K | 1 (ASIA only) | ~7K | filter to ASIA's 5 nations |
| Final groupby | ~7K | — | 5 | Pre-agg collapses to 5 rows per map task |

**Shuffle barriers:** 6 (5 joins + final aggregate).

**Per-stage shuffle volume (per sf=1):**

| Stage | Bytes through plasma |
|---|---|
| co ⋈ lineitem | ~35 MB co + ~750 MB lineitem ≈ **~785 MB** ← **peak** |
| col ⋈ supplier | ~140 MB col + ~1 MB supplier ≈ ~141 MB |
| cols ⋈ nation | ~6 MB cols + tiny nation ≈ ~6 MB |
| colsn ⋈ region | ~6 MB + tiny ≈ ~6 MB |
| Final groupby | tiny (5 groups) |

**Bottleneck:** `co ⋈ lineitem` shuffle. Once lineitem is in the chain,
intermediate result tracks lineitem cardinality until the composite-key supplier
join drops 96% of it.

**Workload pattern:** **longest join chain + composite-key shuffle**. Composite-
key `_make_join_partition_fn` is only exercised here. Final low-card aggregate
fully collapses via pre-agg. This is the closest thing to a "stress test the
whole shuffle pipeline" query.

**Before the join-order fix:** the previous Q5 wrote `rns ⋈ customer ON n_nationkey`
which **cartesian-multiplied supplier × customer per nation** → 1.2 B intermediate
rows at sf=10, OOMing on v1 and producing 718 M-row `JoinShuffleReduce` on v2.

---

## Q12 — Shipping Modes and Order Priority

**Intent:** For two ship modes (MAIL, SHIP), count high-priority vs low-priority
orders received in 1994 with late commits and timely shipments.

**Plan:**
```
Read orders (2 cols) ─────────────────────────────────────────────────┐
                                                                       │
Read lineitem (5 cols) ── Filter (shipmode IN (MAIL,SHIP)               │
                          AND commitdate<receiptdate                    │
                          AND shipdate<commitdate                       │
                          AND receiptdate ∈ [1994-01,1995-01))          │
                                                                       │
                          Join INNER (o_orderkey = l_orderkey) ────────┘
                                                                       │
                              MapBatches add_column high_line, low_line │
                                                                       │
                              ShuffleMapOp[HashAggregateMap(keys=(l_shipmode))]
                                                                       │
                              ShuffleReduceOp[HashAggregateReduce]
```

**Joins:** 1× INNER PK/FK.

| Join | Left rows (sf=1) | Right rows | Output rows |
|---|---|---|---|
| orders ⋈ lineitem | 1.5M (no filter) | ~600K (filter ~10%) | ~600K |

**Shuffle barriers:** 2 (join + final aggregate).

**Per-stage shuffle volume (per sf=1):**

| Stage | Bytes through plasma |
|---|---|
| Join (`o_orderkey`) | ~170 MB orders + ~75 MB lineitem (filtered hard) = ~245 MB |
| Final groupby | 2 distinct shipmodes → ~2 rows per map task |

**Bottleneck:** the single join — orders is not pre-filtered (170 MB × sf
through plasma). Lineitem is heavily filtered.

**Workload pattern:** single big-table join + low-card aggregate. Mid-point
between Q1 (no join) and Q3/Q5 (many joins). Useful for **comparing v1 vs v2
join on identical plan shape** because there's only one shuffle barrier in the
critical path.

---

## Q18 — Large Volume Customer

**Intent:** Customers whose individual orders sum >300 in lineitem quantity,
report customer/order info with sum.

**Plan (4-stage, lineitem read **twice**):**
```
─── Stage 1: hot orderkeys ───
Read lineitem #1 (2 cols) → ShuffleMapOp[HashAggregateMap(keys=(l_orderkey))]
                          → ShuffleReduceOp[HashAggregateReduce]
                          → Filter sum(l_quantity) > 300
                          → "hot_orderkeys"

─── Stage 2: filter orders ───
Read orders ──┐
              ├→ Join LEFT_SEMI (o_orderkey = l_orderkey ← hot_orderkeys)
hot_orderkeys ┘                                                      │
                                                            "orders_hot"

─── Stage 3: fact chain ──────
Read customer ──────┐
                    ├→ Join INNER (c_custkey = o_custkey)  ── co
orders_hot ─────────┘                                        │
                                                              ▼
Read lineitem #2 ─────────── Join INNER (o_orderkey = l_orderkey)
                                                              │
                                                            "col_lo"

─── Stage 4: final groupby ───
ShuffleMapOp[HashAggregateMap(keys=(c_name, c_custkey, o_orderkey,
                                    o_orderdate, o_totalprice))]
ShuffleReduceOp[HashAggregateReduce(Sum(l_quantity))]
```

**Joins:** 1× LEFT_SEMI + 2× INNER.

| Stage | Approx output rows (sf=1) |
|---|---|
| Stage 1: lineitem groupby `l_orderkey` | **~1.5M rows** (one per orderkey) |
| Stage 1 filter (sum>300) | **~57** (heavy customers, ~0.004% of orders) |
| Stage 2: orders LEFT_SEMI hot_orderkeys | **~57** |
| Stage 3a: customer ⋈ orders_hot | **~57** |
| Stage 3b: co ⋈ lineitem #2 | **~230** (each heavy order has ~4 lineitems) |
| Stage 4: final groupby | **~57** unique customer-order tuples |

**Shuffle barriers:** 4 (Stage 1 agg + Stage 2 semi + Stage 3b inner + Stage 4
final agg).

**Per-stage shuffle volume (per sf=1):**

| Stage | Bytes through plasma | Notes |
|---|---|---|
| Stage 1 HashAggregate | ~750 MB lineitem → **map-side pre-agg cannot fully collapse** (orderkey is high cardinality, ~1.5M groups × 16 B = ~24 MB), shuffle bytes ≈ 24 MB | The biggest pre-agg failure case in the suite |
| Stage 2 LEFT_SEMI | ~5 MB orders + ~tiny hot_orderkeys ≈ ~5 MB | hot_orderkeys is tiny after filter |
| Stage 3b INNER (co ⋈ lineitem #2) | tiny co + ~750 MB lineitem ≈ **~750 MB** ← **peak** | Lineitem is re-read from parquet (Stage 1 result was thrown away) |
| Stage 4 final | ~57 rows → tiny | |

**Bottleneck:** Stage 3b (lineitem re-read + inner join). The reason Stage 3b
sees the full lineitem is that the planner doesn't push the `orderkey IN
hot_orderkeys` semi-filter down into the lineitem read — semi-join only filters
the **left** side of Stage 3b (which is `co`).

Stage 1 is the secondary cost: high-cardinality groupby where pre-agg helps
~30× but not infinitely.

**Workload pattern:** **lineitem read twice + high-cardinality intermediate
groupby + LEFT_SEMI + INNER chain**. Most complex plan tree in the suite.
Stresses both pre-agg edge cases and v2 join's LEFT_SEMI path.

---

## Cross-query summary

| Query | # shuffle barriers | # joins (semi/inner) | Bottleneck stage | Shuffle peak per sf (cluster) | Pre-agg collapse? |
|---|---|---|---|---|---|
| q1 | 1 | 0 | scan + filter | ~0 (4-group pre-agg) | ✅ full |
| q3 | 3 | 2 INNER | join `o_orderkey` | ~390 MB | ❌ (high-card final agg) |
| q4 | 2 | 1 LEFT_SEMI | semi-join | ~155 MB | ✅ partial (5-group final agg) |
| q5 | 6 | 5 INNER | join `co ⋈ lineitem` | ~785 MB | ✅ full (5 groups) |
| q12 | 2 | 1 INNER | join `o_orderkey` | ~245 MB | ✅ full (2 groups) |
| q18 | 4 | 1 SEMI + 2 INNER | join `co ⋈ lineitem #2` (lineitem re-read) | ~750 MB | partial (mid-pipeline high-card agg + low-card final) |

**Coverage matrix** (which query exercises which code path):

| Code path | q1 | q3 | q4 | q5 | q12 | q18 |
|---|---|---|---|---|---|---|
| `ReadFilesParquetV2` + filter pushdown | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `MapBatches` (add_column) | ✓ |  |  | ✓ | ✓ |  |
| v1/v2 hash shuffle (INNER join) |  | ✓ |  | ✓ | ✓ | ✓ |
| v2 hash shuffle (LEFT_SEMI join) |  |  | ✓ |  |  | ✓ |
| **Composite-key shuffle** |  |  |  | ✓ |  |  |
| HashAggregate (low cardinality, pre-agg full) | ✓ |  | ✓ | ✓ | ✓ |  |
| HashAggregate (high cardinality, pre-agg partial) |  | ✓ |  |  |  | ✓ |
| Mid-pipeline aggregate (not final) |  |  |  |  |  | ✓ |
| Lineitem read twice |  |  |  |  |  | ✓ |
| LEFT_OUTER / RIGHT_OUTER / FULL_OUTER / ANTI / GPU / sort_shuffle / random_shuffle / repartition | — | — | — | — | — | — (**none**) |

For "broadcast join useless" baselines (every join side > 10 MB threshold):
**q4** (single-join + LEFT_SEMI), **q12** (single-join + INNER), **q18** (3-side
fact chain). q3 also qualifies at sf ≥ 10. q5 has region/nation/supplier as
broadcast candidates at any sf, so it does **not** qualify as a
"broadcast-irrelevant" workload.

---

## BHJ (Broadcast Hash Join) suitability

Ray Data does **not currently implement BHJ** (confirmed by grep —
`JoinOperator` always goes through hash shuffle whether v1 or v2). This
section is a forward-looking analysis: **if BHJ were added** with a
default threshold of 10 MB on the build side, which joins would benefit
and which wouldn't.

### Per-join verdict

Convention used below:
- "BHJ ✓" — build side ≤ 10 MB at this sf; broadcast wins big (probe side
  avoids shuffle entirely)
- "BHJ ✗" — build side > 10 MB; must shuffle, BHJ would actively hurt
  (broadcast latency dominates)
- "BHJ marginal" — build side near 10 MB threshold; benefit depends on
  network speed and cluster size

| Query | Join | Smaller side raw bytes (per sf) | sf=1 | sf=10 | sf=100 | sf=1000 |
|---|---|---|---|---|---|---|
| **q1** | _no joins_ | — | — | — | — | — |
| **q3** | customer(BUILDING) ⋈ orders | filtered customer ~5 MB | ✓ BHJ on customer | marginal (~50 MB) | ✗ shuffle | ✗ shuffle |
| **q3** | (co) ⋈ lineitem | co ~20 MB | marginal | ✗ shuffle | ✗ shuffle | ✗ shuffle |
| **q4** | orders ⋈ lineitem (LEFT_SEMI) | filtered orders ~5 MB | ✓ BHJ on orders¹ | marginal (~50 MB) | ✗ shuffle | ✗ shuffle |
| **q5** | customer ⋈ orders | filtered orders ~24 MB | marginal | ✗ shuffle | ✗ shuffle | ✗ shuffle |
| **q5** | (co) ⋈ lineitem | co ~35 MB | ✗ shuffle | ✗ shuffle | ✗ shuffle | ✗ shuffle |
| **q5** | (col) ⋈ supplier (composite) | supplier ~1 MB | ✓✓ **BHJ** | ✓ BHJ (~10 MB) | marginal (~100 MB) | ✗ shuffle |
| **q5** | (cols) ⋈ nation | nation < 2 KB | ✓✓✓ **always BHJ** | ✓✓✓ | ✓✓✓ | ✓✓✓ |
| **q5** | (colsn) ⋈ region | region < 1 KB | ✓✓✓ **always BHJ** | ✓✓✓ | ✓✓✓ | ✓✓✓ |
| **q12** | orders ⋈ lineitem | filtered lineitem ~75 MB | ✗ shuffle | ✗ shuffle | ✗ shuffle | ✗ shuffle |
| **q18** | orders LEFT_SEMI hot_orderkeys | hot_orderkeys ~4 KB (heavy filter) | ✓✓✓ **always BHJ** | ✓✓✓ | ✓✓✓ | ✓✓✓ |
| **q18** | customer ⋈ orders_hot | orders_hot ~4 KB | ✓✓✓ **always BHJ** | ✓✓✓ | ✓✓✓ | ✓✓✓ |
| **q18** | (co) ⋈ lineitem #2 | co ~5 KB (carries through tiny rowset) | ✓✓✓ **always BHJ** | ✓✓✓ | ✓✓✓ | ✓✓✓ |

¹ For LEFT_SEMI, the standard impl picks the smaller side regardless of
join role: build hash table on the small side, stream the big side
through, emit big-side row when hash table lookup succeeds. The output
columns belong to whichever side is preserved by the join_type — for
LEFT_SEMI the output is the **left** schema, so if we broadcast right
the implementation matches naturally.

### Which queries would benefit the most from BHJ

| Query | Joins where BHJ helps | Joins where BHJ doesn't | Net verdict |
|---|---|---|---|
| **q1** | n/a | n/a | irrelevant (no joins) |
| **q3** | 0–1 of 2 (sf-dependent) | 1–2 of 2 | small win at sf≤1, no win at sf≥10 |
| **q4** | 0–1 of 1 (sf-dependent) | 1 of 1 at sf≥10 | small win at sf≤1, no win at sf≥10 |
| **q5** | **3 of 5 always** (supplier sf≤10, nation always, region always) | 2 of 5 (the big-table fact chain) | **medium-large win at every sf** — saves the small-table shuffles, which are otherwise full hash partition over 500 partitions for ~25 rows |
| **q12** | 0 of 1 | 1 of 1 | no win at any sf |
| **q18** | **3 of 3 always** | 0 of 3 | ⭐ **huge win at every sf** — after the heavy filter chain, every join probe side is full lineitem/customer while build side is tiny; the current shuffle implementation re-shuffles 750 MB lineitem just to find ~230 matching rows |

### Why q18 is the killer BHJ candidate

After Stage 1 + 2 in q18, the "hot" branch carries < 1 KB × sf of
metadata. Stage 3 joins this against `customer` (25 MB × sf) and then
against `lineitem` (750 MB × sf). With the current shuffle impl, each
of those joins **redistributes every customer / lineitem row** across
500 partitions so they can be partition-aligned with the ~57 hot
order-keys — which then get matched by ~57 reducers, while the other
443 reducers do nothing useful.

With BHJ:
- Broadcast 4 KB of hot-orderkeys to every node (one-time cost ≈ μs)
- Each customer / lineitem block reads its share of the parquet, probes
  the in-memory hash table locally, emits matching rows
- Total network bytes for the join ≈ # nodes × 4 KB instead of `~250 MB × sf`
- Disk / plasma churn for the join ≈ 0 instead of `~750 MB × sf`

**Estimated speedup for q18 with BHJ at sf=100:** the join-chain
shuffle goes from ~75 GB through plasma down to ~kB through Ray's
object broadcast layer. That's the same order of magnitude as moving
from "spill-bound" to "scan-bound" — could cut wall-clock by 5–10×.

### Why q12 / q4 are "BHJ-irrelevant" benchmarks

Filtered orders (q4) and filtered lineitem (q12) sit at ~5 MB and
~75 MB per sf respectively. Beyond sf=1, the smaller side is already
multiple times the broadcast threshold. There is no way to short-cut
the shuffle; the planner has no choice but to hash-partition both
sides. That makes q4 and q12 a good **pair of "BHJ-doesn't-apply"
baselines** to measure pure hash-shuffle performance against q5/q18
where BHJ would change the plan shape.

### Implementation cost estimate

If we wanted to add BHJ to Ray Data:

1. **Logical layer:** add `BroadcastJoin` as a logical operator (or
   stash a `prefer_broadcast=True` hint on `Join`).
2. **Planner:** in `plan_join_op`, estimate each side's size via
   `LogicalOperator.estimated_num_outputs() * estimated_row_size`. If
   the smaller side ≤ `data_context.broadcast_join_threshold` (default
   ~10 MB), emit `BroadcastJoinOp` instead of `JoinOperator`.
3. **Physical layer:** `BroadcastJoinOp` materializes the build side
   once (small `ds.materialize()` + `ray.put` → single ObjectRef), then
   runs a TaskPoolMapOperator on the probe side where each task does
   `ray.get` of the build ref + `pa.Table.join` in-process. No shuffle
   barrier, no `ShuffleReduceOp`. About 100–200 lines of Python.
4. **Outer join semantics:** LEFT/RIGHT/FULL OUTER need null-padding
   logic identical to what `JoiningAggregation._preprocess` /
   `_postprocess` already does — can be reused verbatim.

The main risk is **size estimation**:
`LogicalOperator.estimated_num_outputs()` is rough; mis-estimating a
"small" side as 10 MB when it's actually 200 MB will OOM every worker
when they all `ray.get` it. Need a fallback "if `ray.get` shows the
broadcast object is much larger than estimated, fall back to shuffle"
path, or just be conservative with the threshold.
