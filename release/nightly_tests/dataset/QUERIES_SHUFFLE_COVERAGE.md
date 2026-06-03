# TPC-H 子集 shuffle 覆盖度 + spill 触发预测

针对 `benchmark_tpch_queries.py` 现有的 q1 / q3 / q5 / q12 四个 query,分析它们对 Ray Data shuffle 实现的覆盖面,以及在哪个 scale factor 下会触发 spill。

参考集群尺寸:**32 节点 × 8 CPU / ~35 GB RAM / ~10 GB plasma 每节点**(cluster aggregate ≈ 256 CPU / 1134 GB RAM / 317 GB object store)。

> ⚠️ **spill 触发是 per-node 不是 cluster aggregate**。Raylet 在 `node_manager.cc:2442` 用 `local_pinned_bytes / local_plasma_capacity ≥ object_spilling_threshold (0.8)` 触发。每节点 ~10 GB plasma → **单节点 pinned 突破 8 GB 那个节点就 spill**,跟集群总用量、跟 Ray Data 自己的 158 GB backpressure cap 都没关系。

---

## 1. 每个 query 在 shuffle 维度上压什么

| Query | 形态 | shuffle 算子链 | shuffle key 类型 | groupby key 基数 |
|---|---|---|---|---|
| **q1** | 单表 groupby | `HashAggregateMap → HashAggregateReduce`(v2) | 单列 hash:`(l_returnflag, l_linestatus)` | **极低**(4 组)— 全表数据汇聚到 4 个 partition,**fan-in 极端** |
| **q3** | 2× join + groupby | 两个 `JoinOperator`(v1)/ `Join v2` 链 + 末尾 v2 aggregate | 单列 hash(`c_custkey`, `o_orderkey`, `o_orderkey`) | **极高**(orderkey ~1.5M 组 × sf) |
| **q5** | 5× join + groupby | 4 个 PK/FK join + **1 个 composite-key supplier join** + v2 aggregate | 包含**复合键** `(l_suppkey, c_nationkey)` | **极低**(5 组,亚洲国家) |
| **q12** | 1× join + groupby | 1 个 `JoinOperator` + v2 aggregate | 单列 hash `o_orderkey` | **低**(7 组,shipmode) |

---

## 2. Ray Data shuffle 实现栈 vs 本 benchmark 覆盖情况

Ray Data 现存的 shuffle 路径(每个都有独立代码路径,我之前 grep 过):

| Shuffle path | 入口 | 谁用 | 本 benchmark 覆盖 |
|---|---|---|---|
| **v1 hash shuffle**(actor pool,`HashShuffleAggregator`) | `JoinOperator` 继承 `HashShufflingOperatorBase` | join 默认走这 | ✅ q3 / q5 / q12 默认 |
| **v2 hash shuffle**(stateless task,`ShuffleMapOp+ShuffleReduceOp`) | `_plan_hash_shuffle_aggregate_v2`,`_plan_hash_shuffle_join_v2`(本 branch 新加) | HashAggregate 默认 + Join 当 `enable_v2_join=True` | ✅ q1 / q3 / q5 / q12 的末尾 aggregate 都走;join 走 v2 需要 `--enable-v2-join` |
| **Sort shuffle (pull-based)** | `_plan_sort_op` 等 | `ds.sort()` / `--strategy sort_shuffle_pull_based` 触发 | ❌ **未覆盖** — 没 query 用 `sort()` |
| **Sort shuffle (push-based)** | `--strategy sort_shuffle_push_based` 触发 | 同上 | ❌ **未覆盖** |
| **Random shuffle** | `ds.random_shuffle()` | 想随机化数据时用 | ❌ **未覆盖** |
| **Repartition (with shuffle)** | `ds.repartition(N, shuffle=True)` | 重新分块 | ❌ **未覆盖** |
| **GPU shuffle** | `--strategy gpu_shuffle` | GPU workload | ❌ **未覆盖**(也无 GPU 资源) |

### Shuffle workload pattern 覆盖

| Pattern | 谁覆盖 | 备注 |
|---|---|---|
| 极低 cardinality fan-in(全表 → 几个 group) | q1, q5, q12 | hash 出来后 partition 极度倾斜 |
| 高 cardinality fan-out(每行一个独立 group) | q3 | groupby orderkey ~ 1.5M × sf 组 |
| 单列 hash key | q1, q3, q12 | 大部分 join / agg |
| **复合 hash key**(多列) | q5 supplier join | 罕见路径,只有 q5 走 |
| PK/FK natural join | q3, q12, q5 fact chain | inner join 主要场景 |
| 多表 chain(>=3 张) | q3 (3), q5 (6) | 多次 shuffle barrier |
| INNER join | 全部 | LEFT/RIGHT/FULL OUTER / SEMI / ANTI **全未覆盖** |
| Self-join | — | **未覆盖** |
| Schema with unsupported columns(struct/list/map) | — | **未覆盖** — `JoiningAggregation._preprocess` 的 fallback 路径走不到 |

---

## 3. 数据量预估 + spill 预测

### 3.1 关键 planner 优化(必须先理解才能算对)

之前的预估之所以偏粗,是因为忽略了几条 planner 实际起作用的优化路径。重点有三:

**1. Column pruning(`columns=` 在 `load_table` 里)**
所有 query 都用了 `columns=[...]` 显式列子集,Parquet reader 物理上只读这几列。例如 q1 只读 lineitem 7/16 列 → 实际 scan 字节数 ~ raw 的 40-50%。

**2. Filter 是独立 op,在 join shuffle 之前完成**
看 user 实跑的 plan printout:
```
... TaskPoolMapOperator[ReadFilesParquetV2->Project->MapBatches(add_column)]
   → JoinOperator[Join(num_partitions=500)]
```
Filter 算子(date / mktsegment / shipmode)是 `TaskPoolMapOperator[Filter(...)]`,**先于 join 的 hash shuffle**。所以 join 看到的输入已经被 filter 收过一遍,**不是 raw lineitem 那 750 MB × sf**。

**3. v2 HashAggregate 在 map task 里做 pre-aggregation**
看 `hash_shuffle_v2.py:127` 的 `_make_aggregate_pre_agg_transformer`,以及 `plan_all_to_all_op.py:194`:
```python
map_op = ShuffleMapOp(
    ...,
    input_block_transformer=pre_agg_transformer,  # ← map 侧 partial aggregate
    ...
)
```
当 groupby key 基数很低(q1 = 4 groups, q5 = 5 groups, q12 = 2-7 groups),每个 map task 输出**最多 `key_card` 行**,而不是上百万。所以**低 cardinality aggregate 阶段的 shuffle 量 ≈ 0**,不管 sf 多大。
高 cardinality(q3 ~150K orderkeys × sf)pre-agg 起不了多大作用,每组 1-4 行,几乎是全量 shuffle。

### 3.2 各 query 实际 shuffle 量重估

TPC-H 标准 selectivity(per sf=1,行数 ≈ 上节表 × sf):

| Query | filter selectivity | 关键 shuffle 阶段 | 该阶段 shuffle 字节(per sf=1) |
|---|---|---|---|
| **q1** | l_shipdate ≤ 1998-09-02 → ~98% 行通过 | 末尾 v2 hash agg → **map 侧 pre-agg 收成 4 行/task** | ~ **0**(`map_tasks × 4 × row_bytes` 量级,与 sf 几乎无关) |
| **q3** | customer="BUILDING" ~20%;orders<1995-03-15 ~50%;lineitem>1995-03-15 ~50% | 2 个 join shuffle:co join + col_lo join;末尾 agg cardinality 150K(高) | ~150 MB(filtered_orders 250 MB × 0.6 + filtered_lineitem 350 MB × 0.4 顶峰) |
| **q5** | orders=1994 ~17%;region=ASIA ~20% | 5 个 join shuffle;末尾 agg 5 groups → pre-agg 收成几行 | **顶峰 ≈ lineitem ⋈ orders 那一步**;orders 17% × 4 lineitem ≈ 0.7 × 750 MB ≈ 500 MB(fact 链未到 region filter 时) |
| **q12** | shipmode in (MAIL,SHIP) ~28%;receipt 1994 + 复合 date 条件 → 整体 ~10% | 1 个 join shuffle:filtered_lineitem(~75 MB)⋈ orders;末尾 agg 2 groups → pre-agg 收成 2 行 | ~ **75-100 MB** |

### 3.3 Spill 预测表(per-node model)

**模型**:peak hot-node pinned bytes 用下式估:

```
hot_node_bytes ≈ cluster_shuffle_bytes / num_nodes × hot_factor
```

`hot_factor` 综合两个放大效应(取 **5×** 作粗略值):

1. **Map + Reduce 双驻留**(~2×):shuffle barrier 期间 map output shards 和 reduce input shards 同时活在 plasma 里,partition 写入侧 + 拉取侧重叠
2. **Hash 分布倾斜**(~2-3×):TPC-H key 不均匀分布(部分 customer 订单多、部分 order lineitem 数量多),hash partition 后某些桶比平均大 2-3 倍
3. (另外热点 reducer 也会成为热点节点,因为 reduce 任务是 SPREAD 但 partition 分布有偏)

**触发条件**:`hot_node_bytes > 8 GB`(0.8 × ~10 GB plasma/node)。

| Query | cluster shuffle peak | hot node (sf=10) | hot node (sf=100) | hot node (sf=1000) | hot node (sf=10000) |
|---|---|---|---|---|---|
| **q1** | ~0(map-side pre-agg)¹ | ~0 | ~0 | ~0 | ~0 — **never spills** |
| **q3** | ~150 MB × sf | 234 MB | 2.3 GB | **23 GB ✱** | **234 GB(heavy)** |
| **q4** | ~100 MB × sf | 156 MB | 1.6 GB | **16 GB ✱** | **160 GB(heavy)** |
| **q5** | ~500 MB × sf | 780 MB | **7.8 GB(临界)** | **78 GB(spill)** | **780 GB(heavy)** |
| **q12** | ~75 MB × sf | 117 MB | 1.2 GB | **12 GB ✱** | **120 GB(heavy)** |
| **q18** | ~750 MB × sf | 1.2 GB | **12 GB ✱** | **120 GB(heavy)** | OOM 风险 |

✱ = 跨过 8 GB hot-node 阈值,**会 spill**
¹ q1 的 map-side pre-agg 把每 map task 输出收到 4 行,与 sf 无关,**任何 sf 都不 spill**

### 3.4 跟实测对得上的几个点

1. **q5 sf=10000 spill 很多**(你跑过了): hot node ~ 780 GB,远超 8 GB。**实测每节点 spill 6-37 GB**,跨度 6× 验证了 hot_factor ≈ 5 的合理性。
2. **q12 sf=1000 在 Q5 之后跑会 spill,自己单跑可能不 spill**:hot node ~ 12 GB,**临界**;但**单跑也可能 spill**(我之前说不会是错的)— 之所以"跟着 Q5 跑必 spill",是因为 Q5 残留把这个节点 baseline 抬到几 GB,12 + 几 GB 必然过 8 GB 阈值。
3. **q5 sf=10 不 spill**:hot node 780 MB,远低于 8 GB。

### 3.5 想稳定看到 spill 的最小 sf

| Query | 最小触发 spill 的 sf | 备注 |
|---|---|---|
| q1 | 不可能(map-side pre-agg) | 跑大 sf 只压 map task 调度 |
| q3 | sf ≈ 350 | (8 GB ÷ 23 MB/sf ≈ 348) |
| q4 | sf ≈ 500 | |
| q5 | sf ≈ 100 | **最容易 spill 的 query** |
| q12 | sf ≈ 670 | |
| q18 | sf ≈ 70 | **lineitem 双参与导致 shuffle 量大** |

如果想压 spill 但 sf 拉不到那么大(S3 上没有那么大的 parquet),改用其他办法:

- 启 cluster 时设小 plasma:`ray start --object-store-memory=1000000000`(1 GB/node),hot-node 阈值变 800 MB,sf=10 q5 也能 spill
- 减少 `num_partitions`,partition 变胖,每个 reducer 拉的数据多,更容易触发
- 跑 `benchmark_ooc_shuffle.py` —— 设计上就是要 spill

### 3.4 怎么稳定打到 spill

想测 spill 行为,有几条路:

1. **拉大 `--scale-factor` 到 10000 跑 q3/q5/q12** — sf=10000 大概要 ~1 TB / ~5 TB / ~1 TB 中间数据。S3 上需要有这个 sf 的 parquet,否则跑不起来。
2. **改 query 故意制造大 shuffle**:
   - q5 末尾 groupby 改成 `(c_custkey,)` 而不是 `(n_name,)` → groupby cardinality 拉到 150K × sf,pre-agg 失效,末尾要全量 shuffle
   - 加一个新 query 跑 `lineitem.repartition(N, shuffle=True)` 或者 `lineitem.sort(...)`,这两条都不会 pre-agg
3. **限制 object_store_memory**(用 `--object-store-memory` 启动 cluster 时设小一点,比如 50 GB),让 spill 阈值降到 ~40 GB,这样 sf=100 的 q5 就能压到 spill
4. **跑 `benchmark_ooc_shuffle.py`** — 那个 script 就是专门压 OOC shuffle 设计的,跑 500 GB 100% spill 没问题

---

## 4. 覆盖空洞 + 建议补充

如果目标是用这套 benchmark 完整测 Ray Data shuffle,**至少加这几个**:

1. **Sort shuffle**:补一个用 `ds.sort()` 的 query。例如 "lineitem ordered by (l_shipdate, l_orderkey)",参数化 `--strategy sort_shuffle_pull_based` / `push_based`,直接对比 sort vs hash 两条 shuffle 路径。
2. **Outer join**:TPC-H 标准没有 outer join,但可以人造一个 q-outer:`customer LEFT OUTER JOIN orders ON c_custkey=o_custkey`(不少 customer 没 order),验证 v2 join 的 null-padding。`test_join.py` 已经覆盖功能正确性,但**性能**层面没有 benchmark。
3. **Composite key join**(更深一些):q5 已经覆盖了 `(l_suppkey, c_nationkey)`,但还可以加一个 **3 列 composite** 看看 hash_partition 在多列上是不是仍均匀分发。
4. **Skewed-key shuffle**:故意制造一列分布极不均的 key(比如 80% 行 key=0),看 v1 actor pool vs v2 task 在 hot partition 上的行为(v1 OOM 概率高,v2 spill 路径概率高)。
5. **Random shuffle / repartition**:即使是给 sanity 用,跑 `ds.random_shuffle(num_blocks=N)` 验证 shuffle plumbing。

---

## 5. 当前 benchmark 跑 spill 的最稳套餐

根据上节 per-node model:

**A. 最容易压 spill 的 TPC-H 跑法**(q5 sf=100):

```bash
python3 release/nightly_tests/dataset/benchmark_tpch_queries.py \
  --scale-factor 100 --queries q5 --num-partitions 500 \
  --strategy hash_shuffle --enable-v2-join
```
q5 sf=100 hot-node 估算 ~7.8 GB,刚好越线;sf ≥ 100 q5/q18 都会 spill。

**B. 同时压多个 query 看 cumulative 效应**(已经验证 q5→q12 会触发的现象):

```bash
python3 release/nightly_tests/dataset/benchmark_tpch_queries.py \
  --scale-factor 1000 --queries q5,q18,q12 --num-partitions 500 \
  --strategy hash_shuffle --enable-v2-join
```

**C. 故意造一个超大 OOC shuffle**(`benchmark_ooc_shuffle.py`):

```bash
python3 release/nightly_tests/dataset/benchmark_ooc_shuffle.py \
  --data-size-gb 500 --num-partitions 500 --strategy hash_shuffle
```

**D. 限制 plasma 把阈值压低**(快速验证 spill 路径,不用大 sf):

```bash
ray stop ; ray start --head --object-store-memory=1000000000  # 1 GB/node
# 然后 sf=10 的 q5/q18 也能 spill,因为 hot-node 阈值变 800 MB
```

**E. 对比 v1 vs v2 join 在 spill 路径下的差异**:同 query 跑两次,一次 `--enable-v2-join` 一次不带。v1 在 hot-node 过限时 OOM(actor 不会 spill),v2 进 plasma → 磁盘 spill 路径,**这两个数据点的对比就是 v2 join 改动最直接的证据**。
