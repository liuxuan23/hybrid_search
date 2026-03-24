# 阶段三实施计划

## 1. 阶段目标

阶段三的目标是在阶段二已经完成的 `adj_index + physical row_id + clustered/unclustered 对照` 基础上，进一步把“聚簇”从静态布局概念推进到真正影响查询执行路径的优化手段。

阶段三不再把重点放在“能否跑通邻接索引查询”，而是聚焦以下核心问题：

- 能否设计比 `by_node_type` 更贴近图访问模式的聚簇策略
- 能否让 `query_k_hop_index()` 真正利用 `cluster_id` 做 frontier 组织与批量读取
- 能否让 clustered adjacency 在 `2-hop / 3-hop` 查询上持续扩大相对 baseline 与 unclustered 的优势
- 能否把 clustered 的收益拆解为更可解释的 locality 指标，而不是只看总延迟

阶段三完成后，应具备如下能力：

- 支持至少一种图结构驱动的聚簇策略
- 支持 cluster-aware 的多跳 frontier 执行路径
- 支持基于 `cluster_id` 的批量读取/批量 materialize
- 能系统对比 `none / by_node_type / graph-aware clustering` 的查询收益
- 能解释 clustered 收益来自哪些局部性改善


## 2. 本阶段范围

### 包含内容

- 新的聚簇策略设计与实现
- 基于 `cluster_id` 的 adjacency 重排与写入策略
- cluster-aware 的 `k-hop` 执行路径优化
- clustered / unclustered / 多种 cluster 策略 benchmark
- 聚簇局部性指标增强
- `PHASE3_PLAN.md` 与对应实验脚本更新

### 不包含内容

- chunk 表
- 动态增量重聚簇
- 在线热点学习
- 跨机 partition / distributed execution
- 边属性索引化
- 图压缩编码优化


## 3. 阶段三要解决的核心问题

### 3.1 聚簇策略问题

阶段二当前主要支持：

- `none`
- `by_node_type`
- `hash`

这些策略具备稳定、易实现、易解释的优点，但对真实图扩展查询未必最优。阶段三需要引入更贴近图结构与访问模式的聚簇策略，例如：

- `community`
- `bfs_order`
- `high_degree_anchor`
- `hybrid`

目标是让更可能在多跳扩展中共同被访问的节点，在物理存储上尽量更接近。

### 3.2 执行路径问题

阶段二已经验证：

- physical `row_id` 可直接作为邻接跳转入口
- `take + level-wise frontier aggregation` 对多跳有显著收益

阶段三需要继续回答：

- frontier 是否应该先按 `cluster_id` 分桶
- 每个 cluster 内是否应该单独排序并批量 `take`
- materialize 时是否应该先做 cluster-aware 合并
- 不同 cluster 策略是否会改变最优的 frontier batching 方式

### 3.3 可解释性问题

阶段三 benchmark 不能只比较“快多少”，还要解释：

- 命中结果是否集中在更少 cluster 中
- physical row span 是否明显收缩
- cluster switch 是否减少
- frontier 的跨 cluster 扩散速度是否下降


## 4. 阶段三交付物

建议在 `experiments/lancedb_graph/` 下新增或增强以下文件：

- `PHASE3_PLAN.md`
- `data_prep/build_cluster_assignments.py`（新增图结构驱动策略）
- `storage_models/lancedb_graph_adjacency.py`（支持更多聚簇构建参数）
- `query_engines/traversal.py`（cluster-aware frontier 执行）
- `benchmarks/benchmark_cluster_locality.py`（扩展多策略比较）
- `benchmarks/benchmark_adjacency_vs_baseline.py`（补充 clustered 策略实验）
- `benchmarks/benchmark_cluster_strategies.py`（可新增）
- `utils/locality_metrics.py`（增强 locality 指标）
- `utils/adjacency_stats.py`（补 cluster 分布分析）
- `docs/` 下补充聚簇与执行策略说明（如果阶段三中途再补文档）


## 5. 三阶段技术路线

### 5.1 聚簇策略层

阶段三建议至少实现以下策略中的一种作为主线：

#### 方案 A：community clustering

核心思想：

- synthetic community 图中，同社区节点更可能共同访问
- 因此优先将同社区节点写到相近物理 row 区间

优点：

- 语义清晰
- 在 community graph 上收益最容易观测

风险：

- 对 uniform 图可能收益不明显
- 需要输入图具备社区标签，或额外做近似社区划分

#### 方案 B：BFS order clustering

核心思想：

- 选取一批种子节点
- 按 BFS / 多源 BFS 的访问顺序给节点编号
- 将访问上相近的节点尽量写近

优点：

- 与多跳访问目标更一致
- 不依赖外部语义标签

风险：

- 构建复杂度更高
- 需要控制高频热点节点对顺序的扰动

#### 方案 C：hybrid clustering

核心思想：

- 先用 `node_type` / `community` 做粗分桶
- 再在桶内按 BFS order 或 degree 排序

优点：

- 容易与现有 `cluster_id` 体系兼容
- 可逐步演进，不必一次重构全流程

建议：

- 阶段三优先从 `community` 或 `hybrid` 开始
- `bfs_order` 可作为后续增强策略


### 5.2 执行路径层

当前多跳查询已经采用 `take + level-wise frontier aggregation`。
阶段三建议进一步加入以下逻辑：

#### cluster-aware frontier grouping

每一跳扩展时：

1. 收集当前 frontier 对应的邻居 row_ids
2. 将这些 row_ids 对应的目标节点按 `cluster_id` 分组
3. 在每个 cluster 内局部排序后执行批量 `take`
4. 合并各 cluster 的读取结果构造下一跳 frontier

目标：

- 让一次扩展尽量在更少的 cluster 区间内完成
- 降低随机 row 访问的离散度

#### cluster-aware materialization

在 `materialize=True` 时：

- 优先按 cluster 聚合待回表 row_ids
- 观察 cluster 内批量读取是否比全局排序读取更优

#### frontier metadata 输出

建议在查询返回中补充：

- `frontier_sizes`
- `frontier_cluster_counts`
- `frontier_row_spans`

这些信息将直接服务于 benchmark 与 locality 解释。


## 6. API 扩展建议

### 6.1 `LanceDBGraphAdjacency`

建议补充或增强以下接口：

- `build_from_tsv(..., cluster_strategy=..., cluster_config=None)`
- `build_from_dataframes(..., cluster_strategy=..., cluster_config=None)`
- `describe_adjacency_layout()`
- `list_nodes_by_cluster(cluster_id)`
- `list_high_degree_nodes(limit=...)`
- `materialize_rows_by_row_id(row_ids)`

### 6.2 `query_k_hop_index()`

建议在返回结构中增加：

```python
{
    "rows": ...,
    "count": ...,
    "time_ms": ...,
    "mode": "index-only" | "materialized",
    "k": ...,
    "direction": ...,
    "frontier_sizes": ...,
    "frontier_cluster_counts": ...,
    "frontier_row_spans": ...,
}
```

这部分不是为了让接口变复杂，而是为了让三阶段 benchmark 可以解释 clustered 的效果来源。


## 7. benchmark 设计

阶段三 benchmark 应重点回答“哪种聚簇策略在什么图和什么 hop 上收益明显”。

### 7.1 聚簇策略对照实验

建议新增：

- `none`
- `by_node_type`
- `community`
- `hybrid`

统一比较：

- 单跳 materialized
- `2-hop` materialized
- `3-hop` materialized
- `2-hop` index-only
- `3-hop` index-only

### 7.2 图分布对照

建议至少在以下 synthetic 图上测：

- `uniform`
- `powerlaw`
- `community`

原因：

- `uniform` 用来验证最坏情况或低结构收益情况
- `powerlaw` 用来观察高热点节点影响
- `community` 用来验证 graph-aware clustering 的典型优势

### 7.3 指标体系

建议统一记录：

- `avg_time_ms`
- `p50_time_ms`
- `p95_time_ms`
- `p99_time_ms`
- `avg_count`
- `avg_frontier_sizes`
- `avg_frontier_cluster_counts`
- `avg_frontier_row_spans`
- `avg_physical_row_span`
- `avg_cluster_switches`
- build time
- storage size


## 8. 建议实施步骤

### 步骤一：明确主聚簇策略

先从以下二选一中选主线：

- `community`
- `hybrid(node_type + local ordering)`

优先选择改动最小、最容易观测收益的一条。

### 步骤二：扩展 cluster assignment 逻辑

在 `data_prep/build_cluster_assignments.py` 中新增主策略的分配逻辑。

要求：

- 结果稳定可复现
- 不破坏现有 `none / by_node_type / hash`
- 可配置必要参数

### 步骤三：调整 adjacency 写入顺序

根据新策略输出的 `cluster_id` 或排序键，重新组织 `adj_index` 写入顺序。

如果需要，可引入：

- `cluster_order`
- `local_order`

但仍尽量避免大改 schema。

### 步骤四：实现 cluster-aware traversal

在 `query_engines/traversal.py` 中加入：

- 按 cluster 分桶 frontier
- cluster 内批量读取
- 收集每跳 cluster / row span 指标

### 步骤五：增强 benchmark

扩展已有 benchmark，或新增 `benchmark_cluster_strategies.py`，系统比较不同 cluster 策略。

### 步骤六：整理结论

最终输出阶段三结论时，应明确回答：

- 哪种策略在哪类图上有效
- clustered 优势主要体现在哪类 hop 查询
- locality 指标与时延之间是否存在明显相关性


## 9. 验收标准

满足以下条件即可视为阶段三主线完成：

- 至少新增一种图结构驱动的聚簇策略
- clustered adjacency 可按该策略稳定构建
- `query_k_hop_index()` 支持 cluster-aware frontier 执行
- `2-hop / 3-hop` benchmark 中 clustered 相对 unclustered 有可观测收益
- benchmark 可以输出 frontier / locality 相关指标
- 能基于 `uniform / powerlaw / community` 至少两类图解释策略效果差异
- `PHASE3_PLAN.md` 已记录目标、方案、步骤与验收标准


## 10. 风险与控制

阶段三需要重点控制以下风险：

- 聚簇策略构建成本过高，抵消查询收益
- 聚簇收益只在极少数图分布下成立
- 执行层改动过大，破坏现有正确性
- 引入 cluster-aware batching 后，查询逻辑变复杂但收益不明显
- benchmark 指标过多，难以解释主结论

控制原则：

- 先选一条最容易证明价值的聚簇主线
- 先在 `2-hop / 3-hop` 上验证，再考虑更广场景
- 每次优化都保留 unclustered 对照
- 先保证结果可解释，再追求更复杂的执行优化


## 11. 建议开发顺序

建议按如下顺序推进：

1. `PHASE3_PLAN.md`
2. 扩展 `build_cluster_assignments.py`
3. 调整 `lancedb_graph_adjacency.py` 支持新聚簇策略
4. 改造 `traversal.py` 为 cluster-aware 执行
5. 扩展 `benchmark_cluster_locality.py`
6. 如有必要，新增 `benchmark_cluster_strategies.py`
7. 补充 `adjacency_stats.py` / `locality_metrics.py` 指标


## 12. 建议的阶段三最小闭环

如果希望阶段三先做一个最小可落地版本，建议闭环如下：

- 选择 `community` 作为新聚簇策略
- 仅优化 `out` 方向 `2-hop / 3-hop`
- 在 traversal 中加入 cluster-aware frontier grouping
- 在 benchmark 中比较：
  - `none`
  - `by_node_type`
  - `community`
- 使用 `community graph` 与 `uniform graph` 做对照

只要这个最小闭环能证明：

- `community clustering + cluster-aware traversal`
- 在 community graph 的多跳查询上明显优于 unclustered

那么阶段三就已经具备明确价值，可以再向更复杂策略继续推进。
