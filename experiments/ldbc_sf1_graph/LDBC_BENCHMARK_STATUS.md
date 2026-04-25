# LDBC SF1 Benchmark Status

本文档记录 `experiments/ldbc_sf1_graph/` 下 LDBC SF1 图实验与 benchmark 接入的当前进度。

后续凡是以下内容发生变化，都应同步更新本文件：

- 数据预处理链路
- 图存储构建链路
- 查询验证结果
- seed 生成策略
- benchmark 运行方式
- benchmark 结果落盘/汇总方式
- 下一阶段计划

---

## 1. 当前目标

当前目标不是完整复现官方 LDBC SNB 基准，而是先把 `LDBC SNB SF1` 稳定接入现有 `hybrid_search` 图实验体系，形成一条：

- 可运行
- 可复现
- 可验证
- 可扩展
- 可对比

的真实图数据实验链路。

当前优先关注：

1. 原始 LDBC CSV → 标准化图数据
2. 标准化图数据 → LanceDB 图存储
3. 邻接/多跳查询正确性验证
4. seed 生成
5. LanceDB 单库 benchmark
6. 后续 cross-db 对比接入

---

## 2. 当前目录状态

当前目录核心内容如下：

```text
experiments/ldbc_sf1_graph/
├── LDBC_BENCHMARK_STATUS.md
├── config.py
├── seeds.json
├── benchmarks/
│   ├── benchmark_lancedb_ldbc.py
│   ├── run_lancedb_ldbc_benchmarks.py
│   └── run_single_ldbc_query.py
├── data_prep/
│   ├── build_lancedb_from_ldbc.py
│   ├── build_ldbc_nodes_edges.py
│   ├── export_ldbc_seeds.py
│   ├── extract_ldbc_archive.py
│   └── inspect_ldbc_schema.py
├── queries/
│   └── validate_ldbc_graph_queries.py
└── results/
```

说明：

- `data_prep/` 主链路已基本落地
- `queries/` 已具备正确性验证脚本
- `benchmarks/` 已具备单条查询、单次 benchmark、批量 benchmark runner
- `results/` 已开始承载 benchmark JSON 输出

---

## 3. 当前已完成内容

### 3.1 LDBC SF1 数据位置已确认

已确认本地存在：

- `data/social_network-sf1-CsvComposite-StringDateFormatter/`
- `data/social_network-sf1-CsvComposite-StringDateFormatter.tar.zst`

当前开发和实验直接使用已解压目录。

---

### 3.2 Schema 扫描已完成

已实现并运行：

- `data_prep/inspect_ldbc_schema.py`

已生成：

- `storage/ldbc_sf1/normalized/schema_summary.json`

当前扫描结论：

- CSV 文件总数：`33`
- 分区：
  - `dynamic`: `23`
  - `static`: `8`
  - `root`: `2`
- 类别：
  - `node`: `8`
  - `edge`: `23`
  - `node_or_dimension`: `2`

识别出的节点类型：

- `Comment`
- `Forum`
- `Organisation`
- `Person`
- `Place`
- `Post`
- `Tag`
- `TagClass`

已确认 LDBC SF1 是一个异构社交图，适合作为后续真实图 workload 的基础数据源。

---

### 3.3 标准化 nodes / edges 已完成

已实现并运行：

- `data_prep/build_ldbc_nodes_edges.py`

已生成：

- `storage/ldbc_sf1/normalized/nodes.parquet`
- `storage/ldbc_sf1/normalized/edges.parquet`

当前标准化结果：

- 节点总数：`3,181,724`
- 边总数：`17,256,038`

当前实现已解决：

- 原始 CSV 重复列名问题
- 节点/边 schema 统一问题
- 大规模数据内存占用问题

当前版本使用流式写 Parquet，避免一次性 materialize 全量 DataFrame 导致 OOM。

已验证的节点分布示例：

- `Comment`: `2,052,169`
- `Post`: `1,003,605`
- `Forum`: `90,492`
- `Tag`: `16,080`
- `Person`: `9,892`
- `Organisation`: `7,955`
- `Place`: `1,460`
- `TagClass`: `71`

---

### 3.4 LanceDB 图存储构建已完成

已实现并运行：

- `data_prep/build_lancedb_from_ldbc.py`

已生成：

- `storage/lancedb_graph/ldbc_sf1/nodes.lance`
- `storage/lancedb_graph/ldbc_sf1/edges.lance`
- `storage/lancedb_graph/ldbc_sf1/adj_index.lance`

当前实现特点：

- 复用 `experiments/lancedb_graph/` 现有图存储与邻接索引实现
- 从标准化 `nodes.parquet` / `edges.parquet` 构建图表
- 自动补充 `degree_out` / `degree_in`
- 当前构建策略为 `cluster_strategy="none"`
- 虽未进行 clustering，但已构建 `adj_index`

这意味着：

- LDBC SF1 已经成功接入现有 LanceDB 图实验链路
- 后续可以直接基于 `adj_index` 进行单跳、多跳、batch 查询测试

---

### 3.5 邻接索引正确性验证已完成

已完成的验证包括：

- `adj_index` 行数检查
- `node_id` 唯一性检查
- `physical_row_id` 完整性检查
- `degree_out` / `degree_in` 与 `edges.parquet` 对齐检查
- 指定节点检查（例如 `Person:933`）
- 随机样本检查

验证结论：

- `adj_index` 与标准化 `edges.parquet` 一致
- 邻接表中的出入度信息正确
- `physical_row_id` 连续且完整

已手动核对过 `Person:933` 的邻居访问结果，确认与 `edges.parquet` 中真实边一致。

---

### 3.6 查询正确性验证已完成

已实现并运行：

- `queries/validate_ldbc_graph_queries.py`

当前已验证的查询类型：

- 单跳 `out`
- 单跳 `in`
- 单跳 `both`
- batch neighbor 查询
- `k-hop` 查询（已验证 `k=2`）

验证方式：

- 将 LanceDB 邻接查询结果与 `edges.parquet` 的期望结果逐项对比

结论：

- single-hop 查询结果正确
- batch neighbor 查询结果正确
- k-hop 查询结果正确

这意味着当前 LanceDB 图查询链路不仅能运行，而且结果可校验。

---

### 3.7 Benchmark seed 导出已完成

已实现并运行：

- `data_prep/export_ldbc_seeds.py`

已生成：

- `seeds.json`

当前策略：

- 基于 `Person` 节点
- 按 `total_degree = in_degree + out_degree` 分层抽样

当前 bucket 划分结果：

- `low_cutoff = 479.5`
- `mid_cutoff = 1657.0`
- `low_count = 4946`
- `mid_count = 3957`
- `high_count = 989`

当前 `seeds.json` 已包含：

- `single_seeds.low_degree`
- `single_seeds.mid_degree`
- `single_seeds.high_degree`
- `single_seeds.random`
- `single_seeds.combined`
- `batch_seeds.low_degree`
- `batch_seeds.mid_degree`
- `batch_seeds.high_degree`
- `batch_seeds.mixed`

这为后续分层 benchmark 提供了稳定输入。

---

### 3.8 LanceDB benchmark runner 已完成

已实现：

- `benchmarks/benchmark_lancedb_ldbc.py`

当前支持的 benchmark 项：

- `single_out`
- `single_both`
- `batch_both`
- `k_hop_out`
- `k_hop_both`

当前已支持：

- 按 seed group 选择单点查询集合
- 按 batch group 选择批量查询集合
- 指定 `k-hop`
- 指定 `repeat`
- 结果落盘

结果输出目录：

- `experiments/ldbc_sf1_graph/results/`

文件命名格式：

- `ldbc_lancedb_benchmark_single-<group>_batch-<group>_k<k>_r<repeat>_<timestamp>.json`

---

### 3.9 批量 benchmark 编排脚本已完成

已实现：

- `benchmarks/run_lancedb_ldbc_benchmarks.py`

当前支持：

- 遍历 `single_groups`
- 遍历 `batch_groups`
- 遍历 `batch_group_indices`
- 遍历 `k_values`
- 顺序执行 benchmark
- 汇总成功/失败运行记录

已完成 smoke test，能成功触发单个组合 benchmark 并得到结果文件。

---

### 3.10 单条查询脚本已完成

已实现：

- `benchmarks/run_single_ldbc_query.py`

用途：

- 每次只执行一条查询
- 便于后续做冷启动测试
- 便于做单 query latency 测试
- 便于手工核对单个 seed 的查询结果

当前支持：

- `neighbor`
- `batch_neighbor`
- `k_hop_2`
- `k_hop_3`
- `out / in / both`
- `materialize true|false`
- `--json`

已完成 smoke test：

- `Person:933`
- `neighbor`
- `direction=out`

运行正常。

---

## 4. 当前阶段判断

当前整体进度建议判断为：

- `Phase 0（数据探查）`: **已完成**
- `Phase 1（标准化 nodes/edges）`: **已完成**
- `Phase 2（构建 LanceDB 图存储）`: **已完成**
- `Phase 3（查询正确性与 seed 生成）`: **已完成**
- `Phase 4（LanceDB baseline benchmark）`: **已完成第一版**
- `Phase 5（批量 benchmark 编排）`: **已完成第一版**
- `Phase 6（cross-db 对比）`: **未开始**
- `Phase 7（更贴近 LDBC 语义的 workload）`: **未开始**
- `Phase 8（clustering / layout 优化）`: **未开始**

当前可以认为：

> LDBC SF1 的 LanceDB 本地实验主链路已经打通，并进入可重复 benchmark 阶段。

---

## 5. 当前已识别的关键问题与限制

### 5.1 目前仅完成 LanceDB 本地链路

虽然 LDBC 数据已经接入 LanceDB 图存储，但当前还没有把同一套 LDBC workload 接到：

- `lance_graph`
- `Postgres`
- `Postgres AGE`
- `ArangoDB`
- 其它 cross-db 对比链路

因此目前仍是：

- 本地 LanceDB baseline 已完成
- cross-db benchmark 尚未开始

### 5.2 当前尚未做结果汇总分析脚本

虽然 benchmark 结果已可落盘，但当前仍缺少：

- 扫描 `results/` 的汇总脚本
- 按 seed group / k / query type 聚合输出
- 自动生成简要性能结论

目前结果仍主要依赖逐个 JSON 文件查看。

### 5.3 当前未启用 clustering / locality 优化

当前 LDBC LanceDB 图构建使用：

- `cluster_strategy="none"`

这意味着当前 benchmark 主要反映：

- 基础图存储能力
- 邻接索引查询能力
- 未优化物理布局下的基线表现

后续如需研究 locality、冷缓存、聚类布局收益，还需要继续扩展。

### 5.4 当前 workload 仍偏“图能力验证”

当前查询主要是：

- neighbor
- batch neighbor
- k-hop

它们适合当前阶段做：

- 正确性验证
- IO/延迟测量
- baseline 图遍历 benchmark

但还不是更贴近官方 LDBC SNB 语义的复杂 workload。

---

## 6. 最近关键产物

### 数据产物

- `storage/ldbc_sf1/normalized/schema_summary.json`
- `storage/ldbc_sf1/normalized/nodes.parquet`
- `storage/ldbc_sf1/normalized/edges.parquet`
- `storage/lancedb_graph/ldbc_sf1/nodes.lance`
- `storage/lancedb_graph/ldbc_sf1/edges.lance`
- `storage/lancedb_graph/ldbc_sf1/adj_index.lance`

### 配置与种子

- `experiments/ldbc_sf1_graph/config.py`
- `experiments/ldbc_sf1_graph/seeds.json`

### 核心脚本

- `data_prep/inspect_ldbc_schema.py`
- `data_prep/build_ldbc_nodes_edges.py`
- `data_prep/build_lancedb_from_ldbc.py`
- `data_prep/export_ldbc_seeds.py`
- `queries/validate_ldbc_graph_queries.py`
- `benchmarks/benchmark_lancedb_ldbc.py`
- `benchmarks/run_lancedb_ldbc_benchmarks.py`
- `benchmarks/run_single_ldbc_query.py`

### 结果目录

- `experiments/ldbc_sf1_graph/results/`

---

## 7. 下一步优先事项

建议当前按以下顺序推进。

### 优先级 1：补 benchmark 结果汇总脚本

目标：

- 扫描 `results/` 下所有 benchmark JSON
- 按 query type、seed group、k 值聚合
- 输出 avg / p50 / p95 / throughput 等汇总结果

建议位置：

- `benchmarks/summarize_lancedb_ldbc_results.py`

### 优先级 2：把单条查询 runner 用于冷启动测试

目标：

- 基于 `benchmarks/run_single_ldbc_query.py`
- 构建更稳定的单条查询冷启动/热启动测试编排
- 便于控制每次只发一条 query

### 优先级 3：开始 cross-db LDBC 接入

目标：

- 将 LDBC SF1 的标准化图数据和 seed/workload 接入 `cross_db_graph`
- 至少先完成 LanceDB / lance_graph / Postgres 中的一条对齐链路

### 优先级 4：设计更贴近 LDBC 语义的 workload

目标：

- 在 neighbor / k-hop 之外
- 逐步增加更接近 LDBC SNB 查询语义的真实 workload

### 优先级 5：尝试 clustering / layout 优化

目标：

- 为 LDBC 图引入更合适的物理布局策略
- 研究 locality、冷缓存、读放大和多跳性能变化

---

## 8. 文档维护规则

后续凡是以下内容变化，都应更新本文件：

- 新增 LDBC 相关脚本
- 完成某个阶段任务
- benchmark 运行方式变化
- 种子抽样策略变化
- 图 schema 映射方式变化
- benchmark 结果落盘格式变化
- 增加结果汇总或分析脚本
- 开始 cross-db benchmark
- 开始 clustering / layout 优化

建议每次更新至少同步以下部分：

1. `当前已完成内容`
2. `当前阶段判断`
3. `当前已识别的关键问题与限制`
4. `最近关键产物`
5. `下一步优先事项`

---

## 9. 当前结论

截至当前，LDBC SF1 相关工作的状态可以概括为：

- **LDBC 数据已就位**
- **schema 已扫描并固化**
- **标准化图数据已生成**
- **LanceDB 图存储已构建**
- **邻接与多跳查询正确性已验证**
- **seed 导出已完成**
- **单次与批量 benchmark runner 已具备**
- **单条查询脚本已具备**

当前最准确的阶段结论是：

> LDBC SF1 的 LanceDB 本地实验与 benchmark 主链路已经完成第一版闭环。

下一步重点应转向：

1. benchmark 结果汇总
2. 冷启动单条查询测试
3. cross-db 接入
4. 更真实的 LDBC 风格 workload
