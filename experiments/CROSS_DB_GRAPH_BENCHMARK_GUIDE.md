# Cross-DB Graph Benchmark Guide

本文档用于固定当前图查询对比测试流程，方便后续在 `LanceDB`、`PostgreSQL`、`ArangoDB` 之间做同口径实验。

## 1. 实验目标

当前实验只关注：

- 同一图数据集上的图查询能力对比
- 不比较向量检索、全文检索或多模态能力
- 先聚焦查询阶段，不聚焦导入吞吐

当前要比较的对象：

- `LanceDB` 图存储方案
- `PostgreSQL` 图扩展方案
- `ArangoDB` 图查询方案

## 2. 实验基本思想

为了保证公平，对比流程固定为：

1. 使用同一份图数据
2. 从图中自动挑选同一批 seed 节点
3. 将 seed 展开成统一 workload
4. 用不同数据库 adapter 执行同一组 workload
5. 记录每次查询耗时、返回条数、是否成功
6. 汇总结果并横向比较

一句话概括：

> 先固定数据和查询任务，再比较不同数据库执行同一批图查询任务的表现。

## 3. 目录说明

当前实现主要位于：

- `experiments/cross_db_graph/`

关键文件职责如下：

- `experiments/cross_db_graph/config.py`
  - 实验配置
  - 包括 LanceDB 存储路径、结果目录、默认 batch size 等

- `experiments/cross_db_graph/seeds.json`
  - 固定查询起点
  - 包含低度、中度、高度节点，以及批量查询节点集合

- `experiments/cross_db_graph/workloads.py`
  - 将 seed 节点展开成标准查询任务
  - 当前包括：`neighbor`、`k_hop(k=2)`、`k_hop(k=3)`、`batch_neighbor`

- `experiments/cross_db_graph/adapters/`
  - 各数据库的统一适配层
  - 当前 `LanceDB` 已打通，`PostgreSQL` 与 `ArangoDB` 仍待实现

- `experiments/cross_db_graph/runner.py`
  - 负责加载 seed、构造 workload、执行 benchmark、写出结果

- `experiments/cross_db_graph/results/`
  - 保存每次 benchmark 的输出结果

## 4. 当前固定的测试流程

### 第一步：从 TSV 三元组建 LanceDB 图库

原始数据集位置：

- `/data/dataset/graph_data`

当前推荐先把原始 TSV 三元组构建成 LanceDB 图存储，再执行 benchmark。

当前可复用的建库能力来自：

- `experiments.lancedb_graph.storage_models.lancedb_graph_adjacency.LanceDBGraphAdjacency`

其核心入口是：

- `build_from_tsv(tsv_path, cluster_strategy="by_node_type")`

建库后，会在目标 LanceDB 路径下生成三张表：

- `nodes`
- `edges`
- `adj_index`

其中：

- `nodes` 保存节点属性与度数统计
- `edges` 保存原始边
- `adj_index` 保存邻接索引，供高效图查询使用

### 第二步：确认 benchmark 使用的 LanceDB 存储路径

当前 benchmark 默认通过下面配置连接 LanceDB 图库存储：

- `experiments/cross_db_graph/config.py` 中的 `LANCEDB_DB_PATH`

这意味着 benchmark 执行前需要保证：

- `LANCEDB_DB_PATH` 指向的目录中已经存在 `nodes`、`edges`、`adj_index`

当前已打通的运行路径是：

- 直接基于 `config.LANCEDB_DB_PATH` 指向的 LanceDB 图库存储运行查询 benchmark

也就是说，当前 benchmark 流程的前提是：

- LanceDB 图库已经准备好
- 数据已经写入 `nodes`、`edges`、`adj_index` 三张表

### 第三步：生成 seed 节点

脚本：

- `experiments/cross_db_graph/scripts/export_graph_data.py`

作用：

- 从 LanceDB 图库读取节点统计信息
- 依据节点度数划分低/中/高三档
- 每档抽取一批节点，写入 `seeds.json`

生成后的 `seeds.json` 用来固定查询起点，保证不同数据库执行的是同一批查询。

### 第四步：展开标准 workload

文件：

- `experiments/cross_db_graph/workloads.py`

当前逻辑：

- 对每个单点 seed，生成：
  - 1 个 `neighbor` 查询
  - 1 个 `2-hop` 查询
  - 1 个 `3-hop` 查询
- 如果存在批量 seed，则再生成：
  - 1 个 `batch_neighbor` 查询

因此，若单点 seed 数量为 $n$，批量 seed 非空，则总 workload 数为：

$$
3n + 1
$$

### 第五步：执行 benchmark

入口：

- `experiments/cross_db_graph/runner.py`

执行流程：

1. 读取 `seeds.json`
2. 构造 workload 列表
3. 创建目标数据库 adapter
4. 执行 warmup
5. 执行正式测量
6. 将结果写入结果目录

当前已实现：

- `LanceDBGraphAdapter`
- `PostgresGraphAdapter`
- `ArangoDBGraphAdapter`

尚未实现：

- 查询结果一致性校验

### 第六步：查看结果

输出目录示例：

- `experiments/cross_db_graph/results/20260327_161351/`

典型输出文件：

- `raw_results.csv`
  - 每条 workload 每次测量的原始记录
- `summary.md`
  - 本次运行的简要汇总

## 5. 当前 LanceDB 已跑通的内容

当前已验证：

- LanceDB 图存储可加载
- seed 自动生成功能可用
- benchmark runner 可执行
- 结果可正确输出到 `results/`

当前已跑通一次，结果表现为：

- 加载 `31` 个 workloads
- 在 `MEASURE_RUNS=3` 条件下共产生 `93` 条原始结果

## 6. 用户实际实验时需要做什么

当前用户通常会遇到三类操作：

### 场景 A：从 TSV 建 LanceDB 图库

适用于：

- 还没有构建 LanceDB 图表
- 更换了原始 TSV 数据
- 希望重新生成 `nodes / edges / adj_index`

执行后会在目标 LanceDB 目录中生成图表存储。

### 场景 B：重新生成 seed

适用于：

- LanceDB 图库内容已变化
- 希望重新挑选测试节点

执行后会更新：

- `experiments/cross_db_graph/seeds.json`

### 场景 C：运行一次 benchmark

适用于：

- 已有 LanceDB 图库存储
- 已有 seed
- 希望执行一次 LanceDB 流程

执行后会新增：

- `experiments/cross_db_graph/results/<run_id>/raw_results.csv`
- `experiments/cross_db_graph/results/<run_id>/summary.md`

## 7. 推荐的完整 LanceDB 操作链路

推荐按如下顺序操作：

1. 从 TSV 构建 LanceDB 图库存储
2. 检查 `config.py` 中的 `LANCEDB_DB_PATH` 是否指向该存储
3. 生成 `seeds.json`
4. 运行 benchmark
5. 查看 `results/` 输出


## 8. 后续扩展方向

后续完整对比还需要继续补齐：

1. 查询结果一致性校验
2. 统一汇总分析脚本
3. 更完善的 ArangoDB 部署与初始化脚本

## 9. ArangoDB 当前接入方式

当前已新增：

- `experiments/cross_db_graph/scripts/import_arangodb.py`
- `experiments/cross_db_graph/adapters/arangodb_adapter.py`
- `runner.py --engine arangodb`

默认配置位于 `experiments/cross_db_graph/config.py`：

- `ARANGODB_URL`
- `ARANGODB_DB`
- `ARANGODB_USERNAME`
- `ARANGODB_PASSWORD`

### ArangoDB 导入流程

在本地启动 ArangoDB 后，可执行：

- 导入 TSV 到 ArangoDB：
  - `python experiments/cross_db_graph/scripts/import_arangodb.py <tsv_path>`

该脚本会：

1. 连接 `_system` 数据库并确保目标数据库存在
2. 创建：
  - 顶点集合 `graph_nodes`
  - 边集合 `graph_edges`
  - 图 `graph_bench_graph`
3. 创建基础索引：
  - `graph_nodes(node_id)` 唯一 hash index
  - `graph_nodes(node_type)`
  - `graph_nodes(community_id)`
  - `graph_nodes(degree_out)` persistent index
  - `graph_edges(src_id)`
  - `graph_edges(dst_id)`
4. 清空旧数据并批量导入节点与边

### ArangoDB benchmark 运行方式

导入完成后，可直接运行：

- `python experiments/cross_db_graph/runner.py --engine arangodb`

该 adapter 当前支持：

- `1-hop` 邻居查询
- `k-hop` 遍历查询
- `batch 1-hop` 查询

### 当前三库实验结果概览

基于当前已跑通的结果目录：

- `LanceDB`：`experiments/cross_db_graph/results/20260327_173441/summary.md`
- `PostgreSQL`：`experiments/cross_db_graph/results/20260327_185547/summary.md`
- `ArangoDB`：`experiments/cross_db_graph/results/20260328_160550/summary.md`

当前平均耗时对比如下：

| engine | neighbor mean ms | k_hop mean ms | batch_neighbor mean ms |
| --- | ---: | ---: | ---: |
| LanceDB | 43.150 | 94.660 | 1287.732 |
| PostgreSQL | 0.977 | 4.988 | 2.611 |
| ArangoDB | 2.342 | 2.365 | 2.241 |

初步观察：

- `LanceDB` 当前图查询实现明显最慢，尤其 `batch_neighbor` 开销很高
- `PostgreSQL` 在 `neighbor` 上非常快，但 `3-hop` 查询会随着结果集扩大而明显变慢
- `ArangoDB` 当前整体最稳定，`neighbor`、`k_hop`、`batch_neighbor` 都维持在约 `2~3 ms`
- 但当前 `ArangoDB` 结果中的 `mean_result_count` 明显偏小，后续仍需要补做查询结果一致性校验，确认三库是否完全执行了同口径查询

## 10. 当前建议

建议后续按以下顺序推进：

1. 固化 LanceDB 流程
2. 接入 PostgreSQL 查询 adapter
3. 接入 ArangoDB 查询 adapter
4. 对三者跑同一份 `seeds.json`
5. 再做最终性能对比与分析