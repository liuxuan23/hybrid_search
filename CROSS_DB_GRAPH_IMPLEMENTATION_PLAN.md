# Cross-DB Graph Benchmark 实施计划

## 1. 目标

本文档用于将跨数据库图查询对比方案固化为可执行的工程结构，面向以下三种方案在同一数据集上的图查询能力对比：

- `LanceDB` 图存储与查询方案
- `PostgreSQL` 图实现方案
- `ArangoDB` 原生图查询方案

本阶段重点是尽快搭建一个**最小可行 benchmark 框架**，用于执行以下 workload：

- 1-hop 邻居查询
- 2-hop 遍历
- 3-hop 遍历
- batch 1-hop 扩展

---

## 2. 建议目录结构

建议新增目录：

```text
experiments/
  cross_db_graph/
    README.md
    __init__.py
    config.py
    workloads.py
    runner.py
    result_schema.py
    seeds.json

    adapters/
      __init__.py
      base.py
      lancedb_adapter.py
      postgres_adapter.py
      arangodb_adapter.py

    scripts/
      __init__.py
      export_graph_data.py
      import_postgres.py
      import_arangodb.py
      validate_consistency.py

    results/
      .gitkeep
```

---

## 3. 文件职责说明

### 3.1 顶层文件

#### `README.md`
说明目录用途、实验目标、运行方式、结果输出位置。

#### `config.py`
统一管理 benchmark 配置，包括：

- 数据集路径
- LanceDB 存储路径
- PostgreSQL 连接参数
- ArangoDB 连接参数
- warmup 次数
- 测量次数
- batch size
- 输出目录

#### `workloads.py`
定义统一 workload，包括：

- `NeighborQuery`
- `KHopQuery`
- `BatchNeighborQuery`

用于把三种数据库上的测试任务抽象成统一接口。

#### `runner.py`
统一 benchmark 入口，负责：

1. 加载配置
2. 加载 seeds
3. 构造 workload
4. 调用各 adapter
5. 执行 warmup 和正式测量
6. 保存结果

#### `result_schema.py`
统一结果结构，建议字段包括：

- `engine`
- `query_type`
- `seed`
- `k`
- `batch_size`
- `time_ms`
- `result_count`
- `success`
- `error_message`

#### `seeds.json`
保存固定测试节点集合，建议按 degree 分组：

- `low_degree`
- `medium_degree`
- `high_degree`
- `batch_seed_set`

---

### 3.2 `adapters/` 目录

#### `base.py`
定义统一 adapter 接口，建议包括：

- `connect()`
- `close()`
- `query_neighbors(seed, direction="out")`
- `query_k_hop(seed, k, direction="out")`
- `query_batch_neighbors(seeds, direction="out")`

#### `lancedb_adapter.py`
封装当前已有的 LanceDB 图查询实现，复用：

- `experiments/lancedb_graph/query_engines/traversal.py`
- `experiments/lancedb_graph/query_engines/basic_queries.py`

主要负责统一输入输出，不重写底层算法。

#### `postgres_adapter.py`
封装 PostgreSQL 查询逻辑，第一阶段只实现：

- 1-hop 查询
- 2-hop 查询
- 3-hop 查询
- batch 1-hop 查询

#### `arangodb_adapter.py`
封装 ArangoDB 查询逻辑，第一阶段只实现：

- 1-hop 查询
- 2-hop 查询
- 3-hop 查询
- batch 1-hop 查询

---

### 3.3 `scripts/` 目录

#### `export_graph_data.py`
将当前图数据导出成统一格式，例如：

- `nodes.jsonl`
- `edges.jsonl`

#### `import_postgres.py`
负责 PostgreSQL 侧：

- 建表
- 建索引
- 导入 nodes
- 导入 edges

#### `import_arangodb.py`
负责 ArangoDB 侧：

- 创建 collections
- 导入 vertex 数据
- 导入 edge 数据

#### `validate_consistency.py`
校验三种系统的数据和查询语义一致，包括：

- 节点数一致
- 边数一致
- sample seeds 的 1-hop 结果一致
- sample seeds 的 2-hop 节点集合一致

---

### 3.4 `results/` 目录

用于保存 benchmark 输出。

建议结构：

```text
results/
  20260326_1/
    raw_results.csv
    summary.md
```

---

## 4. 与现有代码的衔接方式

建议复用以下已有模块：

- `experiments/lancedb_graph/query_engines/traversal.py`
- `experiments/lancedb_graph/query_engines/basic_queries.py`
- `experiments/lancedb_graph/benchmarks/`

实现策略不是替换现有 `lancedb_graph` 代码，而是：

1. 新增 `cross_db_graph` 目录；
2. 将 LanceDB 查询能力包装为 `adapter`；
3. 新增 PostgreSQL 与 ArangoDB adapter；
4. 通过统一 `runner.py` 执行 benchmark。

---

## 5. 第一阶段实施顺序

建议按以下顺序推进。

### Step 1：搭建目录与文件骨架
先创建：

- `config.py`
- `workloads.py`
- `runner.py`
- `adapters/base.py`
- `adapters/lancedb_adapter.py`
- `adapters/postgres_adapter.py`
- `adapters/arangodb_adapter.py`

### Step 2：接入 LanceDB adapter
目标：

- 能够调用现有 LanceDB 查询逻辑
- 能输出统一格式结果

### Step 3：实现 PostgreSQL 最简查询
目标：

- 使用 `nodes/edges` 表和索引
- 完成 1-hop、2-hop、3-hop、batch 1-hop

### Step 4：实现 ArangoDB 最简查询
目标：

- 使用 vertex/edge collections
- 完成 1-hop、2-hop、3-hop、batch 1-hop

### Step 5：实现导入脚本
包括：

- 导出统一数据
- 导入 PostgreSQL
- 导入 ArangoDB

### Step 6：实现 benchmark runner
实现：

- warmup
- 多轮重复测量
- CSV / Markdown 输出

---

## 6. 第一版最小目标

第一版 benchmark 只要求达到以下状态：

### 数据
- 使用一份统一图数据
- 规模可先固定为 `100K nodes`

### 系统
- LanceDB
- PostgreSQL
- ArangoDB

### workload
- 1-hop
- 2-hop
- 3-hop
- batch 1-hop

### 输出
- 原始结果 CSV
- 简要汇总 Markdown

---

## 7. 实施注意事项

1. **先保证语义一致，再比较性能**；
2. PostgreSQL 第一阶段先用最简 SQL 实现，不引入 AGE；
3. ArangoDB 第一阶段先用最直接的 AQL traversal；
4. LanceDB adapter 尽量复用已有查询实现，避免重复写逻辑；
5. 固定测试种子，避免每次结果波动太大。

---

## 8. 下一步建议

在本计划基础上，下一步直接开始：

1. 创建 `experiments/cross_db_graph/` 目录结构；
2. 写出 `base.py` 和三个 adapter 的代码骨架；
3. 写出 `runner.py` 和 `workloads.py` 的最小版本；
4. 再补导入脚本和一致性校验脚本。
