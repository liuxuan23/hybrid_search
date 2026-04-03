# Lance Graph 与 Adjacency 大图基准测试方案

## 1. 背景

当前 `lance-graph` 自带的 benchmark 主要衡量的是：基于内存中的 `RecordBatch` 输入，以及较简单的合成图结构，执行 Cypher 查询的性能。这对于验证 parser / planner / DataFusion 执行链路的速度是有价值的，但它并不能充分代表超大规模图上的真实查询路径。

相比之下，`experiments/lancedb_graph/` 下基于 adjacency 的实现，核心设计围绕“局部邻接访问”展开：

- seed 节点查找
- `node_id -> physical_row_id` 映射解析
- 基于行的 `take()` / `_rowid` 读取
- 面向多跳查询的类 BFS frontier 扩展

如果要公平比较这两套系统，就需要一个更贴近真实大图访问模式的 benchmark。

---

## 2. 基准目标

这个 benchmark 应该比较：

- `lance-graph`：Cypher -> logical plan -> DataFusion scan/join 执行
- adjacency：基于 seed 的局部邻接读取与 frontier 扩展

核心问题不只是“查询在逻辑上能不能表达”，而是：在真实的大图访问模式下，这两种执行路径的行为差异是什么。

---

## 3. 为什么现有 Benchmark 不够

当前 `lance-graph` benchmark 的特点：

- 使用合成的环状或规则图结构
- 将查询输入预加载为内存中的 `RecordBatch`
- 重点是内存表上的端到端查询执行
- 不能体现 adjacency-local 的访问模式

这意味着，它更接近“列式关系执行 benchmark”，而不是“真实的大图遍历 benchmark”。

---

## 4. 基准原则

一个更真实的 benchmark 至少应满足：

1. 从明确的 seed 节点出发
2. 强调局部扩展，而不只是全图语义
3. 同时衡量 warm 与 cold-ish 两种执行行为
4. 区分 ID-only 与 materialized 两种结果模式
5. 两个引擎使用同一份图数据，并遵循相同查询语义

---

## 5. Workload 设计

### 5.1 单 Seed 邻居查询

目标：
- 从一个 seed 节点查询 1-hop 邻居

原因：
- 这是最常见的局部图查询形态
- 可以直接体现 adjacency-local lookup 的优势

变体：
- 低度数 seed
- 中度数 seed
- 高度数 seed
- out / in / both 三种方向
- ID-only / materialized 两种返回模式

---

### 5.2 批量邻居查询

目标：
- 在一次请求中查询一批 seed 的邻居

原因：
- 真实系统中经常要同时扩展多个 seed 或一批 frontier 节点

变体：
- batch size 16 / 64 / 256 / 1024
- 混合度数 seed
- out / in / both
- ID-only / materialized

---

### 5.3 Seeded K-Hop 查询

目标：
- 从一个 seed 出发，执行固定 `k` 的多跳扩展

原因：
- 这是最有代表性的遍历型比较点
- 预计更容易放大 scan/join 与局部 adjacency 访问之间的差异

变体：
- `k = 2`
- `k = 3`
- 可选 `k = 4`
- 低 / 中 / 高度数 seed
- ID-only / materialized

---

### 5.4 Frontier Expansion Step Benchmark

目标：
- 不只测完整的 k-hop 端到端查询，也测单步 frontier 扩展

原因：
- 更贴近真实遍历系统的执行方式
- 更容易解释 frontier 大小时的扩展行为

变体：
- frontier size 64 / 256 / 1024
- out / both 方向
- ID-only / materialized

---

## 6. 数据集设计

建议至少覆盖两类图。

### 6.1 均匀图

用途：
- 观察纯粹的规模效应
- 降低度数倾斜带来的影响

建议规模：
- 1M nodes
- 10M edges

### 6.2 幂律 / 偏斜图

用途：
- 更接近真实知识图谱 / 社交图行为
- 压测高度数热点与 frontier 膨胀问题

建议规模：
- 1M nodes
- 10M 到 50M edges

---

## 7. 测量指标

### 7.1 核心指标

- `latency_ms`
- `result_count`
- `query_type`
- `seed_type`
- `batch_size`
- `k`

### 7.2 IO 指标

- 进程级 `read_bytes`
- 进程级 `write_bytes`

### 7.3 内存指标

建议后续扩展：
- `rss_mb_before`
- `rss_mb_after`
- `rss_mb_peak`

### 7.4 Lance Graph 的计划诊断信息

建议后续扩展：
- explain 输出
- logical plan 摘要
- physical plan 摘要
- 可变长查询中的 join 数量 / union 数量

---

## 8. Warm 与 Cold-ish 模式

### Warm 模式

定义：
- 在同一进程中重复执行，不重置内存对象

目的：
- 测量 steady-state 执行成本

当前实现：
- 对应 `runner.py` 中的 `execute_benchmark()`
- 复用同一个 adapter / connection
- 先执行 `WARMUP_RUNS` 轮预热
- 再执行 `MEASURE_RUNS` 轮正式测量
- 适合观察连接、catalog、planner、数据库 buffer cache 已经热起来之后的表现

### Cold-ish 模式

定义：
- 在新的进程或新的 engine 实例中重新执行 workload
- 尽可能避免意外的缓存复用

目的：
- 近似 first-query / low-cache 场景

说明：
- 完整清理 OS page cache 是可选项，且可能需要更高权限
- 在第一阶段中，进程重启或连接隔离可以视为可接受的近似方案

当前实现拆成两种：

#### coldish

- 对应 `runner.py` 中的 `execute_coldish_benchmark()`
- 保持原始 workload 顺序
- 每个 workload 单独创建 adapter、建立连接、执行后关闭
- 可以较好隔离连接级缓存、session 状态、部分执行上下文复用

#### group-coldish

- 对应 `runner.py` 中的 `execute_group_coldish_benchmark()`
- 先按 workload 类别分组：
   - `neighbor`
   - `k_hop(k=2)`
   - `k_hop(k=3)`
   - `batch_neighbor(batch_size=...)`
- 每个类别使用一个新连接，组内复用，组间断开重连
- 适合观察“同类查询在相对冷但不完全冷状态下”的表现

#### 与真实 cold cache 的差异

- 以上两种模式都**不会自动清理 OS page cache**
- PostgreSQL / ArangoDB 也不会因为单纯新建连接就完全失去 buffer cache
- 因此它们是“cold-ish”，不是严格意义上的 cold-cache benchmark
- 如果要更接近严格冷启动，需要配合服务重启，必要时再清 OS page cache

#### 当前配套脚本

- `scripts/clear_service_caches.sh`
   - 可重启 PostgreSQL / ArangoDB 服务
   - 并提示可选的 Linux page cache 清理命令

---

## 9. 公平性原则

为了尽可能负责任地比较结果：

1. 两个引擎必须使用同一份图数据
2. 两个引擎必须实现相同查询语义
3. 结果统计必须遵循相同的去重 / 方向规则
4. benchmark 模式必须明确区分：
   - ID-only
   - materialized
5. 后续阶段中的 `lance-graph` 应优先通过大表/provider 路径测试，而不只是依赖内存 `RecordBatch`

---

## 10. 第一阶段范围

第一阶段聚焦于：在不修改上游 `lance-graph` 的前提下，尽快完成集成。

### 第一阶段目标

将 `lance_graph` 作为一个新的 engine 接入 `experiments/cross_db_graph/`，并使用等价的 Cypher 查询运行现有 benchmark workload。

### 第一阶段包含内容

- 添加 `LanceGraphAdapter`
- 支持单点邻居查询
- 支持批量邻居查询
- 支持固定 k-hop 查询
- 复用现有 seeds 与 benchmark runner
- 仅使用现有 Python bindings

### 第一阶段不包含内容

- 修改上游 `lance-graph`
- 新增原生的大表执行 API
- 完整的 cold-cache harness
- 每条查询的 plan capture
- 内存 / RSS 指标采集

补充：
- 后续已在 `cross_db_graph/runner.py` 中落地 `warm` / `coldish` / `group-coldish`
- 并新增单点查询脚本，便于用户手动在两次查询间清理缓存

---

## 11. 第二阶段方向

当第一阶段可用之后，第二阶段应进一步提升公平性与真实性。

建议的后续方向：

- 从 Python 暴露更真实的 `lance-graph` 执行路径
- 尽量优先使用 provider / context-backed 执行，而不是预先物化到内存中的表
- 增加 cold-ish 的进程隔离执行模式
- 增加 plan capture 以及更丰富的 IO / 内存指标
- 引入更真实的偏斜图数据集

---

## 12. 第一阶段实施方案

### 12.1 新增 Adapter

新增：
- `experiments/cross_db_graph/adapters/lance_graph_adapter.py`

职责：
- 从 benchmark 使用的存储中加载图数据
- 构造 `GraphConfig`
- 将 benchmark workload 翻译为 Cypher 查询
- 通过 Python `lance_graph` bindings 执行查询
- 将返回结果归一化到统一的 benchmark schema

### 12.2 Runner 集成

更新：
- `experiments/cross_db_graph/runner.py`

改动：
- 增加 `lance_graph` engine 选项
- 增加对应的 adapter 构造路径

### 12.3 初始查询映射

单点邻居查询：
- `MATCH (a:Entity {entity_id: $seed})-[:RELATIONSHIP]->(b:Entity) RETURN b.entity_id`

批量邻居查询：
- `MATCH (a:Entity)-[:RELATIONSHIP]->(b:Entity) WHERE a.entity_id IN [...] RETURN a.entity_id, b.entity_id`

K-hop 查询：
- `MATCH (a:Entity {entity_id: $seed})-[:RELATIONSHIP*2..2]->(b:Entity) RETURN b.entity_id`

---

## 13. 第一阶段预期结果

第一阶段还不能算是一个完美的大图公平性 benchmark，但它可以：

- 将 `lance-graph` 接入同一个 cross-db benchmark 框架
- 建立查询映射上的语义对齐
- 产出与 adjacency 的初步时间对比结果
- 识别出为了更强的大图评估，上游 `lance-graph` 还缺哪些能力

---

## 14. Single-Seed 单点测试实现

为支持更可控的人工冷缓存实验，当前已新增：

- `experiments/cross_db_graph/scripts/run_single_seed_queries.py`

作用：

- 对单个 seed 依次执行：
   - `neighbor`
   - `k_hop(k=2)`
   - `k_hop(k=3)`
- 适合用户在每次查询前手动重启服务、清缓存，再执行下一条命令

当前支持引擎：

- `lancedb`
- `lance_graph`
- `postgres`
- `arangodb`

当前支持参数：

- `--engine`
- `--seed`
- `--direction`
- `--materialize`
- `--json`
- `--db-path`

其中：

- `--db-path` 主要用于 `lancedb` / `lance_graph`
- 尤其适合 `lance_graph` 独立目录式存储，因为不同 benchmark 可能对应不同输出目录

示例：

- 官方 `lance_graph` 独立目录：
   - `uv run python -m experiments.cross_db_graph.scripts.run_single_seed_queries --engine lance_graph --db-path /path/to/lance_graph_dir --seed "type1:node_282449"`
- PostgreSQL：
   - `uv run python -m experiments.cross_db_graph.scripts.run_single_seed_queries --engine postgres --seed "type1:node_282449"`

该脚本的设计目的不是批量 benchmark 吞吐，而是：

- 支持人工插入“重启服务 / 清缓存 / 等待系统稳定”步骤
- 做单 seed 的可解释性排查
- 对比 1-hop / 2-hop / 3-hop 在不同缓存状态下的差异

---

## 15. 总结

这个 benchmark 方案试图把比较问题从：

- “两个系统能不能回答这个查询？”

转向：

- “在真实的大图遍历模式下，这两条执行路径分别表现如何？”

第一阶段先在 `experiments/cross_db_graph/` 内完成实用集成；后续阶段再逐步减少 `lance-graph` 的内存偏置，并补充更冷的执行模式。

---

## 15. 直接下一步

直接在以下目录中实施第一阶段：
- `experiments/cross_db_graph/`

暂时先不修改上游 `lance-graph`。
