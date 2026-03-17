# 阶段一实施计划

## 1. 阶段目标

阶段一的目标是完成 `LanceDB nodes + edges` 图存储实验的最小可运行闭环。

本阶段不追求性能最优，而是优先完成以下事情：

- 固定实验目录结构
- 建立基础配置
- 建立可控图数据生成链路
- 从现有图数据生成 `nodes` 和 `edges`
- 将两张表写入 `LanceDB`
- 提供统一的基础查询接口
- 提供一个最小可运行的 smoke benchmark

阶段一完成后，应具备如下能力：

- 能从输入图数据构建节点表和边表
- 能把两张表写入 LanceDB
- 能执行基础单跳查询
- 能执行基础 `k-hop` 查询
- 能输出基础统计信息
- 能通过 smoke benchmark 验证链路可用


## 2. 本阶段范围

### 包含内容

- 新实验目录骨架
- `README.md`
- `config.py`
- 合成图数据生成脚本
- 数据转换脚本
- LanceDB 基础图存储实现
- 基础查询逻辑
- 基础统计逻辑
- smoke benchmark

### 不包含内容

- 邻接索引
- 正向/反向双边表
- 高度节点专项优化
- 缓存策略
- 高级 frontier 批量执行优化
- 外部系统对比


## 3. 阶段一交付物

建议在 `experiments/lancedb_graph/` 下完成以下首批文件：

- `README.md`
- `config.py`
- `data_prep/__init__.py`
- `data_prep/generate_synthetic_graph.py`
- `data_prep/build_graph_tables.py`
- `storage_models/__init__.py`
- `storage_models/lancedb_graph_basic.py`
- `query_engines/__init__.py`
- `query_engines/basic_queries.py`
- `benchmarks/__init__.py`
- `benchmarks/smoke_benchmark.py`
- `utils/__init__.py`
- `utils/io.py`
- `utils/stats.py`


## 4. 目录与职责

建议本阶段先建立如下目录：

```text
experiments/lancedb_graph/
├── EXECUTION_PLAN.md
├── PHASE1_PLAN.md
├── README.md
├── config.py
├── data_prep/
├── storage_models/
├── query_engines/
├── benchmarks/
└── utils/
```

各目录职责如下：

- `data_prep/`: 从原始图数据生成节点表和边表
- `data_prep/`: 从原始图数据或合成图数据生成节点表和边表
- `storage_models/`: LanceDB 基础图存储实现
- `query_engines/`: 单跳和 `k-hop` 查询逻辑
- `benchmarks/`: 基础验证脚本
- `utils/`: 公共工具函数


## 5. Schema 规划

阶段一采用简化 schema，优先保证一致性与易实现。

### 节点表 `nodes`

- `node_id`
- `node_type`
- `degree_out`
- `degree_in`
- `attrs_json`

### 边表 `edges`

- `edge_id`
- `src_id`
- `dst_id`
- `edge_type`
- `src_type`
- `dst_type`
- `attrs_json`

说明：

- `attrs_json` 在阶段一统一使用 JSON 字符串
- `edge_id` 使用稳定生成方式
- `node_type` 为空时使用默认值


## 6. 实施步骤

### 步骤一：搭建目录骨架

创建以下目录与基础文件：

- `data_prep/`
- `storage_models/`
- `query_engines/`
- `benchmarks/`
- `utils/`
- 各目录下的 `__init__.py`

同时补充 `README.md` 初稿，记录：

- 实验目标
- 当前阶段范围
- 数据来源
- 运行方式


### 步骤二：建立配置文件

实现 `config.py`，集中管理：

- 项目根目录
- 输入数据路径
- LanceDB 存储路径
- 节点表名
- 边表名
- smoke benchmark 默认数据规模
- 默认随机种子
- 默认 `k-hop` 上限


### 步骤三：实现数据转换脚本

实现 `data_prep/build_graph_tables.py`，完成：

1. 读取原始图数据
2. 生成边记录
3. 聚合节点信息
4. 统计入度和出度
5. 生成节点表 DataFrame
6. 生成边表 DataFrame

建议先复用现有 `triples.tsv` 作为输入源。

脚本输出建议支持：

- 返回 `nodes_df`
- 返回 `edges_df`
- 可选写入 LanceDB


### 步骤四：实现可控图数据生成脚本

实现 `data_prep/generate_synthetic_graph.py`，用于直接生成 `.tsv` 图数据文件。

目标：

1. 避免后续实验完全受限于现有大规模数据的分布
2. 能构造更适合图检索实验的节点度分布与社区结构
3. 让不同存储方案在统一、可控的数据上进行对比

阶段一建议至少支持以下生成模式：

- `uniform`: 接近均匀分布
- `powerlaw`: 带明显热点节点的长尾分布
- `community`: 带社区结构的图

输出格式统一为带表头的 `.tsv`：

- `head_type`
- `head`
- `relation`
- `tail_type`
- `tail`

阶段一的目标不是一次覆盖所有图生成模型，而是先提供一个可以稳定产出实验数据的生成器。


### 步骤五：实现基础 LanceDB 图存储

实现 `storage_models/lancedb_graph_basic.py`，定义基础类：

- `LanceDBGraphBasic`

建议实现的方法：

- `build_from_tsv()`
- `build_from_dataframes()`
- `load()`
- `stats()`
- `get_node(node_id)`
- `query_out_neighbors(node_id, edge_type=None)`
- `query_in_neighbors(node_id, edge_type=None)`
- `query_neighbors(node_id, edge_type=None)`
- `query_k_hop(node_id, k)`

本阶段要求：

- 查询结果正确
- 接口统一
- 不提前加入复杂优化


### 步骤六：实现基础查询逻辑

实现 `query_engines/basic_queries.py`，包含：

- 单点节点查找
- 单跳出邻居查询
- 单跳入邻居查询
- 单跳双向邻居查询
- 朴素 `k-hop` 查询

返回结构建议统一为字典：

- `rows`
- `count`
- `time_ms`


### 步骤七：实现基础统计

实现 `utils/stats.py`，输出：

- 节点总数
- 边总数
- 节点类型数
- 边类型数
- 平均出度
- 平均入度

这一步主要用于：

- 验证数据转换正确性
- 作为 smoke benchmark 输出内容


### 步骤八：实现 smoke benchmark

实现 `benchmarks/smoke_benchmark.py`，验证完整链路：

1. 构建基础图表
2. 读取已构建表
3. 随机挑选若干节点
4. 运行出邻居查询
5. 运行入邻居查询
6. 运行双向邻居查询
7. 运行 `2-hop` 查询
8. 输出简单耗时和结果规模

这一脚本只承担“能跑通”的职责，不作为正式 benchmark。


## 7. 建议开发顺序

建议按如下顺序实施：

1. `config.py`
2. `README.md`
3. `generate_synthetic_graph.py`
4. `build_graph_tables.py`
5. `lancedb_graph_basic.py`
6. `basic_queries.py`
7. `stats.py`
8. `smoke_benchmark.py`


## 8. 关键实现决策

阶段一建议统一以下决策：

### `edge_id`

使用稳定生成方式，例如输入顺序递增：

- `edge_0`
- `edge_1`
- `edge_2`

### `attrs_json`

阶段一统一使用 JSON 字符串，默认值为 `{}`。

### `k-hop` 语义

阶段一先固定为“按有向出边扩展”。

### 查询返回格式

统一返回：

```python
{
    "rows": ...,
    "count": ...,
    "time_ms": ...,
}
```


## 9. 阶段一验收标准

满足以下条件即可视为阶段一完成：

- 目录骨架已建立
- `config.py` 可统一管理路径和表名
- 能从原始图数据构建 `nodes` 和 `edges`
- 能生成一份可控分布的 `.tsv` 图数据
- LanceDB 中能成功创建两张表
- 能执行单跳出边查询
- 能执行单跳入边查询
- 能执行双向邻居查询
- 能执行基础 `2-hop` 查询
- `stats()` 能输出基础统计
- `smoke_benchmark.py` 能跑通
- `README.md` 已记录运行方式和 schema


## 10. 风险与控制

阶段一需要重点控制以下风险：

- 数据字段命名不统一，导致转换层和查询层脱节
- 查询层大面积退化为全表加载，影响后续演进
- `k-hop` 逻辑一次写得过重，拖慢整体进度

本阶段的基本原则是：

- 先求正确
- 再求可运行
- 最后才考虑优化
