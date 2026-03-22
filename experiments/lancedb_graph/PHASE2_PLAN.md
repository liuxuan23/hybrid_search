# 阶段二实施计划

## 1. 阶段目标

阶段二的目标是在当前 `LanceDB nodes + edges` baseline 基础上，引入基于 `row_id` 的邻接索引表，并结合 `cluster_id` 聚簇优化，建立适用于单跳查询与基础多跳扩展的索引驱动执行路径。

本阶段的重点不再是单纯比较不同边表布局，而是围绕“邻接索引表主导查询”这一核心思路，验证以下问题：

- 能否通过 `adj_index` 将单跳查询从边表过滤转为索引表查找
- 能否基于稳定 `row_id` 实现 `row_id -> row_id` 的多跳扩展路径
- 能否通过 `cluster_id` 对索引表聚簇，提升局部扩展与社区型查询的效率
- 在需要回节点表或边表 materialize 结果时，整体收益是否仍然成立

阶段二完成后，应具备如下能力：

- 能从输入图数据构建 `nodes`、`edges` 和 `adj_index`
- 能基于 `node_id` 在 `adj_index` 中找到节点索引项
- 能执行基于邻接索引的单跳出邻居与入邻居查询
- 能执行基于邻接索引的基础 `2-hop` 与 `3-hop` 扩展
- 能在 clustered 与 unclustered 索引表之间进行对照实验
- 能与阶段一 baseline 进行统一 benchmark 对比


## 2. 本阶段范围

### 包含内容

- 固化阶段一 baseline
- 基于稳定 `row_id` 的邻接索引表 `adj_index`
- `cluster_id` 分配逻辑与索引表聚簇写入
- 基于索引表的单跳查询接口
- 基于索引表的基础多跳扩展接口
- baseline / adjacency / adjacency+clustered benchmark
- 邻接索引统计与局部性分析
- `PHASE2_PLAN.md`、设计文档与新增代码骨架

### 不包含内容

- `chunk` 表
- 动态更新与增量维护优化
- 热点缓存
- 高级 frontier 批量执行优化
- 边属性索引化
- 分布式 partition 或 sharding
- 外部系统对比


## 3. 阶段二交付物

建议在 `experiments/lancedb_graph/` 下完成以下首批文件：

- `PHASE2_PLAN.md`
- `data_prep/build_adjacency_index.py`
- `data_prep/build_cluster_assignments.py`
- `data_prep/build_query_samples.py`
- `storage_models/lancedb_graph_adjacency.py`
- `query_engines/adjacency_queries.py`
- `query_engines/traversal.py`
- `benchmarks/benchmark_adjacency_vs_baseline.py`
- `benchmarks/benchmark_cluster_locality.py`
- `utils/adjacency_stats.py`
- `utils/locality_metrics.py`
- `docs/adjacency_design.md`
- `docs/clustering_strategy.md`


## 4. 目录与职责

建议阶段二在现有目录基础上扩展如下内容：

```text
experiments/lancedb_graph/
├── EXECUTION_PLAN.md
├── PHASE1_PLAN.md
├── PHASE2_PLAN.md
├── README.md
├── config.py
├── data_prep/
│   ├── build_graph_tables.py
│   ├── build_adjacency_index.py
│   ├── build_cluster_assignments.py
│   └── build_query_samples.py
├── storage_models/
│   ├── lancedb_graph_basic.py
│   └── lancedb_graph_adjacency.py
├── query_engines/
│   ├── basic_queries.py
│   ├── adjacency_queries.py
│   └── traversal.py
├── benchmarks/
│   ├── smoke_benchmark.py
│   ├── local_validation.py
│   ├── benchmark_adjacency_vs_baseline.py
│   └── benchmark_cluster_locality.py
├── utils/
│   ├── stats.py
│   ├── adjacency_stats.py
│   └── locality_metrics.py
└── docs/
    ├── adjacency_design.md
    └── clustering_strategy.md
```

各目录职责如下：

- `data_prep/`: 构建邻接索引、cluster 分配与 benchmark 样本
- `storage_models/`: baseline 与 adjacency 存储实现
- `query_engines/`: 单跳索引查询与多跳扩展逻辑
- `benchmarks/`: baseline 与 adjacency 的统一评测脚本
- `utils/`: 邻接索引统计、局部性指标与通用工具
- `docs/`: 记录邻接索引与聚簇策略设计


## 5. `adj_index` Schema 规划

阶段二采用“每节点一行”的简化邻接索引表，不引入 `chunk` 表，优先验证 `row_id` 驱动查询闭环。

### 5.1 邻接索引表 `adj_index`

建议至少包含以下字段：

- `node_id`
- `node_type`
- `cluster_id`
- `degree_out`
- `degree_in`
- `out_neighbor_row_ids`
- `in_neighbor_row_ids`
- `attrs_json`

### 5.2 字段说明

#### `node_id`

业务侧节点唯一标识，用于查询入口和与 `nodes` 表做映射。

#### `node_type`

节点类型，便于后续做过滤、统计与 cluster 规则分析。

#### `cluster_id`

用于聚簇排序的逻辑分组键。阶段二优先使用简单、稳定、可复现的 cluster 分配策略。

#### `degree_out` / `degree_in`

分别表示出度与入度。主要用于：

- benchmark 分层采样
- 高度节点分析
- 局部性与热点诊断

#### `out_neighbor_row_ids`

当前节点所有出邻居在 `adj_index` 中对应的 `row_id` 列表。查询单跳出邻居和多跳扩展时优先使用该字段。

#### `in_neighbor_row_ids`

当前节点所有入邻居在 `adj_index` 中对应的 `row_id` 列表。查询单跳入邻居时优先使用该字段。

#### `attrs_json`

阶段二仍统一使用 JSON 字符串，默认值沿用阶段一。

### 5.3 关于 `row_id`

本阶段明确使用 Lance 已验证可稳定使用的 `row_id`。因此：

- 邻接索引中直接保存邻居在 `adj_index` 中的 `row_id`
- 多跳扩展路径以 `row_id` 作为节点跳转入口
- 需要 materialize 时，再回 `adj_index`、`nodes` 或 `edges` 获取详情

### 5.4 约束与设计决策

阶段二建议统一以下决策：

- 默认保留有向图语义
- 默认保留多重边语义；邻接列表是否去重可在查询层控制
- 默认保留自环
- 邻接列表顺序先按构建顺序或稳定 row_id 顺序生成，不提前做复杂重排
- `cluster_id` 写入前需确定，聚簇主要作用于 `adj_index`


## 6. API 设计

阶段二在保留 `LanceDBGraphBasic` 的同时，引入新的存储实现：

- `LanceDBGraphAdjacency`

### 6.1 `LanceDBGraphAdjacency`

建议实现的方法：

- `build_from_tsv()`
- `build_from_dataframes()`
- `build_adjacency_index()`
- `load()`
- `stats()`
- `get_adj_entry(node_id)`
- `query_out_neighbors_index(node_id, materialize=False)`
- `query_in_neighbors_index(node_id, materialize=False)`
- `query_neighbors_index(node_id, materialize=False)`
- `query_k_hop_index(node_id, k, materialize=False)`
- `query_out_neighbors_baseline(node_id)`
- `query_in_neighbors_baseline(node_id)`

### 6.2 查询接口说明

#### `get_adj_entry(node_id)`

输入：

- `node_id`

返回：

- 该节点在 `adj_index` 中的索引行
- 包括 `cluster_id`、度数和出入邻居 row ids

#### `query_out_neighbors_index(node_id, materialize=False)`

输入：

- `node_id`
- `materialize`: 是否回表获取邻居节点详情

输出建议统一为：

```python
{
    "rows": ...,
    "count": ...,
    "time_ms": ...,
    "mode": "index-only" | "materialized",
}
```

语义：

- `materialize=False` 时返回邻居 row ids 或最小节点标识
- `materialize=True` 时返回邻居节点详情

#### `query_in_neighbors_index(node_id, materialize=False)`

与 `query_out_neighbors_index()` 对称。

#### `query_neighbors_index(node_id, materialize=False)`

同时返回出入邻居，结果中建议保留方向字段：

- `direction = "out"`
- `direction = "in"`

#### `query_k_hop_index(node_id, k, materialize=False)`

语义：

- 使用 `row_id -> row_id` 路径进行 BFS 扩展
- 每一层 frontier 以 `row_id` 集合表示
- 通过 `visited` 集合避免重复访问
- 默认按有向出边扩展

返回建议统一为：

```python
{
    "rows": ...,          # 分 hop 返回
    "count": ...,         # 总扩展结果数
    "time_ms": ...,       # 总耗时
    "frontier_sizes": ...,# 每跳 frontier 大小
    "mode": "index-only" | "materialized",
}
```

### 6.3 benchmark 相关 API

建议补充辅助接口：

- `describe_adjacency_layout()`
- `list_high_degree_nodes()`
- `list_nodes_by_cluster(cluster_id)`
- `materialize_rows_by_row_id(row_ids)`

这些接口主要用于 benchmark、局部性分析与后续阶段扩展。


## 7. 实施步骤

### 步骤一：冻结 baseline

固定阶段一已有能力，确保以下内容作为对照组可稳定复用：

- `nodes + edges` 构建流程
- baseline 单跳查询
- baseline `k-hop` 查询
- baseline smoke benchmark

本阶段不再随意修改 baseline 语义。


### 步骤二：设计并实现 `adj_index` 构建逻辑

实现 `data_prep/build_adjacency_index.py`，完成：

1. 基于 `nodes_df` 生成索引候选数据
2. 写入 `adj_index` 并获得每个节点对应的 `row_id`
3. 构建 `node_id -> row_id` 映射
4. 遍历 `edges_df` 回填：
   - `src_id` 的 `out_neighbor_row_ids`
   - `dst_id` 的 `in_neighbor_row_ids`
5. 生成最终 `adj_index`

脚本输出建议支持：

- 返回 `adj_index_df`
- 返回 `node_id -> row_id` 映射
- 可选直接写入 LanceDB


### 步骤三：实现 `cluster_id` 分配

实现 `data_prep/build_cluster_assignments.py`，集中管理 cluster 生成策略。

阶段二建议至少支持：

- `by_node_type`
- `hash_bucket`
- `community_label`（如果 synthetic graph 已有社区标签）

输出建议：

- 在 `nodes_df` 或 `adj_index_df` 中补充 `cluster_id`
- 保证同一输入可复现


### 步骤四：实现 adjacency 存储模型

实现 `storage_models/lancedb_graph_adjacency.py`，定义：

- `LanceDBGraphAdjacency`

要求：

- 可从 `.tsv` 构建
- 可从 `nodes_df + edges_df` 构建
- 可加载已有 `adj_index`
- 可选择 clustered / unclustered 构建方式
- 保持与 baseline 尽量统一的查询接口风格


### 步骤五：实现基于索引的查询逻辑

实现 `query_engines/adjacency_queries.py`，包含：

- `get_adj_entry()`
- 单跳出邻居查询
- 单跳入邻居查询
- 单跳双向邻居查询
- `row_id` materialize 辅助逻辑

返回结构建议统一为：

- `rows`
- `count`
- `time_ms`
- `mode`


### 步骤六：实现基于索引的基础多跳扩展

实现 `query_engines/traversal.py`，包含：

- 基于 `row_id` 的 BFS
- `visited` 管理
- `frontier` 管理
- 每跳结果汇总

本阶段要求：

- 先保证语义正确
- 默认按出边扩展
- 先不引入高级 frontier 合并优化


### 步骤七：建立阶段二 benchmark

实现以下 benchmark：

- `benchmarks/benchmark_adjacency_vs_baseline.py`
- `benchmarks/benchmark_cluster_locality.py`

至少覆盖以下场景：

1. baseline 单跳出邻居查询
2. adjacency 单跳出邻居查询
3. adjacency clustered 单跳出邻居查询
4. baseline `2-hop`
5. adjacency `2-hop`
6. community 图上的 cluster 局部扩展
7. 低/中/高度节点分层测试

统一记录：

- 平均延迟
- `P50`
- `P95`
- `P99`
- 结果规模
- 每跳 frontier 大小
- 回表耗时
- 构建耗时
- 存储占用


## 8. 建议开发顺序

建议按如下顺序实施：

1. `PHASE2_PLAN.md`
2. `data_prep/build_adjacency_index.py`
3. `data_prep/build_cluster_assignments.py`
4. `storage_models/lancedb_graph_adjacency.py`
5. `query_engines/adjacency_queries.py`
6. `query_engines/traversal.py`
7. `benchmarks/benchmark_adjacency_vs_baseline.py`
8. `benchmarks/benchmark_cluster_locality.py`
9. `utils/adjacency_stats.py`
10. `utils/locality_metrics.py`


## 9. 新增文件的代码骨架列表

下面给出阶段二建议新增文件的代码骨架，作为实现起点。

### 9.1 `data_prep/build_adjacency_index.py`

```python
from typing import Dict, Tuple

import pandas as pd


def build_adjacency_index_dataframe(
    nodes_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    cluster_assignments: Dict[str, str] | None = None,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """构建 adj_index DataFrame，并返回 node_id 到 row_id 的映射。"""
    raise NotImplementedError()
```

### 9.2 `data_prep/build_cluster_assignments.py`

```python
from typing import Dict

import pandas as pd


def assign_clusters_by_node_type(nodes_df: pd.DataFrame) -> Dict[str, str]:
    """按 node_type 生成 cluster_id。"""
    raise NotImplementedError()


def assign_clusters_by_hash(nodes_df: pd.DataFrame, num_buckets: int) -> Dict[str, str]:
    """按 node_id 哈希分桶生成 cluster_id。"""
    raise NotImplementedError()
```

### 9.3 `data_prep/build_query_samples.py`

```python
from typing import Dict, List

import pandas as pd


def build_degree_bucket_samples(nodes_df: pd.DataFrame, sample_size: int) -> Dict[str, List[str]]:
    """构建低/中/高度节点样本。"""
    raise NotImplementedError()
```

### 9.4 `storage_models/lancedb_graph_adjacency.py`

```python
from typing import Optional

import lancedb


class LanceDBGraphAdjacency:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = lancedb.connect(db_path)
        self.nodes_tbl = None
        self.edges_tbl = None
        self.adj_index_tbl = None

    def build_from_tsv(self, tsv_path: str, cluster_strategy: str = "by_node_type"):
        raise NotImplementedError()

    def build_from_dataframes(self, nodes_df, edges_df, cluster_strategy: str = "by_node_type"):
        raise NotImplementedError()

    def load(self):
        raise NotImplementedError()

    def stats(self):
        raise NotImplementedError()

    def get_adj_entry(self, node_id: str):
        raise NotImplementedError()

    def query_out_neighbors_index(self, node_id: str, materialize: bool = False):
        raise NotImplementedError()

    def query_in_neighbors_index(self, node_id: str, materialize: bool = False):
        raise NotImplementedError()

    def query_neighbors_index(self, node_id: str, materialize: bool = False):
        raise NotImplementedError()

    def query_k_hop_index(self, node_id: str, k: int, materialize: bool = False):
        raise NotImplementedError()
```

### 9.5 `query_engines/adjacency_queries.py`

```python
import time


def get_adj_entry(adj_index_tbl, node_id: str):
    """获取某个节点在 adj_index 中的索引项。"""
    raise NotImplementedError()


def query_out_neighbors_index(adj_index_tbl, node_id: str, materialize: bool = False):
    """基于邻接索引查询出邻居。"""
    raise NotImplementedError()


def query_in_neighbors_index(adj_index_tbl, node_id: str, materialize: bool = False):
    """基于邻接索引查询入邻居。"""
    raise NotImplementedError()
```

### 9.6 `query_engines/traversal.py`

```python
def query_k_hop_index(adj_index_tbl, node_id: str, k: int, materialize: bool = False):
    """基于 row_id 邻接索引执行 k-hop 扩展。"""
    raise NotImplementedError()
```

### 9.7 `benchmarks/benchmark_adjacency_vs_baseline.py`

```python
def main():
    """对比 baseline 与 adjacency 查询性能。"""
    raise NotImplementedError()


if __name__ == "__main__":
    main()
```

### 9.8 `benchmarks/benchmark_cluster_locality.py`

```python
def main():
    """评估 clustered adjacency 在局部性查询上的收益。"""
    raise NotImplementedError()


if __name__ == "__main__":
    main()
```

### 9.9 `utils/adjacency_stats.py`

```python
def build_adjacency_stats(adj_index_tbl):
    """输出邻接索引表基础统计。"""
    raise NotImplementedError()
```

### 9.10 `utils/locality_metrics.py`

```python
def compute_cluster_locality_metrics(results):
    """计算 cluster 相关局部性指标。"""
    raise NotImplementedError()
```


## 10. 阶段二验收标准

满足以下条件即可视为阶段二完成：

- baseline 已固定并可重复运行
- 能从输入图数据构建 `adj_index`
- `adj_index` 中已包含 `out_neighbor_row_ids` 和 `in_neighbor_row_ids`
- 能通过 `node_id` 获取邻接索引项
- 能执行基于邻接索引的单跳出邻居查询
- 能执行基于邻接索引的单跳入邻居查询
- 能执行基于邻接索引的基础 `2-hop` 查询
- clustered 与 unclustered 的索引表都可构建
- 能对 baseline、adjacency、adjacency+clustered 做统一 benchmark
- 能输出邻接统计与局部性分析结果
- `PHASE2_PLAN.md` 已记录 schema、API 与运行思路


## 11. 风险与控制

阶段二需要重点控制以下风险：

- `row_id` 使用边界不清晰，导致构建与查询语义不一致
- 邻接索引行过大，导致热点节点查询波动明显
- materialize 回表吞掉索引层收益
- `cluster_id` 分配过于粗糙，导致聚簇收益不明显
- benchmark 没有区分 index-only 与 materialized，结论不可解释

本阶段的基本原则是：

- 先建立索引驱动闭环
- 再验证聚簇收益
- 全程保留 baseline 对照
- 优先保证实验可解释
````
