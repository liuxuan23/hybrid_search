# Cross-DB Graph Benchmark Scripts

这里固定用户实际操作实验时需要执行的步骤。

## 当前可直接使用的脚本

### `build_lancedb_from_tsv.sh`

作用：

1. 从原始 TSV 三元组构建 LanceDB 图存储
2. 在目标目录下生成 `nodes`、`edges`、`adj_index`

默认参数：

- TSV 路径：`/data/dataset/graph_data/triples.tsv`
- 输出目录：`storage/lancedb_graph/cross_db_graph_benchmark`
- cluster 策略：`by_node_type`

### `run_lancedb_benchmark.sh`

作用：

1. 自动从 LanceDB 图库生成 `seeds.json`
2. 自动执行一次 LanceDB benchmark

输出位置：

- `experiments/cross_db_graph/seeds.json`
- `experiments/cross_db_graph/results/<run_id>/`

## 当前各脚本职责

- `export_graph_data.py`
  - 从 LanceDB 图存储中抽取 seed 节点并生成 `seeds.json`

- `build_lancedb_from_tsv.sh`
  - 从 TSV 构建 LanceDB 图表存储

- `run_lancedb_benchmark.sh`
  - 一键执行当前 LanceDB 实验流程

- `import_postgres.py`
  - 预留给 PostgreSQL 数据导入

- `import_arangodb.py`
  - 预留给 ArangoDB 数据导入

- `validate_consistency.py`
  - 预留给跨数据库结果一致性校验

## 当前推荐使用方式

如果是第一次跑，推荐顺序是：

1. 先执行 `build_lancedb_from_tsv.sh`
2. 再确认 `config.py` 中的 `LANCEDB_DB_PATH`
3. 再执行 `run_lancedb_benchmark.sh`

如果只是想跑通当前 LanceDB 实验流程，直接执行：

- `experiments/cross_db_graph/scripts/run_lancedb_benchmark.sh`

## 前提条件

执行前需保证：

- `hybrid_search` 项目 Python 环境可用
- `config.py` 中的 `LANCEDB_DB_PATH` 指向可用图库存储
- 对应 LanceDB 库中存在：`nodes`、`edges`、`adj_index`