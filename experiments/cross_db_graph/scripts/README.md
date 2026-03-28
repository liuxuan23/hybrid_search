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

### `run_postgres_benchmark.sh`

作用：

1. 将 TSV 图数据导入 PostgreSQL
2. 执行一次 PostgreSQL benchmark
3. benchmark 结束后自动删除本次创建的表

默认参数：

- TSV 路径：`/data/dataset/graph_data/cluster/synthetic_community_100000.tsv`
- PostgreSQL DSN：`postgresql://postgres:postgres123@localhost:5432/graph_bench`

自动清理行为：

- 脚本退出时会自动删除：
  - `graph_edges`
  - `graph_nodes`

说明：

- 该脚本适合“一次性导入 + 测试 + 清理”的 benchmark 流程
- 如果需要保留 PostgreSQL 数据做进一步排查或手工分析，需要临时去掉脚本中的 `trap cleanup EXIT`

### `run_arangodb_benchmark.sh`

作用：

1. 将 TSV 图数据导入 ArangoDB
2. 执行一次 ArangoDB benchmark
3. benchmark 结束后自动删除本次创建的 graph 与 collection

默认参数：

- TSV 路径：`/data/dataset/graph_data/cluster/synthetic_community_100000.tsv`
- ArangoDB URL：`http://127.0.0.1:8529`
- 数据库名：`graph_bench`
- 用户名：`root`
- 密码：`123456`

自动清理行为：

- 脚本退出时会自动删除：
  - `graph_bench_graph`
  - `graph_edges`
  - `graph_nodes`

说明：

- 该脚本适合“一次性导入 + 测试 + 清理”的 benchmark 流程
- 如果需要保留 ArangoDB 数据做进一步排查或手工分析，需要临时去掉脚本中的 `trap cleanup EXIT`

## 当前各脚本职责

- `export_graph_data.py`
  - 从 LanceDB 图存储中抽取 seed 节点并生成 `seeds.json`

- `build_lancedb_from_tsv.sh`
  - 从 TSV 构建 LanceDB 图表存储

- `run_lancedb_benchmark.sh`
  - 一键执行当前 LanceDB 实验流程

- `import_postgres.py`
  - PostgreSQL 数据导入脚本

- `run_postgres_benchmark.sh`
  - 一键执行 PostgreSQL 导入、benchmark 与自动清理

- `import_arangodb.py`
  - ArangoDB 数据导入脚本

- `run_arangodb_benchmark.sh`
  - 一键执行 ArangoDB 导入、benchmark 与自动清理

- `validate_consistency.py`
  - 预留给跨数据库结果一致性校验

## 当前推荐使用方式

如果是第一次跑，推荐顺序是：

1. 先执行 `build_lancedb_from_tsv.sh`
2. 再确认 `config.py` 中的 `LANCEDB_DB_PATH`
3. 再执行 `run_lancedb_benchmark.sh`

如果只是想跑通当前 LanceDB 实验流程，直接执行：

- `experiments/cross_db_graph/scripts/run_lancedb_benchmark.sh`

如果要跑 PostgreSQL 或 ArangoDB，一般直接执行：

- `experiments/cross_db_graph/scripts/run_postgres_benchmark.sh`
- `experiments/cross_db_graph/scripts/run_arangodb_benchmark.sh`

注意：

- 上面两个脚本在执行结束后会自动清理导入的数据表 / collection
- 因此默认不会保留 benchmark 后的数据库状态

## 前提条件

执行前需保证：

- `hybrid_search` 项目 Python 环境可用
- `config.py` 中的 `LANCEDB_DB_PATH` 指向可用图库存储
- 对应 LanceDB 库中存在：`nodes`、`edges`、`adj_index`