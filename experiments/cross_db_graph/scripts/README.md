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

### `build_lance_graph_from_tsv.py`

作用：

1. 按 `lance_graph` 官方目录式存储链路初始化图目录
2. 写出官方独立使用的 `graph.yaml`
3. 写出官方查询所需的 `nodes.lance`、`edges.lance`

默认参数：

- TSV 路径：`/data/dataset/graph_data/triples.tsv`
- 输出目录：`storage/lance_graph/cross_db_graph_benchmark`

说明：

- 该链路用于 `lance_graph` 官方查询扩展的独立 benchmark
- 与 `build_lancedb_from_tsv.sh` 不同，这里不会生成 `adj_index`
- `lancedb` 与 `lance_graph` 应分别使用各自独立构建的数据目录

### `run_lancedb_benchmark.sh`

作用：

1. 自动从 LanceDB 图库生成 `seeds.json`
2. 自动执行一次 LanceDB benchmark

输出位置：

- `experiments/cross_db_graph/seeds.json`
- `experiments/cross_db_graph/results/<run_id>/`

### `run_lance_graph_benchmark.sh`

作用：

1. 按官方 `lance_graph` 独立链路从 TSV 构建图目录
2. 直接从 TSV 生成 `seeds.json`
3. 执行一次 `lance_graph` benchmark

默认参数：

- TSV 路径：`/data/dataset/graph_data/cluster/synthetic_community_100000.tsv`
- 输出目录：`storage/lance_graph/cross_db_graph_benchmark`
- benchmark 模式：`warm`

说明：

- 该脚本不会构建 `adj_index`
- 该脚本用于官方 `lance_graph` 独立目录式存储 + 官方查询链路
- 第 3 个参数可传 `warm` / `coldish` / `group-coldish`

### `run_single_seed_queries.py`

作用：

1. 对单个 seed 运行 `neighbor`、`k_hop(2)`、`k_hop(3)`
2. 支持人工在每次运行前清理服务缓存
3. 适合单点排查与冷缓存实验

支持引擎：

- `lancedb`
- `lance_graph`
- `postgres`
- `arangodb`

补充参数：

- `--db-path`
  - 用于给 `lancedb` / `lance_graph` 显式指定数据目录
  - 推荐在 `lance_graph` 独立链路下显式传入

### `clear_service_caches.sh`

作用：

1. 重启 PostgreSQL / ArangoDB 服务，尽量清理服务级缓存
2. 输出 Linux 下可选的 page cache 清理提示

支持模式：

- `postgres`
- `arangodb`
- `all`
- `hint`

说明：

- 该脚本主要用于辅助 cold-ish / 手工冷缓存测试
- 需要本机具备 `sudo` 权限时才可自动重启服务
- 即便重启服务，也不等于一定清除了 OS page cache

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

- `build_lance_graph_from_tsv.py`
  - 从 TSV 构建 `lance_graph` 官方目录式图存储

- `run_lancedb_benchmark.sh`
  - 一键执行当前 LanceDB 实验流程

- `run_lance_graph_benchmark.sh`
  - 一键执行官方 `lance_graph` 实验流程

- `run_single_seed_queries.py`
  - 对单个 seed 执行 1-hop / 2-hop / 3-hop 查询

- `clear_service_caches.sh`
  - 辅助重启数据库服务并提示清理 OS page cache

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

如果想跑官方 `lance_graph` 独立链路，执行：

- `experiments/cross_db_graph/scripts/run_lance_graph_benchmark.sh`

如果想做单点查询与手工冷缓存实验，执行：

- `uv run python -m experiments.cross_db_graph.scripts.run_single_seed_queries --engine postgres --seed "type1:node_123"`
- `uv run python -m experiments.cross_db_graph.scripts.run_single_seed_queries --engine lance_graph --db-path /path/to/lance_graph_dir --seed "type1:node_123"`

如果想在两次查询之间重启服务缓存，可执行：

- `bash experiments/cross_db_graph/scripts/clear_service_caches.sh postgres`
- `bash experiments/cross_db_graph/scripts/clear_service_caches.sh arangodb`
- `bash experiments/cross_db_graph/scripts/clear_service_caches.sh all`

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
- `config.py` 中的 `LANCE_GRAPH_DB_PATH` 指向可用的 `lance_graph` 图目录
- 对应 LanceDB 库中存在：`nodes`、`edges`、`adj_index`
- 对应 `lance_graph` 图目录中存在：`graph.yaml`、`nodes.lance`、`edges.lance`
- 如需自动重启 PostgreSQL / ArangoDB 服务，当前用户需具备 `sudo` 权限