# 本机 PostgreSQL 配置流程

本文档记录 `hybrid_search` 使用本机 PostgreSQL 实例的最小配置流程，目标是让当前项目使用独立数据库 `graph_bench`，不依赖其他项目的 k3d PostgreSQL。

## 目标配置

- Host: `localhost`
- Port: `5432`
- User: `postgres`
- Password: `postgres123`
- Database: `graph_bench`

对应 DSN：

- `postgresql://postgres:postgres123@localhost:5432/graph_bench`

当前项目中的默认配置位置：

- `experiments/cross_db_graph/config.py`

## 1. 安装 PostgreSQL

以 Ubuntu / Debian 为例：

```bash
sudo apt update
sudo apt install -y postgresql postgresql-contrib
```

## 2. 启动 PostgreSQL 服务

```bash
sudo systemctl enable postgresql
sudo systemctl start postgresql
sudo systemctl status postgresql
```

## 3. 设置 `postgres` 用户密码

```bash
sudo -u postgres psql
```

在 `psql` 中执行：

```sql
ALTER USER postgres WITH PASSWORD 'postgres123';
```

退出：

```sql
\q
```

## 4. 创建独立数据库 `graph_bench`

```bash
sudo -u postgres createdb graph_bench
```

如果数据库已存在，可跳过。

## 5. 验证连接

```bash
psql "postgresql://postgres:postgres123@localhost:5432/graph_bench" -c "\l"
```

如果该命令成功返回数据库列表，则说明本机 PostgreSQL 已可供 `hybrid_search` 使用。

## 6. 项目侧配置

`experiments/cross_db_graph/config.py` 当前默认使用：

```python
POSTGRES_DSN = os.environ.get(
    "POSTGRES_DSN",
    "postgresql://postgres:postgres123@localhost:5432/graph_bench",
)
```

如需临时覆盖，可设置环境变量：

```bash
export POSTGRES_DSN="postgresql://postgres:postgres123@localhost:5432/graph_bench"
```

## 7. 导入社区图数据

使用以下命令将社区图 TSV 导入 PostgreSQL：

```bash
/home/liuxuan/workplace/.venv/bin/python -m experiments.cross_db_graph.scripts.import_postgres \
  --tsv /data/dataset/graph_data/cluster/synthetic_community_100000.tsv
```

## 8. 后续用途

导入成功后，可继续：

1. 验证 `PostgresGraphAdapter` 查询正确性
2. 将 PostgreSQL 接入 cross-db benchmark runner
3. 与 LanceDB、后续的 ArangoDB 在同一 seeds/workloads 下做对比

## 9. 注意事项

- 当前项目应使用独立数据库 `graph_bench`
- 避免与其他项目共享同一数据库中的表
- 若 benchmark 期间有其他数据库重负载任务，延迟结果可能受干扰
- 若未来需要更强隔离，可再切换到独立端口或独立实例

## 10. 当前验证结果

本机已成功执行：

```bash
psql "postgresql://postgres:postgres123@localhost:5432/graph_bench" -c "\l"
```

说明：

- PostgreSQL 当前可通过本机 `localhost:5432` 访问
- `graph_bench` 数据库已可连接
- 可继续尝试运行导入脚本
