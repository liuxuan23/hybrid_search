# 本机 ArangoDB 配置流程

本文档记录 `hybrid_search` 在 Ubuntu 22.04 本机安装并启用 `ArangoDB` 的最小流程，目标是让当前项目可直接使用本机 `ArangoDB` 实例运行 `cross_db_graph` benchmark。

## 目标配置

- Host: `127.0.0.1`
- Port: `8529`
- User: `root`
- Password: 本机安装时设置的 root 密码
- Database: `graph_bench`

当前项目中的默认配置位置：

- `experiments/cross_db_graph/config.py`

对应配置项：

```python
ARANGODB_URL = "http://127.0.0.1:8529"
ARANGODB_DB = "graph_bench"
ARANGODB_USERNAME = "root"
ARANGODB_PASSWORD = "<your-password>"
```

## 1. 系统环境

当前验证通过的环境：

- OS: `Ubuntu 22.04`
- Package Manager: `apt`

## 2. 添加 ArangoDB APT 源

本次安装过程中，官方仓库签名 key 已过期，因此采用了临时信任仓库的方式完成安装。

先写入源：

```bash
echo "deb [trusted=yes] https://download.arangodb.com/arangodb312/DEBIAN/ /" \
  | sudo tee /etc/apt/sources.list.d/arangodb.list
```

然后更新索引：

```bash
sudo apt-get update
```

说明：

- 即使看到 `NO_PUBKEY` 或 `GPG error` 警告，只要 `Packages` 成功拉取，后续通常仍可安装。
- 这是临时实验环境方案，不建议长期用于生产环境。

## 3. 安装 ArangoDB

```bash
sudo apt-get install -y arangodb3
```

安装过程中会提示设置 `root` 密码，请记住该密码，后续需要写入：

- `experiments/cross_db_graph/config.py`

## 4. 启动服务

```bash
sudo systemctl enable --now arangodb3
```

检查服务状态：

```bash
systemctl status arangodb3 --no-pager
```

检查端口监听：

```bash
ss -ltnp | grep 8529
```

若成功，应看到类似：

```text
LISTEN 0 64 127.0.0.1:8529 0.0.0.0:*
```

## 5. 项目侧配置

编辑：

- `experiments/cross_db_graph/config.py`

将 ArangoDB 配置改为本机实际值，例如：

```python
ARANGODB_URL = "http://127.0.0.1:8529"
ARANGODB_DB = "graph_bench"
ARANGODB_USERNAME = "root"
ARANGODB_PASSWORD = "123456"
```

如需更安全的做法，后续可再改为环境变量读取。

## 6. 验证连接

可先验证 `_system` 数据库是否可用：

```bash
/home/liuxuan/workplace/.venv/bin/python - <<'PY'
from arango import ArangoClient
from experiments.cross_db_graph import config

client = ArangoClient(hosts=config.ARANGODB_URL)
sys_db = client.db('_system', username=config.ARANGODB_USERNAME, password=config.ARANGODB_PASSWORD)
print('HAS_DB_CALL', sys_db.has_database(config.ARANGODB_DB))
PY
```

说明：

- 若输出 `HAS_DB_CALL False`，表示连接成功，但目标数据库 `graph_bench` 还未创建。
- 这属于正常现象，导入脚本会自动创建该数据库。
- 若出现 `HTTP 401`，说明用户名或密码不正确。
- 若出现连接拒绝，说明 ArangoDB 服务未正常启动。

## 7. 导入图数据

使用以下命令将社区图 TSV 导入 `ArangoDB`：

```bash
/home/liuxuan/workplace/.venv/bin/python -m experiments.cross_db_graph.scripts.import_arangodb \
  /data/dataset/graph_data/cluster/synthetic_community_100000.tsv
```

该脚本会自动：

1. 连接 `_system` 数据库
2. 创建目标数据库 `graph_bench`（若不存在）
3. 创建顶点集合 `graph_nodes`
4. 创建边集合 `graph_edges`
5. 创建图 `graph_bench_graph`
6. 创建基础索引
7. 清空旧数据并重新导入节点和边

## 8. 运行 benchmark

导入成功后，执行：

```bash
/home/liuxuan/workplace/.venv/bin/python -m experiments.cross_db_graph.runner --engine arangodb
```

结果会输出到：

- `experiments/cross_db_graph/results/<run_id>/`

## 9. 当前已验证结果

本机已成功完成：

- 安装 `arangodb3`
- 启动 `ArangoDB` 服务
- 监听 `127.0.0.1:8529`
- 用更新后的 root 密码通过 Python 客户端连接 `_system`
- 导入 `synthetic_community_100000.tsv` 到 `graph_bench`
- 成功运行：

```bash
/home/liuxuan/workplace/.venv/bin/python -m experiments.cross_db_graph.runner --engine arangodb
```

一次成功结果目录为：

- `experiments/cross_db_graph/results/20260328_160550/`

## 10. 注意事项

- 当前安装方式依赖 `trusted=yes` 仓库配置，仅适合本机实验用途。
- 官方仓库 key 过期时，`apt-get update` 可能仍会输出 GPG 警告。
- 当前 `import_arangodb.py` 使用的索引 API 会出现 deprecation warning，但不影响功能。
- 若后续修改了 root 密码，需要同步更新 `experiments/cross_db_graph/config.py`。
