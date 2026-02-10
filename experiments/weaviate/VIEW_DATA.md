# Weaviate 容器数据查看指南

## 📍 数据存储位置

Weaviate 的数据存储在容器内的 `/data` 目录中，包含以下内容：

- **schema.db**: Schema 定义数据库
- **modules.db**: 模块配置数据库
- **classifications.db**: 分类数据
- **author/**: Author 集合的数据目录
- **document/**: Document 集合的数据目录
- **raft/**: Raft 共识算法的数据

## 🔍 查看数据的方法

### 方法 1: 使用 Shell 脚本（推荐）

运行提供的脚本快速查看：

```bash
cd /home/liuxuan/LanceTest/Lance/hybrid_search/test_weaviate
bash view_container_data.sh
```

这个脚本会显示：
- 数据目录结构
- 各集合的数据目录
- 数据库文件大小
- 数据目录总大小
- Schema 信息

### 方法 2: 使用 Python 脚本（通过 API）

通过 Weaviate API 查询数据：

```bash
cd /home/liuxuan/LanceTest/Lance/hybrid_search/test_weaviate
python query_weaviate_data.py
```

这个脚本会显示：
- Schema 信息
- 各集合的对象数量
- 示例对象内容

### 方法 3: 直接使用 Docker 命令

#### 3.1 查看数据目录结构

```bash
# 查看 /data 目录内容
docker exec test_weaviate-weaviate-1 ls -lah /data

# 查看 Author 集合数据
docker exec test_weaviate-weaviate-1 ls -lah /data/author

# 查看 Document 集合数据
docker exec test_weaviate-weaviate-1 ls -lah /data/document
```

#### 3.2 查看数据大小

```bash
# 查看数据目录总大小
docker exec test_weaviate-weaviate-1 du -sh /data

# 查看各子目录大小
docker exec test_weaviate-weaviate-1 du -sh /data/*
```

#### 3.3 查找特定文件

```bash
# 查找所有数据库文件
docker exec test_weaviate-weaviate-1 find /data -name "*.db"

# 查找所有数据文件
docker exec test_weaviate-weaviate-1 find /data -type f
```

### 方法 4: 进入容器交互式查看

进入容器内部进行交互式查看：

```bash
# 进入容器
docker exec -it test_weaviate-weaviate-1 sh

# 在容器内执行命令
cd /data
ls -lah
du -sh *
cat schema.db  # 注意：这是二进制文件，不能直接查看
```

### 方法 5: 复制数据到主机

将容器内的数据复制到主机进行查看：

```bash
# 复制整个数据目录
docker cp test_weaviate-weaviate-1:/data ./weaviate-data

# 复制特定文件
docker cp test_weaviate-weaviate-1:/data/schema.db ./

# 复制后查看
ls -lah ./weaviate-data
```

### 方法 6: 使用 Weaviate REST API

#### 6.1 查看 Schema

```bash
curl http://localhost:8080/v1/schema | python3 -m json.tool
```

#### 6.2 查询对象数量

```bash
# 使用 GraphQL 查询 Author 数量
curl -X POST http://localhost:8080/v1/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ Aggregate { Author { meta { count } } } }"
  }' | python3 -m json.tool

# 查询 Document 数量
curl -X POST http://localhost:8080/v1/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ Aggregate { Document { meta { count } } } }"
  }' | python3 -m json.tool
```

#### 6.3 获取示例对象

```bash
# 获取前 5 个 Author 对象
curl -X POST http://localhost:8080/v1/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ Get { Author(limit: 5) { name department } } }"
  }' | python3 -m json.tool

# 获取前 5 个 Document 对象
curl -X POST http://localhost:8080/v1/graphql \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ Get { Document(limit: 5) { title content category publish_year } } }"
  }' | python3 -m json.tool
```

### 方法 7: 使用 Weaviate 客户端库

如果已安装 `weaviate-client`，可以使用 Python 客户端：

```python
import weaviate

client = weaviate.connect_to_local(
    host="localhost",
    port=8080,
    grpc_port=50052
)

try:
    # 获取 Schema
    schema = client.collections.list_all()
    print(schema)
    
    # 获取 Author 集合
    author_col = client.collections.get("Author")
    print(f"Author 数量: {author_col.query.fetch_objects(limit=1).total}")
    
    # 获取 Document 集合
    doc_col = client.collections.get("Document")
    print(f"Document 数量: {doc_col.query.fetch_objects(limit=1).total}")
    
finally:
    client.close()
```

## 📊 数据持久化

**重要提示**：当前配置中，数据只存储在容器内部。如果容器被删除，数据会丢失。

如果需要数据持久化，可以在 `docker-compose.yml` 中添加 volumes：

```yaml
services:
  weaviate:
    # ... 其他配置 ...
    volumes:
      - ./weaviate-data:/data
```

这样数据会保存在主机的 `./weaviate-data` 目录中。

## 🔧 常用命令速查

```bash
# 查看容器状态
docker compose ps

# 查看容器日志
docker compose logs weaviate

# 查看数据目录
docker exec test_weaviate-weaviate-1 ls -lah /data

# 查看数据大小
docker exec test_weaviate-weaviate-1 du -sh /data

# 复制数据到主机
docker cp test_weaviate-weaviate-1:/data ./weaviate-data

# 通过 API 查看 Schema
curl http://localhost:8080/v1/schema | python3 -m json.tool
```

## ⚠️ 注意事项

1. **数据库文件是二进制的**：`.db` 文件是 SQLite 数据库，不能直接用文本编辑器查看
2. **数据目录结构**：每个集合都有自己的子目录，包含 LSM 树和向量索引文件
3. **数据备份**：定期使用 `docker cp` 备份数据，或配置 volumes 持久化
4. **权限问题**：容器内数据属于 root 用户，复制到主机后可能需要调整权限

