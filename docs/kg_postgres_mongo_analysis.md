# PostgreSQL 与 MongoDB 统一存储实现分析

> 文件路径:  
> - `lightrag/kg/postgres_impl.py` — **5758 行**  
> - `lightrag/kg/mongo_impl.py` — **2505 行**  
> 核心特点: 两者都在**单一数据库**中实现了全部 4 种存储类型（KV / 向量 / 图 / 文档状态）

---

## 一、对比概览

### 1.1 类对照表

| 存储类型 | 抽象基类 | PostgreSQL 实现 | MongoDB 实现 |
|---------|---------|----------------|-------------|
| **KV 存储** | `BaseKVStorage` | `PGKVStorage` | `MongoKVStorage` |
| **向量存储** | `BaseVectorStorage` | `PGVectorStorage` | `MongoVectorDBStorage` |
| **图存储** | `BaseGraphStorage` | `PGGraphStorage` | `MongoGraphStorage` |
| **文档状态** | `DocStatusStorage` | `PGDocStatusStorage` | `MongoDocStatusStorage` |
| **连接管理** | — | `ClientManager` + `PostgreSQLDB` | `ClientManager` |

### 1.2 技术栈对比

| 方面 | PostgreSQL | MongoDB |
|------|-----------|---------|
| **驱动** | `asyncpg` | `pymongo` (AsyncMongoClient) |
| **向量扩展** | `pgvector` | Atlas Vector Search |
| **图引擎** | Apache AGE (Cypher) | `$graphLookup` 聚合 |
| **连接管理** | 连接池 (`asyncpg.Pool`) | 单客户端共享 |
| **数据隔离** | `workspace` 字段（行级） + AGE 图名 | Collection 前缀命名 |
| **必需环境变量** | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DATABASE` | `MONGO_URI`, `MONGO_DATABASE` |
| **代码量** | 5758 行 | 2505 行 |

---

## 二、PostgreSQL 统一存储实现

### 2.1 架构总览

```
PostgreSQL 数据库
├── 连接层: PostgreSQLDB (连接池 + SSL + 重试)
├── 管理层: ClientManager (单例 + 引用计数)
│
├── PGKVStorage       ← 多张表 (kv_store_{namespace})
├── PGVectorStorage   ← 多张表 (vdb_{type}_{model_suffix})
├── PGGraphStorage    ← Apache AGE 图 (Cypher 查询)
└── PGDocStatusStorage ← doc_status 表
```

### 2.2 `PostgreSQLDB` — 数据库底层封装

**行号**: 128-1695

**职责**: 连接池管理、表创建、数据迁移、查询执行

#### 核心配置

```python
class PostgreSQLDB:
    host, port, user, password, database  # 基础连接
    workspace                              # 数据隔离
    max_connections                        # 连接池大小（默认 50）
    ssl_mode, ssl_cert, ssl_key, ...      # SSL 配置
    vector_index_type                      # 向量索引类型: HNSW / IVFFlat / VChordRQ
    hnsw_m, hnsw_ef                       # HNSW 参数
    connection_retry_attempts              # 重试次数（最多 100）
    connection_retry_backoff               # 重试退避（最长 5 分钟）
```

#### 连接池与重试机制

```python
async def initdb(self):
    # 创建 asyncpg 连接池
    self.pool = await asyncpg.create_pool(
        user=self.user, password=self.password, database=self.database,
        host=self.host, port=self.port,
        min_size=1, max_size=self.max,
        ssl=ssl_context,
        init=register_vector,  # 注册 pgvector 类型
    )

async def _run_with_retry(self, func, ...):
    # 使用 tenacity 实现指数退避重试
    # 自动重置连接池（_reset_pool）
    # 支持暂态异常自动恢复
```

#### 表结构管理（`check_tables()`）

PostgreSQL 实现在 `check_tables()` 中创建约 15 张表：

**KV 存储表**:
- `kv_store_full_docs` — 文档全文
- `kv_store_text_chunks` — 文本块
- `kv_store_llm_response_cache` — LLM 缓存
- `kv_store_full_entities` — 实体完整信息
- `kv_store_full_relations` — 关系完整信息
- `kv_store_entity_chunks` — 实体-chunk 追踪
- `kv_store_relation_chunks` — 关系-chunk 追踪

**向量存储表**（表名带模型后缀）:
- `vdb_entities_{model_suffix}` — 实体向量
- `vdb_relationships_{model_suffix}` — 关系向量
- `vdb_chunks_{model_suffix}` — chunk 向量

**文档状态表**:
- `doc_status` — 文档处理状态

**图存储**:
- Apache AGE 图（Cypher 图数据库扩展，不是普通表）

#### 数据迁移系统

PostgreSQL 实现包含丰富的自动迁移机制（~600 行）：

| 迁移方法 | 功能 |
|---------|------|
| `_migrate_llm_cache_schema()` | LLM 缓存 schema 迁移 |
| `_migrate_timestamp_columns()` | 添加时间戳列 |
| `_migrate_doc_chunks_to_vdb_chunks()` | 文档 chunk 迁移到向量表 |
| `_migrate_llm_cache_to_flattened_keys()` | LLM 缓存键扁平化 |
| `_migrate_doc_status_add_chunks_list()` | 添加 chunks_list 列 |
| `_migrate_doc_status_add_track_id()` | 添加 track_id 列 |
| `_migrate_doc_status_add_metadata_error_msg()` | 添加 metadata/error_msg 列 |
| `_migrate_field_lengths()` | 字段长度扩展 |
| `_migrate_create_full_entities_relations_tables()` | 创建实体/关系追踪表 |

### 2.3 `ClientManager` — 连接管理器

**行号**: 1696-1868

**模式**: 单例 + 引用计数

```python
class ClientManager:
    _instances: dict = {"db": None, "ref_count": 0}
    _lock = asyncio.Lock()

    @classmethod
    async def get_client(cls) -> PostgreSQLDB:
        # 首次调用：创建 PostgreSQLDB 实例 + initdb + check_tables
        # 后续调用：返回已有实例，ref_count + 1

    @classmethod
    async def release_client(cls, db: PostgreSQLDB):
        # ref_count - 1
        # 当 ref_count == 0 时，关闭连接池
```

**配置来源**（优先级从高到低）:
1. 环境变量（`POSTGRES_HOST`, `POSTGRES_USER`, ...）
2. `config.ini` 文件（`[postgres]` 段）
3. 默认值

### 2.4 `PGKVStorage` — KV 存储实现

**行号**: 1872-2360

**数据隔离**: 通过 `workspace` 字段（同一张表，行级隔离）

```sql
-- 表结构示例 (kv_store_full_docs)
CREATE TABLE IF NOT EXISTS kv_store_full_docs (
    workspace VARCHAR(1024) NOT NULL DEFAULT '',
    id        VARCHAR(1024) NOT NULL,
    data      JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace, id)
);
```

**核心操作**:

| 方法 | SQL 操作 |
|------|---------|
| `get_by_id()` | `SELECT data FROM ... WHERE workspace=$1 AND id=$2` |
| `get_by_ids()` | `SELECT id, data FROM ... WHERE workspace=$1 AND id = ANY($2::text[])` |
| `filter_keys()` | `SELECT id FROM ... WHERE workspace=$1 AND id = ANY($2::text[])` → 返回不存在的 |
| `upsert()` | `INSERT ... ON CONFLICT (workspace, id) DO UPDATE SET data=EXCLUDED.data` |
| `delete()` | `DELETE FROM ... WHERE workspace=$1 AND id = ANY($2::text[])` |

**特点**:
- 使用 `JSONB` 存储灵活的 KV 数据
- 支持 `ON CONFLICT ... DO UPDATE`（upsert）
- 针对不同 namespace 自动识别表名

### 2.5 `PGVectorStorage` — 向量存储实现

**行号**: 2361-3214

**数据隔离**: 通过 `workspace` 字段 + **模型后缀**（表名隔离）

```sql
-- 表结构示例 (vdb_entities_{model_suffix})
CREATE TABLE IF NOT EXISTS vdb_entities_text_embedding_3_large_3072d (
    workspace   VARCHAR(1024) NOT NULL DEFAULT '',
    id          VARCHAR(1024) NOT NULL,
    content     TEXT,
    vector      VECTOR(3072),  -- pgvector 向量列
    -- 元数据字段 ...
    entity_name VARCHAR(2048),
    source_id   TEXT,
    file_path   TEXT,
    created_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (workspace, id)
);
```

**向量索引支持**:

| 索引类型 | 适用场景 |
|---------|---------|
| **HNSW** (默认) | 通用场景，高召回率 |
| **IVFFlat** | 大数据集，更快构建 |
| **VChordRQ** | 高维向量，资源受限 |

**核心操作**:

| 方法 | 说明 |
|------|------|
| `upsert()` | 根据 namespace 类型分发到 `_upsert_chunks()` / `_upsert_entities()` / `_upsert_relationships()` |
| `query()` | 使用 `<=>` 运算符计算余弦距离，按相似度排序 |
| `delete()` | 按 workspace + id 删除 |
| `get_vectors_by_ids()` | 仅返回 id 和 vector 数据 |

**模型后缀机制**:
- 表名格式: `vdb_entities_{model_name}_{dim}d`
- 支持同一数据库中使用不同 embedding 模型
- 自动从旧表迁移数据到新表

### 2.6 `PGGraphStorage` — 图存储实现

**行号**: 3865-5758

**图引擎**: Apache AGE（PostgreSQL 的图数据库扩展）

**查询语言**: Cypher

**数据隔离**: 通过不同的图名（`{workspace}_{namespace}`）

```python
def _get_workspace_graph_name(self) -> str:
    if workspace and workspace != "default":
        return f"{safe_workspace}_{safe_namespace}"  # 如 "mywork_chunk_entity_relation"
    else:
        return safe_namespace  # 如 "chunk_entity_relation"
```

**节点文档示例**:
```cypher
CREATE (n:entity {
    entity_id: "CompanyA",
    entity_type: "Organization",
    description: "A major technology company",
    source_id: "chunk-xxx<SEP>chunk-yyy",
    file_path: "doc.pdf",
    created_at: 1749904575
})
```

**核心操作**:

| 方法 | Cypher 查询 |
|------|------------|
| `has_node()` | `MATCH (n:entity {entity_id: $id}) RETURN count(n)` |
| `has_edge()` | `MATCH (s:entity)-[r:related]->(t:entity) WHERE ...` |
| `get_node()` | `MATCH (n:entity {entity_id: $id}) RETURN properties(n)` |
| `get_edge()` | `MATCH (s)-[r:related]->(t) WHERE ... RETURN properties(r)` |
| `upsert_node()` | `MERGE (n:entity {entity_id: $id}) SET n += $props` |
| `upsert_edge()` | `MERGE (s)-[r:related]->(t) SET r += $props` |
| `node_degree()` | `MATCH (n)-[r]-() WHERE ... RETURN count(r)` |
| `get_knowledge_graph()` | BFS 子图查询（`_bfs_subgraph()`） |

**批量操作优化**:
- `get_nodes_batch()`: 使用 `UNWIND` 批量查询
- `node_degrees_batch()`: 单次查询获取多个节点的度
- `get_edges_batch()`: 批量获取边属性

**BFS 子图查询** (`_bfs_subgraph()`):
- 使用层级遍历（BFS）
- 支持 `max_depth` 和 `max_nodes` 限制
- 返回 `KnowledgeGraph` 对象（节点列表 + 边列表 + 是否截断标志）

### 2.7 `PGDocStatusStorage` — 文档状态存储

**行号**: 3215-3844

```sql
-- 表结构
CREATE TABLE IF NOT EXISTS doc_status (
    workspace       VARCHAR(1024) NOT NULL DEFAULT '',
    id              VARCHAR(1024) NOT NULL,
    content_summary TEXT,
    content_length  INTEGER,
    file_path       TEXT,
    status          VARCHAR(64),
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    chunks_count    INTEGER DEFAULT 0,
    chunks_list     TEXT,
    track_id        TEXT,
    error_msg       TEXT,
    metadata        JSONB DEFAULT '{}',
    PRIMARY KEY (workspace, id)
);
```

**特有功能**:

| 方法 | 说明 |
|------|------|
| `get_docs_paginated()` | 分页查询，支持排序和状态过滤 |
| `get_status_counts()` | `GROUP BY status` 获取各状态数量 |
| `get_docs_by_track_id()` | 按追踪 ID 查询 |
| `get_doc_by_file_path()` | 按文件路径查询 |

---

## 三、MongoDB 统一存储实现

### 3.1 架构总览

```
MongoDB 数据库
├── 连接层: AsyncMongoClient (pymongo)
├── 管理层: ClientManager (单例 + 引用计数)
│
├── MongoKVStorage         ← Collection: {workspace}_{namespace}
├── MongoVectorDBStorage   ← Collection: {workspace}_{namespace} + Atlas Vector Search
├── MongoGraphStorage      ← Collection: {workspace}_{namespace} + _edges
└── MongoDocStatusStorage  ← Collection: {workspace}_doc_status
```

### 3.2 `ClientManager` — 连接管理器

**行号**: 42-77

**模式**: 单例 + 引用计数（与 PG 类似但更简洁）

```python
class ClientManager:
    @classmethod
    async def get_client(cls) -> AsyncMongoClient:
        # 从 MONGO_URI + MONGO_DATABASE 创建客户端
        uri = os.environ.get("MONGO_URI", "mongodb://root:root@localhost:27017/")
        database_name = os.environ.get("MONGO_DATABASE", "LightRAG")
        client = AsyncMongoClient(uri)
        db = client.get_database(database_name)
```

**特点**:
- 配置更简单（只需 URI + 数据库名）
- 无连接池管理（pymongo 内部管理）
- 无 SSL 配置（通过 URI 参数）

### 3.3 数据隔离策略

MongoDB 使用 **Collection 名称前缀**实现工作空间隔离：

```python
# 在 __post_init__ 中
if effective_workspace:
    self.final_namespace = f"{effective_workspace}_{self.namespace}"
    # 例: "mywork_full_docs", "mywork_entities"
else:
    self.final_namespace = self.namespace
    # 例: "full_docs", "entities"

self._collection_name = self.final_namespace
```

**优先级**:
1. `MONGODB_WORKSPACE` 环境变量（最高）
2. 构造函数传入的 `workspace` 参数
3. 空字符串（无前缀）

### 3.4 `MongoKVStorage` — KV 存储实现

**行号**: 81-284

**Collection**: `{workspace}_{namespace}`（如 `mywork_full_docs`）

**文档结构**:
```json
{
    "_id": "doc-xxxx",
    "content": "文档全文...",
    "file_path": "/path/to/file.pdf",
    "created_at": "2025-02-10T12:00:00Z",
    "updated_at": "2025-02-10T12:00:00Z"
}
```

**核心操作**:

| 方法 | MongoDB 操作 |
|------|-------------|
| `get_by_id()` | `find_one({"_id": id})` |
| `get_by_ids()` | `find({"_id": {"$in": ids}})` |
| `filter_keys()` | `find({"_id": {"$in": list(keys)}}, {"_id": 1})` → 返回不存在的 |
| `upsert()` | `bulk_write([UpdateOne({"_id": k}, {"$set": v}, upsert=True)])` |
| `delete()` | `delete_many({"_id": {"$in": ids}})` |

**特点**:
- 使用 MongoDB 的 `_id` 字段作为主键
- 使用 `bulk_write` 批量操作
- JSON 文档原生存储，无需 `JSONB` 转换

### 3.5 `MongoVectorDBStorage` — 向量存储实现

**行号**: 2040-2505

**Collection**: `{workspace}_{namespace}`（如 `mywork_entities`）

**向量索引**: Atlas Vector Search

**文档结构**:
```json
{
    "_id": "ent-xxxx",
    "vector": [0.1, 0.2, ...],         // 向量嵌入
    "content": "实体描述...",
    "entity_name": "CompanyA",
    "source_id": "chunk-xxx",
    "file_path": "doc.pdf",
    "created_at": 1749904575
}
```

**向量索引创建**:
```python
async def create_vector_index_if_not_exists(self):
    search_index_model = SearchIndexModel(
        definition={
            "fields": [
                {
                    "type": "vector",
                    "numDimensions": self.embedding_func.embedding_dim,
                    "path": "vector",
                    "similarity": "cosine",
                }
            ]
        },
        name=self._index_name,
        type="vectorSearch",
    )
    await self._data.create_search_index(model=search_index_model)
```

**核心操作**:

| 方法 | MongoDB 操作 |
|------|-------------|
| `upsert()` | 计算向量 → `bulk_write([UpdateOne(upsert=True)])` |
| `query()` | `$vectorSearch` 聚合管道 |
| `delete()` | `delete_many({"_id": {"$in": ids}})` |

**向量查询**:
```python
pipeline = [
    {
        "$vectorSearch": {
            "index": self._index_name,
            "path": "vector",
            "queryVector": embedding,
            "numCandidates": top_k * 10,
            "limit": top_k,
        }
    },
    {"$addFields": {"score": {"$meta": "vectorSearchScore"}}},
    {"$match": {"score": {"$gte": 1 - self.cosine_better_than_threshold}}},
]
```

**注意**: MongoDB 的 `$vectorSearch` 返回的是相似度分数（越高越好），而 pgvector 返回的是距离（越低越好）。

### 3.6 `MongoGraphStorage` — 图存储实现

**行号**: 728-2039

**模式**: 两个 Collection（节点 + 边）

```python
self._collection_name = self.final_namespace              # 节点 collection
self._edge_collection_name = f"{self._collection_name}_edges"  # 边 collection
```

**节点文档**:
```json
{
    "_id": "CompanyA",
    "entity_id": "CompanyA",
    "entity_type": "Organization",
    "description": "A major technology company",
    "source_id": "chunk-xxx",
    "source_ids": ["chunk-xxx"],
    "file_path": "custom_kg",
    "created_at": 1749904575
}
```

**边文档**:
```json
{
    "_id": ObjectId("..."),
    "source_node_id": "CompanyA",
    "target_node_id": "ProductX",
    "relationship": "Develops",
    "description": "CompanyA develops ProductX",
    "weight": 1.0,
    "keywords": "develop, produce",
    "source_id": "chunk-xxx",
    "source_ids": ["chunk-xxx"],
    "file_path": "custom_kg",
    "created_at": 1749904575
}
```

**核心操作**:

| 方法 | MongoDB 操作 |
|------|-------------|
| `has_node()` | `collection.find_one({"_id": node_id})` |
| `has_edge()` | `edge_collection.find_one({$or: [{src, tgt}, {tgt, src}]})` (无向) |
| `get_node()` | `collection.find_one({"_id": node_id})` |
| `get_edge()` | `edge_collection.find_one({$or: [{src, tgt}, {tgt, src}]})` |
| `upsert_node()` | `update_one({"_id": node_id}, {"$set": data}, upsert=True)` |
| `upsert_edge()` | `update_one({src, tgt}, {"$set": data}, upsert=True)` |
| `node_degree()` | `count_documents({$or: [{src: id}, {tgt: id}]})` |
| `get_node_edges()` | `find({$or: [{src: id}, {tgt: id}]})` |

**BFS 子图查询**（支持两种模式）:

| 模式 | 环境变量 | 说明 |
|------|---------|------|
| `bidirectional` (默认) | `MONGO_GRAPH_BFS_MODE=bidirectional` | 双向 BFS，从起始节点同时向两个方向扩展 |
| `in_out_bound` | `MONGO_GRAPH_BFS_MODE=in_out_bound` | 使用 `$graphLookup` 分别查找入边和出边 |

**搜索标签**（多种策略）:
1. Atlas Text Search（全文搜索）
2. Atlas Autocomplete Search（自动补全）
3. Atlas Compound Search（复合搜索）
4. Fallback: 正则表达式搜索

### 3.7 `MongoDocStatusStorage` — 文档状态存储

**行号**: 285-727

**Collection**: `{workspace}_doc_status`

**文档结构**:
```json
{
    "_id": "doc-xxxx",
    "content_summary": "文档前100字...",
    "content_length": 5000,
    "file_path": "/path/to/file.pdf",
    "status": "processed",
    "created_at": "2025-02-10T12:00:00Z",
    "updated_at": "2025-02-10T12:00:00Z",
    "track_id": "insert-xxx",
    "chunks_count": 10,
    "chunks_list": ["chunk-1", "chunk-2"],
    "error_msg": null,
    "metadata": {}
}
```

**索引管理** (`create_and_migrate_indexes_if_not_exists()`):
- `status` 索引 — 按状态查询
- `track_id` 索引 — 按追踪 ID 查询
- `file_path` 索引 — 按文件路径查询
- `(status, updated_at)` 复合索引 — 分页查询
- `(status, created_at)` 复合索引 — 分页查询

---

## 四、核心设计对比

### 4.1 数据隔离策略

| 方面 | PostgreSQL | MongoDB |
|------|-----------|---------|
| **KV 存储** | 同一张表，`workspace` 列区分 | 不同 Collection（前缀命名） |
| **向量存储** | 同一张表，`workspace` 列区分 + 模型后缀 | 不同 Collection（前缀命名） |
| **图存储** | 不同 AGE 图（图名包含 workspace） | 不同 Collection（前缀命名） |
| **文档状态** | 同一张表，`workspace` 列区分 | 不同 Collection（前缀命名） |

**PG 方式**: 行级隔离，通过 `WHERE workspace=$1` 过滤  
**Mongo 方式**: Collection 级隔离，通过 `{workspace}_{namespace}` 命名

### 4.2 向量检索实现

| 方面 | PostgreSQL (pgvector) | MongoDB (Atlas Vector Search) |
|------|----------------------|-------------------------------|
| **索引类型** | HNSW / IVFFlat / VChordRQ | Atlas Vector Search |
| **相似度度量** | 余弦距离 (`<=>` 运算符) | 余弦相似度 (`$vectorSearch`) |
| **过滤方式** | `WHERE 1 - (vec <=> query_vec) >= threshold` | `$match: {score: {$gte: threshold}}` |
| **索引创建** | `CREATE INDEX ... USING hnsw (vector ...)` | `create_search_index(SearchIndexModel)` |
| **维度校验** | 表创建时指定 `VECTOR(dim)` | 运行时校验，不匹配报错 |

### 4.3 图查询实现

| 方面 | PostgreSQL (AGE) | MongoDB ($graphLookup) |
|------|-----------------|----------------------|
| **查询语言** | Cypher | MongoDB 聚合管道 |
| **无向图** | 查询时 `MATCH (s)-[r]-(t)` 或双向 UNION | `$or: [{src, tgt}, {tgt, src}]` |
| **BFS 遍历** | Cypher 路径查询 | `$graphLookup` / 手动 BFS |
| **批量操作** | `UNWIND` 批量查询 | 循环查询（Python 层面） |
| **标签搜索** | `MATCH (n) WHERE n.entity_id =~ $pattern` | Atlas Search / 正则回退 |

### 4.4 连接管理对比

| 方面 | PostgreSQL | MongoDB |
|------|-----------|---------|
| **连接池** | `asyncpg.Pool` (显式管理) | pymongo 内部管理 |
| **最大连接数** | 可配置（默认 50） | pymongo 自动管理 |
| **SSL 支持** | 丰富配置（6 个参数） | 通过 URI 参数 |
| **重试机制** | tenacity + 指数退避（100 次/5 分钟） | 无显式重试 |
| **连接重置** | `_reset_pool()` 自动重建 | 无 |

### 4.5 迁移系统对比

| 方面 | PostgreSQL | MongoDB |
|------|-----------|---------|
| **自动迁移** | 10+ 个迁移方法 | 索引迁移 |
| **Schema 变更** | `ALTER TABLE ADD COLUMN` | 无需（灵活 schema） |
| **数据迁移** | 批量 INSERT + 删除旧数据 | 无需 |
| **向后兼容** | 旧表数据自动迁移到新表 | Collection 名称兼容 |

---

## 五、环境变量配置

### 5.1 PostgreSQL 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `POSTGRES_HOST` | `localhost` | 主机地址 |
| `POSTGRES_PORT` | `5432` | 端口 |
| `POSTGRES_USER` | `postgres` | 用户名（**必需**） |
| `POSTGRES_PASSWORD` | — | 密码（**必需**） |
| `POSTGRES_DATABASE` | `postgres` | 数据库名（**必需**） |
| `POSTGRES_WORKSPACE` | — | 工作空间（覆盖 LightRAG workspace） |
| `POSTGRES_MAX_CONNECTIONS` | `50` | 最大连接数 |
| `POSTGRES_SSL_MODE` | — | SSL 模式 |
| `POSTGRES_VECTOR_INDEX_TYPE` | `HNSW` | 向量索引类型 |
| `POSTGRES_HNSW_M` | `16` | HNSW M 参数 |
| `POSTGRES_HNSW_EF` | `64` | HNSW ef_construction |
| `POSTGRES_CONNECTION_RETRIES` | `10` | 连接重试次数 |
| `POSTGRES_CONNECTION_RETRY_BACKOFF` | `3.0` | 重试退避基数（秒） |

### 5.2 MongoDB 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MONGO_URI` | `mongodb://root:root@localhost:27017/` | 连接 URI（**必需**） |
| `MONGO_DATABASE` | `LightRAG` | 数据库名（**必需**） |
| `MONGODB_WORKSPACE` | — | 工作空间（覆盖 LightRAG workspace） |
| `MONGO_GRAPH_BFS_MODE` | `bidirectional` | 图 BFS 模式 |

---

## 六、使用建议

### 6.1 PostgreSQL 全家桶

```python
rag = LightRAG(
    kv_storage="PGKVStorage",
    vector_storage="PGVectorStorage",
    graph_storage="PGGraphStorage",
    doc_status_storage="PGDocStatusStorage",
    workspace="my_project",
)
```

**优势**: 单一数据库、事务支持、成熟生态、丰富迁移  
**劣势**: 需要 AGE 扩展、配置较复杂、图查询性能一般

### 6.2 MongoDB 全家桶

```python
rag = LightRAG(
    kv_storage="MongoKVStorage",
    vector_storage="MongoVectorDBStorage",
    graph_storage="MongoGraphStorage",
    doc_status_storage="MongoDocStatusStorage",
    workspace="my_project",
)
```

**优势**: 灵活 Schema、配置简单、水平扩展、Atlas 云服务  
**劣势**: 需要 Atlas Vector Search、图查询能力弱于专用图数据库

### 6.3 选型参考

| 场景 | 推荐 | 原因 |
|------|------|------|
| 已有 PostgreSQL | PG 全家桶 | 统一运维，减少组件 |
| 已有 MongoDB | Mongo 全家桶 | 统一运维，减少组件 |
| 企业级生产 | PG 全家桶 | 事务支持，成熟稳定 |
| 云原生 | Mongo 全家桶 | Atlas 云服务，弹性扩展 |
| 大规模图查询 | PG(KV+Doc) + Neo4j(图) | 专用图数据库性能更优 |
| 大规模向量 | PG(KV+Doc+图) + Milvus(向量) | 专用向量数据库性能更优 |

---

## 七、总结

PostgreSQL 和 MongoDB 的统一存储实现是 LightRAG 框架的**两套完整的单数据库解决方案**：

1. **PostgreSQL**: 依托 `pgvector` + `Apache AGE` 扩展，在关系数据库上实现了向量检索和图查询，配合丰富的自动迁移系统，适合追求**数据一致性和成熟运维**的场景

2. **MongoDB**: 利用 Atlas Vector Search + `$graphLookup`，在文档数据库上实现了向量检索和图遍历，配合灵活的 Schema 设计，适合追求**开发效率和弹性扩展**的场景

两者都遵循相同的抽象接口（`BaseKVStorage` / `BaseVectorStorage` / `BaseGraphStorage` / `DocStatusStorage`），可以无缝切换，体现了 LightRAG 框架**插件式存储架构**的设计优势。
