# `base.py` 抽象基类与数据结构详细分析

> 文件路径: `lightrag/base.py`  
> 总行数: **908 行**  
> 核心职责: 定义所有存储接口的抽象基类和核心数据结构

---

## 一、文件概览

`base.py` 是 LightRAG 框架的**抽象层核心**，定义了：

1. **4 大存储抽象基类** - 定义存储接口契约
2. **核心数据结构** - 查询参数、结果、文档状态等
3. **枚举类型** - 文档状态、存储状态
4. **辅助类** - Ollama 服务器信息、文本块模式等

---

## 二、类与数据结构分类

### 2.1 存储抽象基类（4 个）

| 类 | 行号 | 继承关系 | 用途 |
|------|------|---------|------|
| `StorageNameSpace` | 172-214 | `ABC` | 所有存储的根基类（命名空间管理） |
| `BaseVectorStorage` | 217-353 | `StorageNameSpace, ABC` | 向量存储抽象基类 |
| `BaseKVStorage` | 355-402 | `StorageNameSpace, ABC` | 键值存储抽象基类 |
| `BaseGraphStorage` | 404-703 | `StorageNameSpace, ABC` | 图存储抽象基类 |
| `DocStatusStorage` | 762-823 | `BaseKVStorage, ABC` | 文档状态存储抽象基类 |

### 2.2 数据结构类（6 个）

| 类 | 行号 | 类型 | 用途 |
|------|------|------|------|
| `QueryParam` | 84-170 | `@dataclass` | 查询参数配置 |
| `QueryResult` | 848-889 | `@dataclass` | 统一查询结果 |
| `QueryContextResult` | 891-908 | `@dataclass` | 查询上下文结果 |
| `DocProcessingStatus` | 715-760 | `@dataclass` | 文档处理状态 |
| `DeletionResult` | 834-843 | `@dataclass` | 删除操作结果 |
| `TextChunkSchema` | 74-79 | `TypedDict` | 文本块数据结构 |

### 2.3 枚举类型（2 个）

| 枚举 | 行号 | 用途 |
|------|------|------|
| `DocStatus` | 705-713 | 文档处理状态枚举 |
| `StoragesStatus` | 825-832 | 存储状态枚举 |

### 2.4 辅助类（1 个）

| 类 | 行号 | 用途 |
|------|------|------|
| `OllamaServerInfos` | 41-72 | Ollama 服务器信息配置 |

---

## 三、存储抽象基类详解

### 3.1 `StorageNameSpace` - 存储命名空间基类

**继承**: `ABC`（抽象基类）

**作用**: 所有存储类的根基，提供命名空间和工作空间管理

**字段**:
```python
namespace: str          # 存储命名空间（如 "full_docs", "entities"）
workspace: str          # 工作空间（数据隔离）
global_config: dict[str, Any]  # 全局配置字典
```

**方法**:

| 方法 | 类型 | 说明 |
|------|------|------|
| `initialize()` | 异步 | 初始化存储（默认空实现） |
| `finalize()` | 异步 | 清理存储（默认空实现） |
| `index_done_callback()` | 抽象异步 | 索引完成后的回调（持久化） |
| `drop()` | 抽象异步 | 删除所有数据 |

**设计意图**:
- 统一所有存储的生命周期管理
- 支持多工作空间数据隔离
- 提供持久化钩子

---

### 3.2 `BaseVectorStorage` - 向量存储抽象基类

**继承**: `StorageNameSpace, ABC`

**作用**: 定义向量数据库的接口契约

**字段**:
```python
embedding_func: EmbeddingFunc              # Embedding 函数（必需）
cosine_better_than_threshold: float = 0.2  # 余弦相似度阈值
meta_fields: set[str] = set()              # 元数据字段集合
```

**核心方法**:

#### A. 查询方法

| 方法 | 签名 | 说明 |
|------|------|------|
| `query()` | `async def query(query: str, top_k: int, query_embedding: list[float] = None) -> list[dict]` | 向量检索查询 |
| `get_by_id()` | `async def get_by_id(id: str) -> dict \| None` | 按 ID 获取向量数据 |
| `get_by_ids()` | `async def get_by_ids(ids: list[str]) -> list[dict]` | 批量获取向量数据 |
| `get_vectors_by_ids()` | `async def get_vectors_by_ids(ids: list[str]) -> dict[str, list[float]]` | 仅获取向量嵌入（高效） |

#### B. 更新方法

| 方法 | 签名 | 说明 |
|------|------|------|
| `upsert()` | `async def upsert(data: dict[str, dict]) -> None` | 插入或更新向量 |
| `delete()` | `async def delete(ids: list[str]) -> None` | 删除向量 |
| `delete_entity()` | `async def delete_entity(entity_name: str) -> None` | 删除实体的所有向量 |
| `delete_entity_relation()` | `async def delete_entity_relation(entity_name: str) -> None` | 删除实体的关系向量 |

**辅助方法**:

- `_validate_embedding_func()`: 验证 `embedding_func` 不为 None
- `_generate_collection_suffix()`: 根据 embedding 函数生成集合后缀（用于多模型支持）

**特点**:
- 支持预计算 embedding（`query_embedding` 参数）
- 提供批量操作接口
- 支持实体级别的删除操作

---

### 3.3 `BaseKVStorage` - 键值存储抽象基类

**继承**: `StorageNameSpace, ABC`

**作用**: 定义键值存储的接口契约

**字段**:
```python
embedding_func: EmbeddingFunc  # Embedding 函数（某些存储后端需要）
```

**核心方法**:

| 方法 | 签名 | 说明 |
|------|------|------|
| `get_by_id()` | `async def get_by_id(id: str) -> dict \| None` | 按 ID 获取值 |
| `get_by_ids()` | `async def get_by_ids(ids: list[str]) -> list[dict]` | 批量获取值 |
| `filter_keys()` | `async def filter_keys(keys: set[str]) -> set[str]` | 过滤不存在的 key（返回不存在的 key 集合） |
| `upsert()` | `async def upsert(data: dict[str, dict]) -> None` | 插入或更新数据 |
| `delete()` | `async def delete(ids: list[str]) -> None` | 删除数据 |
| `is_empty()` | `async def is_empty() -> bool` | 检查存储是否为空 |

**特点**:
- `filter_keys()` 方法用于去重检查（返回不存在的 key）
- 所有操作都是异步的
- 支持批量操作

**使用场景**:
- 存储文档全文（`full_docs`）
- 存储文本块（`text_chunks`）
- 存储 LLM 缓存（`llm_response_cache`）
- 存储实体/关系完整信息（`full_entities`、`full_relations`）
- 存储 chunk 追踪（`entity_chunks`、`relation_chunks`）

---

### 3.4 `BaseGraphStorage` - 图存储抽象基类

**继承**: `StorageNameSpace, ABC`

**作用**: 定义知识图谱存储的接口契约

**重要说明**: **所有边操作都是无向的**（undirected）

**字段**:
```python
embedding_func: EmbeddingFunc  # Embedding 函数（某些操作需要）
```

**核心方法分类**:

#### A. 节点操作

| 方法 | 签名 | 说明 |
|------|------|------|
| `has_node()` | `async def has_node(node_id: str) -> bool` | 检查节点是否存在 |
| `get_node()` | `async def get_node(node_id: str) -> dict \| None` | 获取节点属性 |
| `get_nodes_batch()` | `async def get_nodes_batch(node_ids: list[str]) -> dict[str, dict]` | 批量获取节点（默认实现：逐个获取） |
| `upsert_node()` | `async def upsert_node(node_id: str, node_data: dict) -> None` | 插入或更新节点 |
| `delete_node()` | `async def delete_node(node_id: str) -> None` | 删除节点 |
| `remove_nodes()` | `async def remove_nodes(nodes: list[str]) -> None` | 批量删除节点 |

#### B. 边操作（无向图）

| 方法 | 签名 | 说明 |
|------|------|------|
| `has_edge()` | `async def has_edge(src_id: str, tgt_id: str) -> bool` | 检查边是否存在 |
| `get_edge()` | `async def get_edge(src_id: str, tgt_id: str) -> dict \| None` | 获取边属性 |
| `get_edges_batch()` | `async def get_edges_batch(pairs: list[dict]) -> dict[tuple, dict]` | 批量获取边 |
| `get_node_edges()` | `async def get_node_edges(node_id: str) -> list[tuple] \| None` | 获取节点的所有边 |
| `get_nodes_edges_batch()` | `async def get_nodes_edges_batch(node_ids: list[str]) -> dict[str, list[tuple]]` | 批量获取节点的边 |
| `upsert_edge()` | `async def upsert_edge(src_id: str, tgt_id: str, edge_data: dict) -> None` | 插入或更新边 |
| `remove_edges()` | `async def remove_edges(edges: list[tuple]) -> None` | 批量删除边 |

#### C. 图查询方法

| 方法 | 签名 | 说明 |
|------|------|------|
| `node_degree()` | `async def node_degree(node_id: str) -> int` | 获取节点的度（连接的边数） |
| `edge_degree()` | `async def edge_degree(src_id: str, tgt_id: str) -> int` | 获取边的总度（源节点度 + 目标节点度） |
| `node_degrees_batch()` | `async def node_degrees_batch(node_ids: list[str]) -> dict[str, int]` | 批量获取节点度 |
| `edge_degrees_batch()` | `async def edge_degrees_batch(edge_pairs: list[tuple]) -> dict[tuple, int]` | 批量获取边度 |
| `get_all_labels()` | `async def get_all_labels() -> list[str]` | 获取所有节点标签（不适用于大图） |
| `get_popular_labels()` | `async def get_popular_labels(limit: int = 300) -> list[str]` | 获取热门标签（按度排序） |
| `search_labels()` | `async def search_labels(query: str, limit: int = 50) -> list[str]` | 模糊搜索标签 |
| `get_knowledge_graph()` | `async def get_knowledge_graph(node_label: str, max_depth: int = 3, max_nodes: int = 1000) -> KnowledgeGraph` | 获取子图（BFS，支持 `*` 获取全图） |
| `get_all_nodes()` | `async def get_all_nodes() -> list[dict]` | 获取所有节点 |
| `get_all_edges()` | `async def get_all_edges() -> list[dict]` | 获取所有边 |

**批量操作默认实现**:
- `get_nodes_batch()`、`get_edges_batch()` 等批量方法提供了默认实现（逐个获取）
- 存储后端可以重写这些方法以提供更高效的批量操作

**特点**:
- **无向图设计**: 所有边操作都是无向的
- **批量操作支持**: 提供批量接口，默认逐个实现，可优化
- **子图查询**: 支持 BFS 子图检索，带深度和节点数限制
- **标签搜索**: 支持热门标签和模糊搜索

---

### 3.5 `DocStatusStorage` - 文档状态存储抽象基类

**继承**: `BaseKVStorage, ABC`

**作用**: 专门用于存储文档处理状态的存储接口

**额外方法**（继承 `BaseKVStorage` 的所有方法）:

| 方法 | 签名 | 说明 |
|------|------|------|
| `get_status_counts()` | `async def get_status_counts() -> dict[str, int]` | 获取各状态的文档数量 |
| `get_docs_by_status()` | `async def get_docs_by_status(status: DocStatus) -> dict[str, DocProcessingStatus]` | 按状态获取文档 |
| `get_docs_by_track_id()` | `async def get_docs_by_track_id(track_id: str) -> dict[str, DocProcessingStatus]` | 按追踪 ID 获取文档 |
| `get_docs_paginated()` | `async def get_docs_paginated(status_filter, page, page_size, sort_field, sort_direction) -> tuple[list, int]` | 分页获取文档 |
| `get_all_status_counts()` | `async def get_all_status_counts() -> dict[str, int]` | 获取所有状态的文档数量 |
| `get_doc_by_file_path()` | `async def get_doc_by_file_path(file_path: str) -> dict \| None` | 按文件路径获取文档 |

**特点**:
- 专门用于文档状态管理
- 支持分页查询
- 支持按状态、追踪 ID、文件路径查询

---

## 四、核心数据结构详解

### 4.1 `QueryParam` - 查询参数配置

**类型**: `@dataclass`

**作用**: 统一管理所有查询相关的参数

**字段详解**:

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `mode` | `Literal["local", "global", "hybrid", "naive", "mix", "bypass"]` | `"mix"` | 查询模式 |
| `only_need_context` | `bool` | `False` | 是否仅返回上下文（不调用 LLM） |
| `only_need_prompt` | `bool` | `False` | 是否仅返回提示词 |
| `response_type` | `str` | `"Multiple Paragraphs"` | 响应格式（段落/列表/单段） |
| `stream` | `bool` | `False` | 是否流式输出 |
| `top_k` | `int` | `40` | 检索的实体/关系数量 |
| `chunk_top_k` | `int` | `20` | 检索的 chunk 数量 |
| `max_entity_tokens` | `int` | `6000` | 实体上下文的最大 token 数 |
| `max_relation_tokens` | `int` | `8000` | 关系上下文的最大 token 数 |
| `max_total_tokens` | `int` | `30000` | 总上下文 token 预算 |
| `hl_keywords` | `list[str]` | `[]` | 高层关键词列表 |
| `ll_keywords` | `list[str]` | `[]` | 低层关键词列表 |
| `conversation_history` | `list[dict]` | `[]` | 对话历史（格式：`[{"role": "user/assistant", "content": "..."}]`） |
| `history_turns` | `int` | `0` | 历史轮数（已废弃） |
| `model_func` | `Callable \| None` | `None` | 可选的 LLM 函数覆盖 |
| `user_prompt` | `str \| None` | `None` | 用户自定义提示词 |
| `enable_rerank` | `bool` | `True` | 是否启用重排序 |
| `include_references` | `bool` | `False` | 是否包含引用列表 |

**使用示例**:
```python
# 创建查询参数
param = QueryParam(
    mode="local",
    top_k=20,
    stream=True,
    hl_keywords=["机器学习", "深度学习"],
    ll_keywords=["神经网络", "CNN"],
)

# 使用查询参数
result = await rag.aquery("什么是深度学习？", param=param)
```

---

### 4.2 `QueryResult` - 统一查询结果

**类型**: `@dataclass`

**作用**: 统一所有查询模式的结果格式

**字段**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `content` | `Optional[str]` | 非流式响应的文本内容 |
| `response_iterator` | `Optional[AsyncIterator[str]]` | 流式响应的迭代器 |
| `raw_data` | `Optional[Dict[str, Any]]` | 完整的结构化数据（包含引用和元数据） |
| `is_streaming` | `bool` | 是否为流式结果 |

**属性**:

| 属性 | 返回类型 | 说明 |
|------|---------|------|
| `reference_list` | `List[Dict[str, str]]` | 从 `raw_data` 提取的引用列表 |
| `metadata` | `Dict[str, Any]` | 从 `raw_data` 提取的元数据 |

**`raw_data` 结构**:
```python
{
    "status": "success" | "failure",
    "message": str,
    "data": {
        "entities": [...],        # 实体列表
        "relationships": [...],    # 关系列表
        "chunks": [...],          # 文本块列表
        "references": [...]       # 引用列表
    },
    "metadata": {
        "query_mode": str,
        "keywords": {...},
        "processing_info": {...}
    }
}
```

**使用示例**:
```python
result = await rag.aquery("什么是深度学习？")

# 非流式响应
if not result.is_streaming:
    print(result.content)

# 流式响应
else:
    async for chunk in result.response_iterator:
        print(chunk, end="")

# 获取引用列表
for ref in result.reference_list:
    print(f"引用: {ref['file_path']}")
```

---

### 4.3 `QueryContextResult` - 查询上下文结果

**类型**: `@dataclass`

**作用**: 仅包含上下文构建的结果（不包含 LLM 响应）

**字段**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `context` | `str` | LLM 上下文字符串 |
| `raw_data` | `Dict[str, Any]` | 完整的结构化数据（与 `QueryResult.raw_data` 相同格式） |

**属性**:

| 属性 | 返回类型 | 说明 |
|------|---------|------|
| `reference_list` | `List[Dict[str, str]]` | 从 `raw_data` 提取的引用列表 |

**使用场景**:
- `only_need_context=True` 时返回此类型
- 用于调试和自定义 LLM 调用

---

### 4.4 `DocProcessingStatus` - 文档处理状态

**类型**: `@dataclass`

**作用**: 表示文档的处理状态和元数据

**字段**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `content_summary` | `str` | 文档内容摘要（前 100 字符） |
| `content_length` | `int` | 文档总长度 |
| `file_path` | `str` | 文档文件路径 |
| `status` | `DocStatus` | 当前处理状态 |
| `created_at` | `str` | 创建时间（ISO 格式） |
| `updated_at` | `str` | 更新时间（ISO 格式） |
| `track_id` | `str \| None` | 追踪 ID（用于监控进度） |
| `chunks_count` | `int \| None` | 分块后的 chunk 数量 |
| `chunks_list` | `list[str] \| None` | chunk ID 列表（用于删除） |
| `error_msg` | `str \| None` | 错误消息（如果失败） |
| `metadata` | `dict[str, Any]` | 额外元数据 |
| `multimodal_processed` | `bool \| None` | 多模态处理完成标志（内部字段，不显示在 repr） |

**`__post_init__` 逻辑**:
- 如果 `multimodal_processed=False` 且 `status=PROCESSED`，则自动将状态改为 `PREPROCESSED`

**状态流转**:
```
PENDING → PROCESSING → PROCESSED
    ↓           ↓
  FAILED    PREPROCESSED (多模态场景)
```

---

### 4.5 `DeletionResult` - 删除操作结果

**类型**: `@dataclass`

**作用**: 表示删除操作的结果

**字段**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `status` | `Literal["success", "not_found", "fail"]` | 操作状态 |
| `doc_id` | `str` | 文档 ID |
| `message` | `str` | 操作消息 |
| `status_code` | `int` | HTTP 状态码（默认 200） |
| `file_path` | `str \| None` | 文件路径（如果可用） |

**使用示例**:
```python
result = await rag.adelete_by_doc_id("doc-xxx")

if result.status == "success":
    print(f"删除成功: {result.message}")
elif result.status == "not_found":
    print(f"文档不存在: {result.doc_id}")
else:
    print(f"删除失败: {result.message}")
```

---

### 4.6 `TextChunkSchema` - 文本块数据结构

**类型**: `TypedDict`

**作用**: 定义文本块的数据结构（类型注解）

**字段**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `tokens` | `int` | chunk 的 token 数 |
| `content` | `str` | chunk 的文本内容 |
| `full_doc_id` | `str` | 所属文档的 ID |
| `chunk_order_index` | `int` | chunk 在文档中的顺序索引 |

**使用场景**:
- 作为 `extract_entities()` 等函数的参数类型注解
- 确保文本块数据格式的一致性

---

## 五、枚举类型详解

### 5.1 `DocStatus` - 文档处理状态枚举

**类型**: `str, Enum`

**值**:

| 值 | 字符串 | 说明 |
|------|--------|------|
| `PENDING` | `"pending"` | 待处理 |
| `PROCESSING` | `"processing"` | 处理中 |
| `PREPROCESSED` | `"preprocessed"` | 预处理完成（多模态场景） |
| `PROCESSED` | `"processed"` | 处理完成 |
| `FAILED` | `"failed"` | 处理失败 |

**状态流转**:
```
PENDING → PROCESSING → PROCESSED
    ↓           ↓
  FAILED    PREPROCESSED
```

---

### 5.2 `StoragesStatus` - 存储状态枚举

**类型**: `str, Enum`

**值**:

| 值 | 字符串 | 说明 |
|------|--------|------|
| `NOT_CREATED` | `"not_created"` | 未创建 |
| `CREATED` | `"created"` | 已创建（在 `__post_init__` 中） |
| `INITIALIZED` | `"initialized"` | 已初始化（调用 `initialize_storages()` 后） |
| `FINALIZED` | `"finalized"` | 已清理（调用 `finalize_storages()` 后） |

**状态流转**:
```
NOT_CREATED → CREATED → INITIALIZED → FINALIZED
```

---

## 六、辅助类详解

### 6.1 `OllamaServerInfos` - Ollama 服务器信息

**作用**: 配置 LightRAG 作为 Ollama 兼容服务器时的模型信息

**字段**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `LIGHTRAG_NAME` | `str` (property) | 模型名称（默认：`"lightrag"`） |
| `LIGHTRAG_TAG` | `str` (property) | 模型标签（默认：`"latest"`） |
| `LIGHTRAG_MODEL` | `str` (property) | 完整模型标识（`"{name}:{tag}"`） |
| `LIGHTRAG_SIZE` | `int` | 模型大小（默认：7365960935） |
| `LIGHTRAG_CREATED_AT` | `str` | 创建时间（默认：`"2024-01-15T00:00:00Z"`） |
| `LIGHTRAG_DIGEST` | `str` | 模型摘要（默认：`"sha256:lightrag"`） |

**使用场景**:
- 当 LightRAG 以 Ollama 兼容 API 运行时
- 让 Ollama 客户端识别 LightRAG 为一个模型

---

## 七、设计模式与架构特点

### 7.1 抽象基类模式

- **`ABC` 继承**: 所有存储基类继承自 `ABC`，使用 `@abstractmethod` 定义接口
- **接口隔离**: 不同类型的存储有独立的抽象基类
- **默认实现**: 某些方法提供默认实现（如 `BaseGraphStorage` 的批量方法）

### 7.2 数据类模式

- **`@dataclass`**: 使用 `dataclass` 简化数据类的定义
- **类型注解**: 完整的类型注解，提高代码可读性和 IDE 支持
- **默认值**: 使用 `field(default=...)` 和 `field(default_factory=...)` 设置默认值

### 7.3 命名空间模式

- **工作空间隔离**: 通过 `workspace` 字段实现数据隔离
- **命名空间**: 通过 `namespace` 字段区分不同的存储用途
- **全局配置**: 通过 `global_config` 共享配置

### 7.4 异步优先

- **所有方法都是异步的**: 使用 `async def` 定义所有存储方法
- **支持并发**: 异步设计支持高并发操作

---

## 八、接口契约总结

### 8.1 存储生命周期

所有存储都必须实现：

1. **初始化**: `initialize()` - 加载数据、建立连接
2. **持久化**: `index_done_callback()` - 将内存更改写入磁盘
3. **清理**: `finalize()` - 关闭连接、释放资源
4. **删除**: `drop()` - 删除所有数据

### 8.2 数据操作契约

- **CRUD 操作**: 所有存储都支持创建、读取、更新、删除
- **批量操作**: 提供批量接口以提高性能
- **异步操作**: 所有操作都是异步的
- **错误处理**: 方法签名明确返回类型，支持 `None` 表示不存在

### 8.3 一致性保证

- **持久化时机**: 内存存储的更改在 `index_done_callback()` 时持久化
- **并发控制**: 注释中明确说明需要锁机制避免数据损坏
- **事务性**: 某些操作需要原子性（如批量删除）

---

## 九、扩展性设计

### 9.1 存储后端扩展

要添加新的存储后端，只需：

1. 继承对应的抽象基类（`BaseVectorStorage`、`BaseKVStorage`、`BaseGraphStorage`）
2. 实现所有 `@abstractmethod` 方法
3. 在 `kg/__init__.py` 中注册

### 9.2 批量操作优化

存储后端可以重写批量方法以提供更高效的实现：

```python
class MyGraphStorage(BaseGraphStorage):
    async def get_nodes_batch(self, node_ids: list[str]) -> dict[str, dict]:
        # 自定义批量实现（如使用 SQL JOIN）
        ...
```

### 9.3 查询参数扩展

`QueryParam` 使用 `@dataclass`，可以轻松添加新字段：

```python
@dataclass
class QueryParam:
    # 现有字段...
    custom_field: str = "default"  # 新字段
```

---

## 十、关键依赖

### 10.1 内部依赖

- `utils.py`: `EmbeddingFunc`
- `types.py`: `KnowledgeGraph`
- `constants.py`: 默认常量值

### 10.2 外部依赖

- `abc`: `ABC`、`abstractmethod`
- `enum`: `Enum`
- `dataclasses`: `dataclass`、`field`
- `typing`: 类型注解

---

## 十一、总结

`base.py` 是 LightRAG 框架的**抽象层核心**，它：

1. **定义了统一的存储接口**: 4 大存储抽象基类，支持插件式扩展
2. **提供了完整的数据结构**: 查询参数、结果、文档状态等
3. **支持多工作空间**: 通过命名空间和工作空间实现数据隔离
4. **异步优先设计**: 所有操作都是异步的，支持高并发
5. **类型安全**: 完整的类型注解，提高代码质量

通过清晰的抽象和良好的接口设计，`base.py` 实现了**高内聚、低耦合**的架构，为 LightRAG 框架提供了坚实的基础。
