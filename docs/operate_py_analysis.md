# `operate.py` 核心操作模块详细分析

> 文件路径: `lightrag/operate.py`  
> 总行数: **5000 行**  
> 核心职责: RAG 流水线的核心操作逻辑（分块、实体提取、知识图谱构建、检索查询）

---

## 一、文件概览

`operate.py` 是 LightRAG 框架的**核心操作引擎**，实现了 RAG 流水线的所有关键步骤：

1. **文本分块** (`chunking_by_token_size`)
2. **实体/关系提取** (`extract_entities`)
3. **知识图谱合并** (`merge_nodes_and_edges`)
4. **知识图谱查询** (`kg_query`)
5. **朴素向量检索** (`naive_query`)
6. **知识重建** (`rebuild_knowledge_from_chunks`)

---

## 二、核心函数分类

### 2.1 文本处理函数

| 函数 | 行号 | 功能 |
|------|------|------|
| `chunking_by_token_size()` | 99-162 | 按 token 大小分块文本（支持字符分隔和重叠） |
| `_truncate_entity_identifier()` | 78-96 | 截断超长的实体标识符 |

### 2.2 实体/关系提取函数

| 函数 | 行号 | 功能 |
|------|------|------|
| `extract_entities()` | 2766-3010 | 从文本块中提取实体和关系（主入口） |
| `_process_single_content()` | 2814-2943 | 处理单个 chunk 的提取 |
| `_process_extraction_result()` | 910-1034 | 解析 LLM 提取结果 |
| `_handle_single_entity_extraction()` | 379-450 | 处理单个实体提取 |
| `_handle_single_relationship_extraction()` | 451-532 | 处理单个关系提取 |
| `_get_cached_extraction_results()` | 820-909 | 获取缓存的提取结果 |

### 2.3 知识图谱合并函数

| 函数 | 行号 | 功能 |
|------|------|------|
| `merge_nodes_and_edges()` | 2396-2765 | 合并所有 chunk 提取的节点和边（主入口） |
| `_merge_nodes_then_upsert()` | 1593-1870 | 合并实体节点并更新存储 |
| `_merge_edges_then_upsert()` | 1871-2395 | 合并关系边并更新存储 |
| `_handle_entity_relation_summary()` | 165-296 | 处理实体/关系描述的摘要（Map-Reduce） |
| `_summarize_descriptions()` | 297-378 | 使用 LLM 摘要描述列表 |

### 2.4 知识图谱查询函数

| 函数 | 行号 | 功能 |
|------|------|------|
| `kg_query()` | 3013-3221 | 知识图谱查询主入口（支持 local/global/hybrid/mix 模式） |
| `_build_query_context()` | 4047-4166 | 构建查询上下文 |
| `_build_context_str()` | 3864-4046 | 构建上下文字符串 |
| `_get_vector_context()` | 3365-3421 | 获取向量检索上下文 |
| `_perform_kg_search()` | 3422-3590 | 执行知识图谱搜索 |
| `_apply_token_truncation()` | 3591-3761 | 应用 token 截断（统一预算控制） |
| `_merge_all_chunks()` | 3762-3863 | 合并所有检索到的 chunk |
| `get_keywords_from_query()` | 3223-3253 | 从查询中获取关键词 |
| `extract_keywords_only()` | 3255-3364 | 仅提取关键词（不执行查询） |

### 2.5 知识图谱数据获取函数

| 函数 | 行号 | 功能 |
|------|------|------|
| `_get_node_data()` | 4167-4224 | 获取节点数据（实体信息） |
| `_get_edge_data()` | 4440-4495 | 获取边数据（关系信息） |
| `_find_most_related_edges_from_entities()` | 4225-4280 | 从实体查找最相关的关系 |
| `_find_related_text_unit_from_entities()` | 4281-4439 | 从实体查找相关文本单元 |
| `_find_most_related_entities_from_relationships()` | 4496-4528 | 从关系查找最相关的实体 |
| `_find_related_text_unit_from_relations()` | 4529-4732 | 从关系查找相关文本单元 |

### 2.6 朴素查询函数

| 函数 | 行号 | 功能 |
|------|------|------|
| `naive_query()` | 4733-5000 | 朴素向量检索查询（不使用知识图谱） |

### 2.7 知识重建函数

| 函数 | 行号 | 功能 |
|------|------|------|
| `rebuild_knowledge_from_chunks()` | 533-819 | 从已有 chunk 重建知识图谱 |
| `_rebuild_from_extraction_result()` | 1035-1070 | 从提取结果重建 |
| `_rebuild_single_entity()` | 1071-1313 | 重建单个实体 |
| `_rebuild_single_relationship()` | 1314-1592 | 重建单个关系 |

---

## 三、核心函数详解

### 3.1 `chunking_by_token_size()` - 文本分块

**功能**: 将文档文本分割成固定大小的 chunk

**参数**:
- `tokenizer`: 分词器实例
- `content`: 要分块的文本
- `split_by_character`: 可选的字符分隔符（如 `"\n"`）
- `split_by_character_only`: 是否仅按字符分隔
- `chunk_overlap_token_size`: chunk 重叠的 token 数（默认 100）
- `chunk_token_size`: 每个 chunk 的最大 token 数（默认 1200）

**返回**: `list[dict[str, Any]]`，每个字典包含：
- `tokens`: chunk 的 token 数
- `content`: chunk 的文本内容
- `chunk_order_index`: chunk 在文档中的顺序索引

**处理逻辑**:
1. **按字符分隔模式** (`split_by_character` 不为 None):
   - 如果 `split_by_character_only=True`: 仅按字符分隔，不进行二次分块
   - 否则: 先按字符分隔，如果某个 chunk 超过 token 限制，再按 token 大小分块
2. **纯 token 分块模式**: 按 `chunk_token_size - chunk_overlap_token_size` 的步长滑动窗口分块

**特点**:
- 支持重叠分块，保持上下文连续性
- 自动处理超长 chunk（抛出 `ChunkTokenLimitExceededError`）
- 返回格式统一，便于后续处理

---

### 3.2 `extract_entities()` - 实体/关系提取

**功能**: 从文本块中提取实体和关系（使用 LLM）

**参数**:
- `chunks`: 文本块字典 `{chunk_id: TextChunkSchema}`
- `global_config`: 全局配置
- `pipeline_status`: 流水线状态（用于取消检测）
- `pipeline_status_lock`: 流水线状态锁
- `llm_response_cache`: LLM 响应缓存
- `text_chunks_storage`: 文本块存储（用于更新缓存列表）

**返回**: `list[tuple[dict, dict]]`，每个元组包含：
- `maybe_nodes`: 提取的实体字典 `{entity_name: [entity_data]}`
- `maybe_edges`: 提取的关系字典 `{edge_key: [edge_data]}`

**处理流程**:

1. **准备提示词**
   - 格式化系统提示词（`entity_extraction_system_prompt`）
   - 格式化用户提示词（包含 chunk 内容）
   - 准备继续提取提示词（用于 gleaning）

2. **并发处理所有 chunk**（使用 `asyncio.Semaphore` 限制并发数）
   - 对每个 chunk 调用 `_process_single_content()`
   - 支持取消检测（检查 `pipeline_status["cancellation_requested"]`）

3. **单个 chunk 处理** (`_process_single_content()`):
   - 调用 LLM 提取实体和关系（使用缓存）
   - 解析提取结果（`_process_extraction_result()`）
   - 如果 `entity_extract_max_gleaning > 0`，进行二次提取（gleaning）
   - 合并初始提取和 gleaning 结果（保留描述更长的版本）
   - 更新 chunk 的 LLM 缓存列表

4. **错误处理**
   - 如果任何 chunk 处理失败，取消所有待处理任务
   - 在异常消息中添加进度前缀（`C[processed/total]`）

**特点**:
- 支持 LLM 响应缓存，减少重复调用
- 支持 gleaning（二次提取），提高提取质量
- 并发处理，提高效率
- 支持取消操作

---

### 3.3 `merge_nodes_and_edges()` - 知识图谱合并

**功能**: 合并所有 chunk 提取的实体和关系，构建知识图谱

**参数**:
- `chunk_results`: chunk 提取结果列表
- `knowledge_graph_inst`: 知识图谱存储实例
- `entity_vdb`: 实体向量数据库
- `relationships_vdb`: 关系向量数据库
- `global_config`: 全局配置
- `full_entities_storage`: 文档实体列表存储
- `full_relations_storage`: 文档关系列表存储
- `doc_id`: 文档 ID
- `pipeline_status`: 流水线状态
- `pipeline_status_lock`: 流水线状态锁
- `llm_response_cache`: LLM 响应缓存
- `entity_chunks_storage`: 实体-chunk 关联存储
- `relation_chunks_storage`: 关系-chunk 关联存储

**处理流程**（三阶段合并）:

#### Phase 1: 处理所有实体（并发）

1. **收集所有实体**
   - 从所有 chunk 结果中收集实体
   - 按实体名分组：`{entity_name: [entity_data_list]}`

2. **并发处理每个实体**（使用 `get_storage_keyed_lock()` 确保同一实体不会被并发处理）
   - 调用 `_merge_nodes_then_upsert()`:
     - 检查实体是否已存在
     - 如果存在：合并描述（使用 `_handle_entity_relation_summary()`）
     - 如果不存在：直接创建
     - 更新图存储、向量存储、chunk 追踪存储

3. **错误处理**
   - 如果任何实体处理失败，取消所有待处理任务
   - 在异常消息中添加实体名前缀

#### Phase 2: 处理所有关系（并发）

1. **收集所有关系**
   - 从所有 chunk 结果中收集关系
   - 按边键分组（排序后的 `(src, tgt)` 元组）：`{edge_key: [edge_data_list]}`

2. **并发处理每个关系**（使用 `get_storage_keyed_lock()` 确保同一关系不会被并发处理）
   - 调用 `_merge_edges_then_upsert()`:
     - 检查关系是否已存在
     - 如果存在：合并描述和关键词
     - 如果不存在：直接创建
     - 更新图存储、向量存储、chunk 追踪存储
     - 确保关联的实体存在（如果不存在则创建）

3. **错误处理**
   - 如果任何关系处理失败，取消所有待处理任务
   - 在异常消息中添加关系键前缀

#### Phase 3: 更新文档索引

- 更新 `full_entities_storage`: 存储文档关联的实体列表
- 更新 `full_relations_storage`: 存储文档关联的关系列表

**特点**:
- 两阶段处理确保数据一致性（先实体后关系）
- 使用键控锁避免并发冲突
- 支持描述合并和摘要（Map-Reduce 策略）
- 自动创建缺失的实体

---

### 3.4 `_handle_entity_relation_summary()` - 描述摘要处理

**功能**: 使用 Map-Reduce 策略摘要实体/关系描述列表

**参数**:
- `description_type`: 描述类型（`"entity"` 或 `"relation"`）
- `entity_or_relation_name`: 实体或关系名称
- `description_list`: 描述字符串列表
- `seperator`: 分隔符（`GRAPH_FIELD_SEP`）
- `global_config`: 全局配置
- `llm_response_cache`: LLM 响应缓存

**返回**: `tuple[str, bool]` - (摘要后的描述, 是否使用了 LLM)

**Map-Reduce 策略**:

1. **无需摘要的情况**:
   - 描述列表为空 → 返回空字符串
   - 只有一个描述 → 直接返回
   - 总 token 数 < `summary_context_size` 且描述数 < `force_llm_summary_on_merge` → 直接拼接

2. **直接摘要**:
   - 总 token 数 < `summary_max_tokens` → 调用 LLM 一次性摘要

3. **分块摘要**（Map-Reduce）:
   - 将描述列表分割成多个 chunk（每个 chunk 的 token 数 < `summary_context_size`）
   - 对每个 chunk 调用 LLM 摘要（Map 阶段）
   - 递归处理摘要结果（Reduce 阶段）
   - 继续直到描述数 < `force_llm_summary_on_merge` 或总 token 数 < `summary_max_tokens`

**特点**:
- 智能判断是否需要摘要
- 使用 Map-Reduce 处理超长描述列表
- 支持 LLM 缓存

---

### 3.5 `kg_query()` - 知识图谱查询

**功能**: 执行知识图谱增强的 RAG 查询

**参数**:
- `query`: 查询字符串
- `knowledge_graph_inst`: 知识图谱存储实例
- `entities_vdb`: 实体向量数据库
- `relationships_vdb`: 关系向量数据库
- `text_chunks_db`: 文本块存储
- `query_param`: 查询参数（`QueryParam`）
- `global_config`: 全局配置
- `hashing_kv`: 缓存存储
- `system_prompt`: 系统提示词（可选）
- `chunks_vdb`: 文档块向量数据库（可选）

**返回**: `QueryResult | None`

**处理流程**:

1. **关键词提取**
   - 如果 `query_param` 中已提供关键词，直接使用
   - 否则调用 `get_keywords_from_query()` → `extract_keywords_only()` 提取
   - 返回 `(high_level_keywords, low_level_keywords)`

2. **构建查询上下文**
   - 调用 `_build_query_context()`:
     - 根据查询模式（`local`/`global`/`hybrid`/`mix`）构建上下文
     - 检索相关实体、关系、chunk
     - 应用 token 预算控制
     - 生成引用列表

3. **检查上下文是否为空**
   - 如果无法构建上下文，返回 `None`

4. **根据查询参数返回不同内容**:
   - `only_need_context=True`: 仅返回上下文字符串
   - `only_need_prompt=True`: 返回完整提示词
   - 否则：调用 LLM 生成回答

5. **LLM 调用**:
   - 检查缓存（使用 `args_hash`）
   - 如果缓存命中，直接返回缓存结果
   - 否则调用 LLM（支持流式/非流式）
   - 保存结果到缓存

6. **返回统一结果**
   - 非流式：返回 `QueryResult(content=response, raw_data=context_result.raw_data)`
   - 流式：返回 `QueryResult(response_iterator=response, raw_data=context_result.raw_data, is_streaming=True)`

**查询模式**:

- **`local`**: 基于低层关键词检索实体及其相关 chunk
- **`global`**: 基于高层关键词检索关系及其相关实体和 chunk
- **`hybrid`**: 结合 local 和 global 的结果（轮询合并）
- **`mix`**: 知识图谱数据 + 向量检索的文档 chunk

---

### 3.6 `_build_query_context()` - 构建查询上下文

**功能**: 根据查询模式构建统一的查询上下文

**处理逻辑**:

1. **根据模式选择检索策略**:
   - `local`: 调用 `_perform_kg_search(mode="local")`
   - `global`: 调用 `_perform_kg_search(mode="global")`
   - `hybrid`: 分别调用 local 和 global，然后轮询合并
   - `mix`: 调用 `_perform_kg_search(mode="mix")` + 向量检索 chunk

2. **应用 token 预算控制** (`_apply_token_truncation()`):
   - 统一 token 预算：`max_total_tokens`
   - 分配预算：实体、关系、chunk、系统提示词
   - 按优先级截断：chunk → 关系 → 实体

3. **合并所有 chunk** (`_merge_all_chunks()`):
   - 去重 chunk
   - 按相关性排序
   - 应用重排序（如果启用）

4. **构建上下文字符串** (`_build_context_str()`):
   - 格式化实体信息
   - 格式化关系信息
   - 格式化 chunk 信息

5. **生成引用列表** (`generate_reference_list_from_chunks()`):
   - 从 chunk 中提取文件路径
   - 生成引用 ID

6. **返回 `QueryContextResult`**:
   - `context`: 上下文字符串
   - `raw_data`: 结构化数据（实体、关系、chunk、引用、元数据）

---

### 3.7 `naive_query()` - 朴素向量检索

**功能**: 纯向量检索查询（不使用知识图谱）

**参数**:
- `query`: 查询字符串
- `chunks_vdb`: 文档块向量数据库
- `query_param`: 查询参数
- `global_config`: 全局配置
- `hashing_kv`: 缓存存储
- `system_prompt`: 系统提示词（可选）

**返回**: `QueryResult | None`

**处理流程**:

1. **向量检索**
   - 调用 `_get_vector_context()` 检索相关 chunk
   - 如果无结果，返回 `None`

2. **Token 预算控制**
   - 计算系统提示词模板的 token 开销
   - 动态计算可用 token 数
   - 截断 chunk 列表

3. **构建上下文**
   - 格式化 chunk 内容
   - 构建系统提示词

4. **LLM 调用**（与 `kg_query()` 类似）
   - 检查缓存
   - 调用 LLM
   - 保存缓存

5. **返回结果**
   - 非流式：`QueryResult(content=response, raw_data=...)`
   - 流式：`QueryResult(response_iterator=response, raw_data=..., is_streaming=True)`

**特点**:
- 简单快速，适合不需要知识图谱的场景
- 返回的 `raw_data` 中 `entities` 和 `relationships` 为空

---

### 3.8 `rebuild_knowledge_from_chunks()` - 知识重建

**功能**: 从已有 chunk 重建知识图谱（用于文档删除后的部分重建）

**参数**:
- `entities_to_rebuild`: 需要重建的实体字典 `{entity_name: [remaining_chunk_ids]}`
- `relationships_to_rebuild`: 需要重建的关系字典 `{edge_key: [remaining_chunk_ids]}`
- `knowledge_graph_inst`: 知识图谱存储实例
- `entities_vdb`: 实体向量数据库
- `relationships_vdb`: 关系向量数据库
- `text_chunks_storage`: 文本块存储
- `llm_response_cache`: LLM 响应缓存
- `global_config`: 全局配置
- `pipeline_status`: 流水线状态
- `pipeline_status_lock`: 流水线状态锁
- `entity_chunks_storage`: 实体-chunk 关联存储
- `relation_chunks_storage`: 关系-chunk 关联存储

**处理流程**:

1. **收集需要重建的 chunk**
   - 从 `entities_to_rebuild` 和 `relationships_to_rebuild` 中收集所有 chunk ID
   - 去重

2. **从 chunk 重新提取实体和关系**
   - 调用 `extract_entities()` 提取
   - 使用 LLM 缓存加速（如果可用）

3. **重建实体** (`_rebuild_single_entity()`):
   - 从提取结果中筛选属于该实体的数据
   - 合并描述（如果实体已存在）
   - 更新图存储、向量存储、chunk 追踪存储

4. **重建关系** (`_rebuild_single_relationship()`):
   - 从提取结果中筛选属于该关系的数据
   - 合并描述和关键词（如果关系已存在）
   - 更新图存储、向量存储、chunk 追踪存储
   - 确保关联的实体存在

**特点**:
- 支持增量重建（只重建受影响的实体/关系）
- 利用 LLM 缓存加速
- 保持数据一致性

---

## 四、关键设计模式

### 4.1 并发控制

- **Semaphore**: 限制并发任务数（`asyncio.Semaphore`）
- **键控锁**: 使用 `get_storage_keyed_lock()` 确保同一实体/关系不会被并发处理
- **取消检测**: 通过 `pipeline_status["cancellation_requested"]` 支持取消操作

### 4.2 错误处理

- **任务取消**: 如果任何任务失败，取消所有待处理任务
- **异常前缀**: 在异常消息中添加上下文信息（chunk ID、实体名、进度等）
- **错误隔离**: 单个实体/关系处理失败不影响其他处理

### 4.3 缓存机制

- **LLM 响应缓存**: 使用 `use_llm_func_with_cache()` 缓存提取结果
- **查询缓存**: 使用 `args_hash` 缓存查询结果
- **缓存列表追踪**: 更新 chunk 的 `llm_cache_list`，支持按文档删除缓存

### 4.4 Token 预算控制

- **统一预算**: `max_total_tokens` 包含系统提示词、实体、关系、chunk
- **动态分配**: 根据实际内容动态分配预算
- **优先级截断**: chunk → 关系 → 实体（按重要性）

### 4.5 Map-Reduce 摘要

- **智能判断**: 根据描述数量和 token 数判断是否需要摘要
- **分块处理**: 将长列表分割成多个 chunk
- **递归合并**: 递归处理摘要结果直到满足条件

---

## 五、数据流图

### 5.1 文档插入流程

```
文档文本
  ↓
chunking_by_token_size()
  ↓
文本块列表
  ↓
extract_entities() [并发处理]
  ↓
实体/关系提取结果列表
  ↓
merge_nodes_and_edges()
  ├─ Phase 1: 合并实体 [并发]
  ├─ Phase 2: 合并关系 [并发]
  └─ Phase 3: 更新文档索引
  ↓
知识图谱构建完成
```

### 5.2 查询流程

```
用户查询
  ↓
get_keywords_from_query()
  ↓
(high_level_keywords, low_level_keywords)
  ↓
_build_query_context()
  ├─ _perform_kg_search() [根据模式]
  ├─ _apply_token_truncation() [预算控制]
  ├─ _merge_all_chunks() [合并去重]
  └─ _build_context_str() [格式化]
  ↓
QueryContextResult
  ├─ context: 上下文字符串
  └─ raw_data: 结构化数据
  ↓
kg_query() / naive_query()
  ├─ 检查缓存
  ├─ 调用 LLM [流式/非流式]
  └─ 保存缓存
  ↓
QueryResult
  ├─ content / response_iterator
  └─ raw_data
```

---

## 六、性能优化要点

1. **并发处理**: 使用 `asyncio.Semaphore` 和 `asyncio.create_task()` 并发处理多个 chunk/实体/关系
2. **缓存机制**: LLM 响应缓存和查询结果缓存大幅减少重复调用
3. **批量操作**: 使用批量接口（`get_by_ids()`、`get_nodes_batch()` 等）
4. **键控锁**: 细粒度锁避免不必要的串行化
5. **延迟计算**: 只在需要时计算 token 数和摘要

---

## 七、关键依赖

### 7.1 内部依赖

- `base.py`: `BaseGraphStorage`、`BaseKVStorage`、`BaseVectorStorage`、`QueryParam`、`QueryResult`
- `prompt.py`: `PROMPTS` 字典（提示词模板）
- `utils.py`: 工具函数（tokenizer、缓存、文本处理等）
- `constants.py`: 常量定义
- `exceptions.py`: 自定义异常

### 7.2 外部依赖

- `asyncio`: 异步编程
- `json_repair`: JSON 修复
- `collections`: `Counter`、`defaultdict`

---

## 八、总结

`operate.py` 是 LightRAG 框架的**核心操作引擎**，实现了：

1. **完整的 RAG 流水线**: 从文本分块到知识图谱构建
2. **灵活的查询模式**: 支持 6 种查询模式（local/global/hybrid/mix/naive/bypass）
3. **智能的合并策略**: Map-Reduce 摘要、描述合并、优先级处理
4. **健壮的并发控制**: Semaphore、键控锁、取消检测
5. **高效的缓存机制**: LLM 响应缓存、查询结果缓存
6. **精确的预算控制**: 统一的 token 预算管理和动态分配

通过清晰的函数划分和良好的抽象设计，`operate.py` 实现了**高内聚、低耦合**的架构，为上层 `lightrag.py` 提供了强大的操作能力。
