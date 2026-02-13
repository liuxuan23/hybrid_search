# LanceDB 存储后端优化计划书

> 编写日期: 2026-02-13  
> 当前状态: Phase 1（核心实现）已完成并通过集成测试  
> 涉及文件: `lightrag/kg/lancedb_impl.py`（~1900 行）

---

## 一、项目现状总结

### 1.1 已完成工作

| 模块 | 类名 | 状态 | 备注 |
|------|------|------|------|
| KV 存储 | `LanceDBKVStorage` | ✅ 已通过单元测试 | JSON Blob 模式存储 |
| 向量存储 | `LanceDBVectorStorage` | ✅ 已通过单元测试 | 支持 cosine/L2/dot 距离度量 |
| 图存储 | `LanceDBGraphStorage` | ✅ 已通过单元测试 | 双表模型（nodes + edges） |
| 文档状态存储 | `LanceDBDocStatusStorage` | ✅ 已通过单元测试 | 固定 schema |
| 端到端集成 | `test_lancedb_integration.py` | ✅ 已通过 | 真实 LLM + Embedding |

### 1.2 当前架构概览

```
LanceDB 统一存储
├── ClientManager (单例异步连接池)
├── KV 存储 → 1 张表: {namespace}_kv
├── 向量存储 → 1 张表: {namespace}_vec (含 embedding 列)
├── 图存储 → 2 张表: {namespace}_nodes + {namespace}_edges
└── 文档状态 → 1 张表: doc_status
```

### 1.3 已知限制

- **图查询无索引**: `source_node_id` 和 `target_node_id` 列无标量索引，边表扫描为 O(n)
- **标签搜索使用 LIKE**: `search_labels()` 使用 `LIKE '%query%'` 全表扫描
- **向量索引未调优**: 使用 LanceDB 默认索引策略，未显式配置 HNSW/IVF 参数
- **无混合搜索**: 向量搜索与全文搜索是独立路径，未利用 LanceDB Hybrid Search
- **度数计算在 Python 侧**: `node_degrees_batch` 取回全部边后在 Python 中计数

---

## 二、优化计划

### Phase 2: 代码审查与基础优化

**目标**: 提升代码健壮性，消除明显性能瓶颈  
**预计工期**: 3-5 天

#### 2.1 代码审查清单

| 审查项 | 关注点 | 优先级 |
|--------|--------|--------|
| 错误处理 | 连接断开重连、表不存在容错、超时处理 | 高 |
| 并发安全 | `ClientManager` 单例在多协程下的安全性 | 高 |
| 资源泄漏 | 异步连接、游标是否正确关闭 | 高 |
| 类型一致性 | PyArrow 类型与 Python 类型的转换边界 | 中 |
| 日志规范 | 日志级别是否合理，是否有残留调试日志 | 中 |
| 配置校验 | 环境变量缺失时的友好提示 | 低 |

#### 2.2 标量索引优化

**问题**: 图存储的边表查询 `WHERE (source_node_id = X) OR (target_node_id = X)` 在无索引情况下为全表扫描。

**方案**: 在表创建后为高频查询列创建 BTree 标量索引。

```python
# 边表索引
await edge_table.create_scalar_index("source_node_id", index_type="BTREE")
await edge_table.create_scalar_index("target_node_id", index_type="BTREE")

# 节点表索引（可选，用于 search_labels）
await node_table.create_scalar_index("entity_type", index_type="BTREE")
```

**影响范围**: 
- `node_degree()` / `node_degrees_batch()` — 从 O(n) 降至 O(log n)
- `get_node_edges()` / `get_nodes_edges_batch()` — 同上
- `edge_degree()` / `edge_degrees_batch()` — 间接受益

**注意事项**:
- LanceDB 标量索引需要表中有一定数据量后才能创建
- 索引创建是异步操作，需要在 `__post_init__` 或首次查询时触发
- 需要处理索引已存在的幂等性

#### 2.3 度数计算优化

**现状**: `node_degrees_batch()` 取回所有匹配的边记录，在 Python 中用 Counter 计数。

**优化方向**: 
- **短期**: 优化 SQL 查询，只选取 `source_node_id` 和 `target_node_id` 两列，减少数据传输量
- **中期**: 考虑在节点表增加 `degree` 缓存字段，在 `upsert_edge` 时更新（需要权衡写放大）

```python
# 优化前: 取回全部字段
results = await self._edge_table.search().where(where_clause).select(["*"]).to_list()

# 优化后: 只取需要的列
results = await self._edge_table.search().where(where_clause) \
    .select(["source_node_id", "target_node_id"]).to_list()
```

---

### Phase 3: 图查询增强

**目标**: 利用 LanceDB 特性提升图查询能力  
**预计工期**: 5-7 天

#### 3.1 全文搜索替代 LIKE 查询

**现状**: `search_labels()` 使用 SQL LIKE 匹配：

```python
# 当前实现
where = f"`_id` LIKE '%{escaped_query}%'"
```

**方案**: 使用 LanceDB FTS（Full-Text Search）索引。

```python
# 创建 FTS 索引
await node_table.create_fts_index("_id", replace=True)
await node_table.create_fts_index("description", replace=True)

# 使用 FTS 搜索
results = await node_table.search(query, query_type="fts") \
    .select(["_id", "entity_type", "description"]) \
    .limit(limit) \
    .to_list()
```

**注意事项**:
- FTS 索引支持 BM25 评分，可以按相关性排序
- 需要注意 CJK（中日韩）分词支持情况，LanceDB 底层使用 Tantivy，对中文分词支持有限
- 可能需要配合 `tokenizer_name` 参数选择合适的分词器
- 如果 CJK 支持不足，可以保留 LIKE 作为 fallback

#### 3.2 图遍历优化

**现状**: `_bidirectional_bfs_nodes()` 在每一层 BFS 时都发起独立查询。

**优化方向**:
1. **批量邻居查询**: 将同一层的所有节点合并为一次 `WHERE IN` 查询
2. **结果缓存**: BFS 过程中缓存已访问节点的邻居列表，避免重复查询
3. **预计算**: 对于度数较高的热点节点，预计算其邻居列表

```python
# 优化前: 每个节点一次查询
for node in current_level_nodes:
    edges = await self.get_node_edges(node)

# 优化后: 批量查询
node_list = ", ".join([f"'{n}'" for n in current_level_nodes])
where = f"source_node_id IN ({node_list}) OR target_node_id IN ({node_list})"
all_edges = await self._edge_table.search().where(where).to_list()
```

#### 3.3 LightRAG 查询流程中的图存储调用优化

LightRAG 的查询流程中，图存储方法的调用顺序和频率如下：

| 查询模式 | 图存储方法调用 | 调用次数 |
|----------|--------------|----------|
| local | get_nodes_batch → node_degrees_batch → get_nodes_edges_batch → get_edges_batch → edge_degrees_batch | 5 |
| global | get_edges_batch → get_nodes_batch | 2 |
| hybrid | local 全部 + global 全部 | 7 |
| mix | hybrid 全部 + chunks_vdb.query | 8 |
| naive | 无图查询 | 0 |

**优化思路**: 
- **合并批量调用**: `get_nodes_batch` + `node_degrees_batch` 可以合并为一次查询（取回节点数据的同时计算度数）
- **并行化**: local 和 global 的图查询可以并行执行（当前 hybrid 模式下是顺序执行）
- **延迟加载**: 对于 token 截断后不需要的实体/关系，跳过详细数据的获取

---

### Phase 4: 混合搜索（Hybrid Search）

**目标**: 将向量搜索与全文搜索结合，提升检索质量  
**预计工期**: 5-7 天

#### 4.1 LanceDB 混合搜索机制

LanceDB 原生支持混合搜索，通过 Reciprocal Rank Fusion（RRF）融合向量搜索和全文搜索结果：

```python
# LanceDB 混合搜索示例
results = await table.search(query_text, query_type="hybrid") \
    .vector(query_vector) \
    .limit(top_k) \
    .to_list()
```

#### 4.2 应用场景分析

**实体搜索 (`entities_vdb.query`)**:
- 现状: 仅使用关键词的 embedding 进行向量搜索
- 优化: 结合向量相似度 + 实体名称/描述的全文搜索
- 收益: 精确名称匹配 + 语义相似，两者互补

**关系搜索 (`relationships_vdb.query`)**:
- 现状: 仅使用关键词的 embedding 进行向量搜索
- 优化: 结合向量相似度 + 关系描述的全文搜索
- 收益: 提升长尾查询的召回率

**文档块搜索 (`chunks_vdb.query`)**:
- 现状: naive/mix 模式使用纯向量搜索
- 优化: 结合向量相似度 + 文档块文本的全文搜索
- 收益: 对精确短语查询的命中率更高

#### 4.3 实现方案

```python
class LanceDBVectorStorage(BaseVectorStorage):
    async def query(self, query: str, top_k: int, ...) -> list[dict]:
        query_vector = await self._compute_query_embedding(query)
        
        if self._enable_hybrid_search and self._fts_field:
            # 混合搜索: 向量 + 全文
            results = await self._table.search(query, query_type="hybrid") \
                .vector(query_vector) \
                .distance_type(self._metric) \
                .limit(top_k) \
                .to_list()
        else:
            # 纯向量搜索（fallback）
            results = await self._table.search(query_vector) \
                .distance_type(self._metric) \
                .limit(top_k) \
                .to_list()
```

**配置化**: 通过 `vector_db_storage_cls_kwargs` 控制是否启用混合搜索：

```python
vector_db_storage_cls_kwargs={
    "lancedb_metric": "cosine",
    "enable_hybrid_search": True,
    "hybrid_reranker": "rrf",  # rrf | linear | cohere | cross_encoder
}
```

#### 4.4 Reranker 选择

| Reranker | 特点 | 适用场景 |
|----------|------|----------|
| RRF (默认) | 无需训练，基于排名融合 | 通用场景，推荐作为默认 |
| Linear | 线性加权，可调向量/文本权重 | 需要精细控制时 |
| Cross-Encoder | 使用模型重排序，质量最高 | 对精度要求极高时 |
| Cohere | 调用 Cohere API 重排序 | 有 Cohere 账号时 |

**建议**: 默认使用 RRF，提供配置项允许用户切换。

---

### Phase 5: 向量索引调优

**目标**: 为大规模数据场景配置最优的 ANN 索引  
**预计工期**: 3-5 天

#### 5.1 索引策略选择

| 索引类型 | 数据规模 | 召回率 | 延迟 | 内存 |
|----------|---------|--------|------|------|
| 无索引（暴力搜索） | <10K | 100% | 低 | 低 |
| IVF-PQ | 100K~10M | 中等 | 低 | 低 |
| HNSW-SQ | 10K~1M | 高 | 极低 | 中 |
| IVF-HNSW-SQ | >1M | 高 | 低 | 中 |

#### 5.2 自动索引策略

根据数据量自动选择索引策略：

```python
async def _maybe_create_vector_index(self):
    """数据量达到阈值时自动创建/更新向量索引"""
    row_count = await self._table.count_rows()
    
    if row_count < 10_000:
        return  # 小表使用暴力搜索即可
    
    if row_count < 100_000:
        # 中等规模: HNSW + SQ 量化
        await self._table.create_index(
            metric=self._metric,
            index_type="IVF_HNSW_SQ",
            num_partitions=min(row_count // 5000, 256),
        )
    else:
        # 大规模: IVF-PQ
        await self._table.create_index(
            metric=self._metric,
            index_type="IVF_PQ",
            num_partitions=min(row_count // 5000, 512),
            num_sub_vectors=min(self._embedding_dim // 8, 96),
        )
```

#### 5.3 查询参数调优

```python
# 添加 ANN 搜索参数
results = await self._table.search(query_vector) \
    .distance_type(self._metric) \
    .nprobes(20)           \  # IVF: 搜索的分区数（越大越精确，越慢）
    .ef(150)               \  # HNSW: 搜索时的扩展因子
    .refine_factor(10)     \  # 重排序因子（提升召回率）
    .limit(top_k) \
    .to_list()
```

---

### Phase 6: 性能基准测试

**目标**: 量化 LanceDB 后端性能，与现有后端对比  
**预计工期**: 5-7 天

#### 6.1 测试维度

| 维度 | 指标 | 说明 |
|------|------|------|
| 写入性能 | 吞吐量 (docs/s) | 文档插入速率 |
| 向量查询 | 延迟 (ms), QPS | 单次查询/并发查询 |
| 图查询 | 延迟 (ms), QPS | 节点/边查询、BFS 遍历 |
| 端到端查询 | 延迟 (ms) | 从用户问题到上下文构建（不含 LLM） |
| 内存占用 | RSS (MB) | 不同数据量下的内存 |
| 磁盘占用 | 大小 (MB) | 与 NanoVectorDB + NetworkX 对比 |
| 可扩展性 | 性能曲线 | 数据量从 1K 到 100K 的性能变化 |

#### 6.2 对比基线

| 存储后端 | 向量存储 | 图存储 | KV 存储 | 适用场景 |
|----------|---------|--------|---------|----------|
| 默认 | NanoVectorDB | NetworkX | JsonKV | 轻量级、单机 |
| PostgreSQL | pgvector | AGE | PostgreSQL | 生产级、团队协作 |
| **LanceDB** | **LanceDB** | **LanceDB** | **LanceDB** | **统一存储、中等规模** |

#### 6.3 测试数据集

| 数据集 | 文档数 | 预期实体数 | 预期关系数 | 用途 |
|--------|--------|-----------|-----------|------|
| 小规模 | 10 | ~100 | ~200 | 功能验证 |
| 中规模 | 100 | ~1K | ~3K | 性能基准 |
| 大规模 | 1000 | ~10K | ~30K | 压力测试 |
| 超大规模 | 5000+ | ~50K | ~150K | 极限测试 |

#### 6.4 基准测试脚本结构

```
tests/benchmarks/
├── bench_config.py          # 测试配置（数据集路径、后端选择等）
├── bench_write.py           # 写入性能测试
├── bench_vector_query.py    # 向量查询性能测试
├── bench_graph_query.py     # 图查询性能测试
├── bench_e2e_query.py       # 端到端查询性能测试（不含 LLM）
├── bench_memory.py          # 内存占用测试
└── bench_report.py          # 生成对比报告
```

#### 6.5 关键测试场景

**写入测试**:
```python
# 批量插入文档，记录每批次耗时
for batch in batches:
    start = time.perf_counter()
    await rag.ainsert(batch)
    elapsed = time.perf_counter() - start
    # 记录: batch_size, elapsed, throughput
```

**向量查询测试**:
```python
# 不同 top_k 下的查询延迟
for top_k in [5, 10, 20, 50]:
    for query in test_queries:
        start = time.perf_counter()
        results = await entities_vdb.query(query, top_k=top_k)
        elapsed = time.perf_counter() - start
        # 记录: top_k, elapsed, result_count
```

**图查询测试**:
```python
# 测试各种图操作
operations = {
    "get_node": lambda: graph.get_node(random_node_id),
    "get_edge": lambda: graph.get_edge(random_src, random_tgt),
    "node_degree": lambda: graph.node_degree(random_node_id),
    "get_node_edges": lambda: graph.get_node_edges(random_node_id),
    "bfs_2hop": lambda: graph._bidirectional_bfs_nodes(random_node_id, max_depth=2),
}
```

---

## 三、优先级与路线图

```
Phase 2 (Week 1)          Phase 3 (Week 2)         Phase 4 (Week 3)        Phase 5-6 (Week 4)
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│ 代码审查         │    │ FTS 替代 LIKE    │    │ 混合搜索实现     │    │ 向量索引调优     │
│ 标量索引         │───>│ 图遍历优化       │───>│ Reranker 集成    │───>│ 性能基准测试     │
│ 度数计算优化     │    │ 批量调用合并     │    │ 配置化接口       │    │ 对比报告         │
└──────────────────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘
        高优先级                高优先级                中优先级              中优先级
```

---

## 四、风险与注意事项

### 4.1 技术风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| LanceDB FTS 对中文分词支持有限 | 中文搜索质量下降 | 保留 LIKE 作为 fallback；评估接入 jieba 分词 |
| 混合搜索 API 在异步模式下行为不一致 | 功能不可用 | 充分测试 async API；关注 LanceDB issue tracker |
| 大规模数据下索引构建耗时 | 写入延迟增加 | 异步构建索引；设置合理的重建阈值 |
| LanceDB 版本升级可能有 breaking changes | 维护成本 | 固定版本号；添加版本兼容测试 |

### 4.2 兼容性注意

- **与现有后端的接口一致性**: 所有优化不应改变 `BaseVectorStorage` / `BaseGraphStorage` 的公共接口签名
- **配置向后兼容**: 新增配置项应有合理默认值，不破坏现有部署
- **降级策略**: 当 FTS 索引或 ANN 索引不可用时，自动降级到基础查询

### 4.3 测试策略

- 每个 Phase 完成后运行全量单元测试 + 集成测试
- 性能优化前后用基准测试量化效果
- 重点关注边界情况: 空表、单条记录、超大批量、特殊字符

---

## 五、参考资料

- [LanceDB 官方文档 - 索引](https://lancedb.com/docs/indexing)
- [LanceDB 官方文档 - FTS 索引](https://docs.lancedb.com/indexing/fts-index)
- [LanceDB 官方文档 - 混合搜索](https://lancedb.com/docs/search/hybrid-search/)
- [LanceDB 异步 API 参考](https://lancedb.github.io/lancedb/python/python/)
- [LightRAG 查询流程分析](./operate_py_analysis.md)
- [LightRAG 项目结构](./ProjectStructure.md)
