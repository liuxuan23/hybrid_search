# 表存储图的查询优化方案分析

> 编写日期: 2026-02-14  
> 涉及文件: `lightrag/kg/lancedb_impl.py`, `lightrag/kg/mongo_impl.py`, `lightrag/kg/postgres_impl.py`, `lightrag/base.py`, `lightrag/operate.py`  
> 关联文档: [LanceDB 优化计划书](./lancedb_optimization_plan.md)

---

## 一、当前架构概要

LightRAG 的图存储采用**节点表 + 边表**的经典关系模型：

- **节点表**: 以 `entity_id` 为主键，存储节点属性（description, entity_type, source_id 等）
- **边表**: 存储 `source_node_id`, `target_node_id` 及边属性（relationship, weight, keywords 等）
- **无向图语义**: 所有边查询都需检查双向

### 1.1 三种后端实现

| 后端 | 节点存储 | 边存储 | 图遍历方式 |
|------|----------|--------|-----------|
| MongoDB | 文档集合 | 文档集合 | `$graphLookup` / 客户端 BFS |
| PostgreSQL | AGE 图表 (agtype) | AGE 图表 (agtype) | Cypher / 客户端 BFS |
| LanceDB | Lance 表 | Lance 表 | 客户端 BFS |

### 1.2 已知性能瓶颈

- **图查询无索引** (LanceDB): `source_node_id` 和 `target_node_id` 列无标量索引，边表扫描为 O(n)
- **OR 条件查询**: 无向图语义要求 `WHERE (src=X) OR (tgt=X)`，即使有索引也需两次查找
- **度数计算在 Python 侧**: `node_degrees_batch` 取回全部边后在 Python 中计数
- **BFS 递归查询**: 每层 BFS 发起独立的数据库查询，depth=3 至少 6 次往返
- **索引管道 N+1 查询**: `_merge_nodes_then_upsert` / `_merge_edges_then_upsert` 中逐个查询

---

## 二、表存储图的通用优化方案

### 2.1 索引管道的 N+1 查询问题（P0 优先级）

**问题**: 在 `operate.py` 的索引管道中，节点和边的合并操作逐个查询数据库。

**节点合并** (`_merge_nodes_then_upsert`):
- 每个实体单独调用 `get_node()` → 单独调用 `upsert_node()`
- 处理 1000 个实体 = 2000 次数据库往返

**边合并** (`_merge_edges_then_upsert`):
- 每条边先调用 `has_edge()` → 再调用 `get_edge()` → 最后调用 `upsert_edge()`
- 处理 1000 条边 = 3000 次数据库往返

**优化方案**:

```python
# 方案A: 在 merge_nodes_and_edges() 中预批量获取
async def merge_nodes_and_edges(entities, knowledge_graph_inst):
    all_entity_ids = [e["entity_id"] for e in entities]
    existing_nodes = await knowledge_graph_inst.get_nodes_batch(all_entity_ids)
    for entity in entities:
        await _merge_nodes_then_upsert(entity, existing_nodes.get(entity["entity_id"]))

# 方案B: 新增批量 upsert 接口
async def upsert_nodes_batch(self, nodes: dict[str, dict]) -> None:
    ops = [UpdateOne({"_id": nid}, {"$set": data}, upsert=True) for nid, data in nodes.items()]
    await self.collection.bulk_write(ops)
```

### 2.2 冗余的 has_edge + get_edge 双查询（P0 优先级）

**问题**: 代码中存在先 `has_edge()` 再 `get_edge()` 的模式。

```python
# 当前: 两次查询
if await knowledge_graph_inst.has_edge(src_id, tgt_id):
    already_edge = await knowledge_graph_inst.get_edge(src_id, tgt_id)

# 优化: 一次查询
already_edge = await knowledge_graph_inst.get_edge(src_id, tgt_id)
if already_edge is not None:
    # 合并逻辑
```

### 2.3 edge_degree 的串行调用（P1 优先级）

**问题** (MongoDB): `edge_degree` 中两次 `node_degree` 是独立的但串行执行。

```python
# 当前: 串行
async def edge_degree(self, src_id, tgt_id):
    src_degree = await self.node_degree(src_id)    # 等待
    trg_degree = await self.node_degree(tgt_id)    # 等待
    return src_degree + trg_degree

# 优化: 并发
async def edge_degree(self, src_id, tgt_id):
    src_degree, trg_degree = await asyncio.gather(
        self.node_degree(src_id),
        self.node_degree(tgt_id)
    )
    return src_degree + trg_degree
```

### 2.4 edge_degrees_batch 未覆盖（P1 优先级）

**问题**: 基类默认实现逐个调用 `edge_degree()`，MongoDB 和 LanceDB 均未覆盖该方法。

```python
# 基类默认: O(2N) 次查询
async def edge_degrees_batch(self, edge_pairs):
    for src_id, tgt_id in edge_pairs:
        degree = await self.edge_degree(src_id, tgt_id)  # 每对调用2次 node_degree

# 优化: 利用 node_degrees_batch，降至 1 次批量查询
async def edge_degrees_batch(self, edge_pairs):
    all_nodes = set()
    for src, tgt in edge_pairs:
        all_nodes.add(src)
        all_nodes.add(tgt)
    node_degrees = await self.node_degrees_batch(list(all_nodes))
    return {
        (src, tgt): node_degrees.get(src, 0) + node_degrees.get(tgt, 0)
        for src, tgt in edge_pairs
    }
```

### 2.5 upsert_edge 中冗余的节点存在性检查（P1 优先级）

**问题**: 每次 `upsert_edge` 都会调用 `upsert_node(source_node_id, {})` 确保源节点存在，在批量插入时产生大量冗余操作。

**优化**: 将节点存在性检查移到调用侧，或在批量操作中跳过保护性调用。

### 2.6 查询路径与索引路径的批量化对比

| 路径 | 批量化状态 | 说明 |
|------|-----------|------|
| 查询路径 (operate.py) | ✅ 已优化 | `get_nodes_batch`, `node_degrees_batch`, `get_edges_batch` 等均已批量化，并通过 `asyncio.gather` 并发 |
| 索引路径 (operate.py) | ❌ 未优化 | `_merge_nodes_then_upsert`, `_merge_edges_then_upsert` 中逐个操作 |

---

## 三、多跳查询优化

### 3.1 多跳查询的两大场景

#### 场景 A: RAG 查询路径的"隐式 1-hop 扩展"

这是系统的核心热路径。`operate.py` 中的查询流程:

```
向量检索种子实体 → 获取种子节点属性+度数 → 获取所有邻边 → 获取边属性+度数 → 排序截断
```

**关键发现**: 当前 RAG 查询只做 **1-hop 扩展**——从种子实体出发，只看直接相连的边和邻居。没有递归的多跳探索。

#### 场景 B: 知识图谱可视化的"显式多跳 BFS"

`get_knowledge_graph` API 提供的子图检索，支持 `max_depth` 参数进行真正的多跳遍历。

### 3.2 当前多跳 BFS 的问题

#### MongoDB `_bidirectional_bfs_nodes` — 递归逐层查询

- **每层 2 次查询**: depth=3 → 至少 6 次数据库往返 + 最终取边 1 次 = 7 次
- **`$or` 无索引**: 边集合上的 `$or` 查询缺少复合索引支撑
- **获取全部字段**: BFS 探索阶段获取了边的所有属性，但只需 `source_node_id` 和 `target_node_id`
- **递归而非迭代**: Python 递归有栈深度限制

#### MongoDB `$graphLookup` 模式

- **需要两次 `$graphLookup`**: 因为本质是有向遍历，无向图必须做出站+入站两轮
- **100MB 内存限制**: 每个聚合管道阶段有内存上限
- **无法在遍历中剪枝**: 不支持按 weight 等条件过滤
- **语义不精确**: 在边→边之间跳转，非经典的"节点→边→节点"图遍历

#### PostgreSQL `_bfs_subgraph` — 逐层 Cypher

- **每层 2 次 Cypher 查询**: 出站 + 入站分开，depth=3 → 6 次 Cypher 调用
- **Cypher 通过 AGE 的开销**: 每次都有 AGE 扩展的解析/计划开销
- **字符串拼接而非参数化**: 有安全风险且影响查询计划缓存

#### LanceDB `_bidirectional_bfs_nodes` — 递归逐层 WHERE

- **每层 2 次查询**: 节点查询 + 边查询
- **OR 条件全表扫描**: 无标量索引时为 O(E)
- **Python 侧聚合**: 邻居发现和去重在 Python 中完成

### 3.3 多跳查询优化方案

#### 方案 1: PostgreSQL 递归 CTE — 单查询多跳

将逐层 Python BFS 替换为一条 SQL 递归查询:

```sql
WITH RECURSIVE bfs AS (
    -- 起始节点
    SELECT b.id AS vid, 0 AS depth
    FROM {graph_name}.base b
    WHERE ag_catalog.agtype_access_operator(
        VARIADIC ARRAY[b.properties, '"entity_id"'::agtype]
    ) = $1::agtype
    
    UNION
    
    -- 逐层扩展（双向）
    SELECT CASE WHEN d.start_id = bfs.vid THEN d.end_id ELSE d.start_id END AS vid,
           bfs.depth + 1
    FROM bfs
    JOIN {graph_name}."DIRECTED" d ON d.start_id = bfs.vid OR d.end_id = bfs.vid
    WHERE bfs.depth < $2  -- max_depth
)
SELECT DISTINCT vid, depth FROM bfs LIMIT $3;  -- max_nodes
```

优势: 单次网络往返、数据库内优化、避免 Cypher 开销。

#### 方案 2: MongoDB 优化 BFS

- BFS 探索阶段只投影 ID 字段（不拉取 description 等大文本）
- BFS 完成后一次性批量获取完整属性
- 通过 `.hint()` 强制使用索引

#### 方案 3: RAG 查询路径的 2-hop 扩展

当前 RAG 查询只做 1-hop 扩展。加入可控的 2-hop 扩展可以发现间接但重要的关系:

```python
async def _find_most_related_edges_from_entities_multihop(
    node_datas, query_param, knowledge_graph_inst, max_hops=2
):
    seed_names = [dp["entity_name"] for dp in node_datas]
    
    # hop 1: 种子节点的直接邻边
    hop1_edges_dict = await knowledge_graph_inst.get_nodes_edges_batch(seed_names)
    hop1_neighbors = set()  # 收集 1-hop 邻居
    
    if max_hops >= 2 and hop1_neighbors:
        # hop 2: 按度数排序，优先扩展高连接度节点
        neighbor_degrees = await knowledge_graph_inst.node_degrees_batch(list(hop1_neighbors))
        top_neighbors = sorted(hop1_neighbors, key=lambda n: neighbor_degrees.get(n, 0), reverse=True)[:top_k]
        hop2_edges_dict = await knowledge_graph_inst.get_nodes_edges_batch(top_neighbors)
```

#### 方案 4: 内存邻接表缓存

对于节点数在 10 万以内的图，在查询初始化时一次性加载邻接结构（详见第五章）。

#### 方案 5: 合并出站/入站为单查询

PostgreSQL 可用原生 SQL 单查询获取双向邻居:

```sql
SELECT CASE WHEN d.start_id = ANY($1) THEN d.end_id ELSE d.start_id END AS neighbor_id,
       d.id, d.start_id, d.end_id, d.properties
FROM {graph_name}."DIRECTED" d
WHERE d.start_id = ANY($1) OR d.end_id = ANY($1);
```

### 3.4 多跳优化方案对比

| 方案 | 适用场景 | DB 查询次数 (depth=3) | 实施难度 | 性能提升 |
|------|----------|----------------------|----------|----------|
| 现状 (各后端 BFS) | — | 7 次 (2D+1) | — | 基准 |
| 方案1: PG 递归 CTE | PG 可视化 | **3 次** | 中 | ~2-3x |
| 方案2: MongoDB 优化 BFS | Mongo 可视化 | **D+2 次** | 低 | ~1.5x |
| 方案3: RAG 2-hop 扩展 | 所有 RAG 查询 | +2 次 | 中 | 召回率提升 |
| 方案4: 内存邻接表 | 中小图 (<100K) | **0 次** | 低 | **10-100x** |
| 方案5: 合并出入站 | 所有 BFS | **D+1 次** | 低 | ~1.5x |

---

## 四、LanceDB 图查询索引设计方案

### 4.1 查询操作 × 性能瓶颈矩阵

| 操作 | 当前实现 | 瓶颈根源 | 调用频率 |
|------|----------|----------|----------|
| `node_degree(X)` | `WHERE src=X OR tgt=X` 全扫描 + len() | 无索引，OR 条件 | 极高 |
| `get_node_edges(X)` | 同上，取 src/tgt 列 | 同上 | 极高 |
| `node_degrees_batch(N个)` | `WHERE src IN(...) OR tgt IN(...)` | 大 IN 列表 + OR | 每次 RAG 查询 |
| `get_popular_labels(limit)` | **全表扫描所有边** + Counter | 无聚合下推 | 前端展示 |
| `search_labels(query)` | `LIKE '%query%'` 全扫描 | 无文本索引 | 前端搜索 |
| `get_knowledge_graph(BFS)` | 递归逐层查边表 | 多次往返 + 无索引 | 图谱可视化 |
| `edge_degrees_batch` | 未覆盖，回退逐个查 | N+1 模式 | 每次 RAG local |

### 4.2 索引方案 1: BTree 标量索引（P0，必做）

```python
# 边表核心索引
await edge_table.create_scalar_index("source_node_id", index_type="BTREE")
await edge_table.create_scalar_index("target_node_id", index_type="BTREE")
await edge_table.create_scalar_index("_id", index_type="BTREE")

# 节点表索引
await node_table.create_scalar_index("_id", index_type="BTREE")
await node_table.create_scalar_index("entity_type", index_type="BTREE")
```

#### BTree 索引原理

LanceDB 的 BTree 是针对列存格式优化的**两级索引结构**:

```
Level 1: page_lookup.lance (常驻内存，几 MiB)
┌───────────────────────────────────────────┐
│ Page 0: min="Alice"  max="David"  idx=0   │
│ Page 1: min="Echo"   max="Harry"  idx=1   │
│ Page 2: min="Iris"   max="Mike"   idx=2   │
│ Page 3: min="Nancy"  max="Zack"   idx=3   │
└───────────────────────────────────────────┘
              │ 二分查找定位到 page_idx
              ▼
Level 2: page_data.lance (磁盘上的排序数据，按需读取)
┌───────────────────────────────────────────┐
│ Page 2: values(sorted): Iris, Jack, Mike  │
│         ids (row IDs):  201,  33,   445   │
└───────────────────────────────────────────┘
```

**查询过程** (`WHERE source_node_id = 'CompanyA'`):

1. 在 `page_lookup` (内存) 中二分查找 → 定位到 page_idx → **0 次 I/O**
2. 从磁盘读取 `page_data` 中命中的页 (4096 行/页) → **1 次 I/O**
3. 页内二分查找匹配的 row_ids → **0 次 I/O (内存操作)**
4. 用 row_ids 回原表取完整记录 → **k 次随机 I/O**

**支持的查询类型**:

| 查询类型 | 说明 | 操作 |
|---------|------|------|
| 等值 | `column = value` | BTree 定位页 → 页内查找 |
| 范围 | `column BETWEEN a AND b` | BTree 定位范围内的页 → 逐页扫描 |
| IN | `column IN (v1, v2, ...)` | 多次 BTree 查找 → 合并结果 |
| IS NULL | `column IS NULL` | 检查 null_count > 0 的页 |

**加速效果**:

| 操作 | 无索引 | 有 BTree |
|------|--------|----------|
| `has_edge(_id=X)` | O(E) | O(log E) |
| `get_node_edges(X)` | O(E) | O(log E + k) |
| `node_degree(X)` | O(E) | O(log E + k) |
| `get_nodes_batch(IN)` | O(N) | O(m × log N) |

**局限**: `WHERE (src=X) OR (tgt=X)` 需要两次索引查找 + 合并。

#### BTree vs Bitmap 选型

| 列 | 推荐索引 | 原因 |
|----|---------|------|
| `_id` | **BTree** | 高基数，每个值唯一 |
| `source_node_id` | **BTree** | 高基数，值是实体名 |
| `target_node_id` | **BTree** | 同上 |
| `entity_type` | **Bitmap** | 低基数 ("ORG", "PERSON" 等少数几种) |
| `relationship` | **Bitmap** | 关系类型通常有限 |
| `file_path` | **BTree** | 取决于文档数量，通常中等基数 |

### 4.3 索引方案 2: 双向边冗余存储（P2，消除 OR 查询）

**核心思想**: 为每条无向边存储两行，分别以两个端点为 anchor，将 OR 查询转为单列查询。

```
原始边表 (1行):
| _id               | source_node_id | target_node_id |
|--------------------|---------------|----------------|
| CompanyA||ProductX | CompanyA      | ProductX       |

双向边索引表 (2行):
| anchor_node | peer_node  | edge_id            | weight | ...属性 |
|-------------|------------|--------------------|--------|---------|
| CompanyA    | ProductX   | CompanyA||ProductX | 1.0    | ...     |
| ProductX    | CompanyA   | CompanyA||ProductX | 1.0    | ...     |
```

查询变化:

```python
# 之前: OR 查询，两次索引查找
where = f"(source_node_id = '{X}') OR (target_node_id = '{X}')"

# 之后: 单列精确查询，一次索引命中
where = f"anchor_node = '{X}'"
```

**代价**: 存储空间翻倍，写入时需维护 2 行。

### 4.4 索引方案 3: 节点度数物化列（P1，消除度数边表扫描）

**核心思想**: 在节点表新增 `degree` 列，在 `index_done_callback()` 中批量计算。

```python
async def index_done_callback(self):
    all_edges = await self._edge_table.query() \
        .select(["source_node_id", "target_node_id"]).to_list()
    counter = Counter()
    for e in all_edges:
        counter[e["source_node_id"]] += 1
        counter[e["target_node_id"]] += 1
    # 批量更新节点表的 degree 列
    for node_id, degree in counter.items():
        await self._node_table.update(
            where=f"_id = '{node_id}'", values={"degree": degree})
```

**加速效果**:

| 操作 | 之前 | 之后 |
|------|------|------|
| `node_degree(X)` | 查边表 O(log E + k) | **读节点表 O(log N)** |
| `node_degrees_batch(N个)` | 查边表 + Counter | **IN 查节点表** |
| `get_popular_labels(limit)` | **全量扫描所有边** | **按 degree 排序取 top-k** |

### 4.5 索引方案 4: FTS 全文索引（P1，加速标签搜索）

```python
from lancedb.index import FTS

await self._node_table.create_index("_id", config=FTS())
await self._node_table.create_index("description", config=FTS())
```

`search_labels`: 从 O(N) 全表 LIKE 扫描 → O(log N) 倒排索引查找 + BM25 评分。

### 4.6 索引方案 5: edge_degrees_batch 覆盖优化（P0，纯逻辑优化）

无需新索引，利用已有 `node_degrees_batch` 实现批量版本，将 2N 次查询降为 1 次。（见 2.4 节）

### 4.7 索引方案 6: file_path 反向索引（P2，加速文档删除）

```python
await edge_table.create_scalar_index("file_path", index_type="BTREE")
await node_table.create_scalar_index("file_path", index_type="BTREE")
```

加速文档删除时的级联清理: `WHERE file_path = 'xxx'`。

### 4.8 方案优先级总结

```
优先级   索引方案                 查询改进              成本          风险
─────────────────────────────────────────────────────────────────────
 P0     ① BTree 标量索引         所有 WHERE 查询        极低(几行代码)  无
 P0     ⑤ edge_degrees_batch    2N→1 次查询            极低(纯逻辑)   无
 P1     ③ 度数物化列(批量重建)   度数查询 O(1)          低(回调中重建)  低
                                 popular_labels 0边扫描
 P1     ④ FTS 全文索引           search_labels 加速     低(几行代码)   中(中文分词)
 P2     ② 双向边索引表           消除所有 OR 查询       中(新表+同步)  低
 P2     ⑥ file_path 索引         文档删除加速           极低           无
```

---

## 五、邻接索引方案深度分析

### 5.1 方案概述

新增一张邻接表（或内存结构），每行代表一个节点，预先存储该节点所有出边/入边的信息，从而将图遍历从"基于 WHERE 条件的过滤扫描"转变为"直接查找"。

### 5.2 I/O 模型对比

#### BTree 索引的 I/O 模型

以 `get_node_edges("CompanyA")` 为例:

```
① page_lookup 二分查找 (内存缓存)                    0 次 I/O
② 读取 page_data 中命中的页                          1 次 I/O
③ 页内找到匹配的 row_ids                             0 (内存)
④ 还需查 target_node_id 索引 (OR 条件另一半)          1 次 I/O
⑤ 合并两组 row_ids                                   0 (内存)
⑥ 用 row_ids 回边表取完整记录: take(row_ids)          k 次随机 I/O
                                                    ─────────
                                        总计: 2 + k 次磁盘 I/O
```

**关键瓶颈**: 步骤⑥的 k 次随机 I/O，因为边表的物理排列不按某个节点的邻边聚集。

#### 邻接索引（磁盘）的 I/O 模型

```
① 按 node_id 查邻接表 (1行)                          1 次 I/O
② 得到 neighbors 和 edge_ids                         0 (已在①中)
                                                    ─────────
                                        总计: 1 次磁盘 I/O
```

**1 次 I/O 得到全部拓扑信息，与边表大小无关。**

#### 邻接索引（内存）的 I/O 模型

```
① 内存哈希表查找                                      0 次 I/O
② 得到全部邻居和边 ID                                  0
                                                    ─────────
                                        总计: 0 次磁盘 I/O
```

### 5.3 拓扑探索 vs 属性获取的分离

图查询有两个阶段，邻接索引天然实现了它们的分离:

```
阶段 1: 拓扑探索 (BFS 发现可达节点)
  → 只需要知道"谁连着谁"
  → 邻接索引完美覆盖，0 次边表 I/O

阶段 2: 属性获取 (对最终结果集取详细数据)
  → 只对筛选后的节点/边取完整属性
  → 经过 max_nodes 截断后，通常远小于遍历到的总量
```

BTree 的问题在于它无法分离这两个阶段——每次查边索引都会回边表取完整记录，即使 BFS 阶段只需要端点 ID。

**定量对比** (depth=3, 平均度数 6, 遍历 186 节点, max_nodes=50):

| 阶段 | BTree | 邻接索引-内存 |
|------|-------|-------------|
| 拓扑探索 (186 个节点) | **~192 次 I/O** | **0 次 I/O** |
| 属性获取 (50 个结果) | 0 (探索时已取) | ~50 次 I/O (batch) |
| **总计** | **~192 次** | **~50 次** |

### 5.4 邻接索引的 row_id 稳定性问题

如果邻接表存储的是边表的物理 row_id，会面临**严重的稳定性问题**:

- **`merge_insert` (upsert)**: 标记旧行删除 + 追加新行，row_id 变化
- **表 compaction**: 重新组织数据文件，所有 row_id 重新分配
- **delete 操作**: 后续 compaction 导致其他行 row_id 变化

**结论: 不建议在邻接表中存储物理 row_id。**

### 5.5 可行的实施路径

#### 方案 A: 存储逻辑 edge_id（推荐）

```
| node_id    | neighbor_ids (list<str>)  | edge_ids (list<str>)            |
|------------|---------------------------|---------------------------------|
| "CompanyA" | ["ProductX", "PersonB"]   | ["CompanyA||ProductX", ...]     |
```

- 完全回避 row_id 稳定性问题
- 度数查询: `len(neighbor_ids)` → O(1)
- 邻居发现: 直接从 `neighbor_ids` 获取，零数据库查询
- 取边属性: `WHERE _id IN (edge_ids)` → 在主键上的 IN 查询

#### 方案 B: 存储物理 row_id + 批量重建（不推荐）

在 `index_done_callback()` 时全量重建。row_id 在两次重建之间稳定，但如果外部触发 compaction 则即刻失效。

#### 方案 C: 节点表内联（可选）

不新增表，在节点表中增加 `degree` 和 `neighbor_ids` 字段。工程更简洁，但列表更新需要读取-修改-写回整行。

### 5.6 推荐方案: 内存邻接索引

对于 LightRAG 的典型规模（几千到几万实体），最务实的方案是直接在 Python 进程中维护内存字典:

```python
class LanceDBGraphStorage(BaseGraphStorage):
    _adj_cache: dict[str, set[str]] = {}      # node_id → {neighbor_ids}
    _edge_id_cache: dict[str, set[str]] = {}  # node_id → {edge_ids}
    _cache_loaded: bool = False

    async def _ensure_adj_cache(self):
        """懒加载: 首次查询时从边表一次性构建"""
        if self._cache_loaded:
            return
        all_edges = await self._edge_table.query() \
            .select(["source_node_id", "target_node_id", "_id"]).to_list()
        for e in all_edges:
            src, tgt, eid = e["source_node_id"], e["target_node_id"], e["_id"]
            self._adj_cache.setdefault(src, set()).add(tgt)
            self._adj_cache.setdefault(tgt, set()).add(src)
            self._edge_id_cache.setdefault(src, set()).add(eid)
            self._edge_id_cache.setdefault(tgt, set()).add(eid)
        self._cache_loaded = True

    async def node_degree(self, node_id: str) -> int:
        await self._ensure_adj_cache()
        return len(self._adj_cache.get(node_id, set()))  # 0 次 I/O

    async def get_node_edges(self, node_id: str):
        await self._ensure_adj_cache()
        # 0 次 I/O，直接从内存返回
        neighbors = self._adj_cache.get(node_id, set())
        return [(node_id, n) for n in neighbors]

    async def index_done_callback(self):
        """索引完成后刷新缓存"""
        self._cache_loaded = False
        self._adj_cache.clear()
        self._edge_id_cache.clear()
```

**内存占用评估**:

| 图规模 | 内存占用 | 适用性 |
|--------|---------|--------|
| 10K 节点 (avg degree 6) | ~4 MB | 完全可以常驻内存 |
| 100K 节点 | ~40 MB | 可以接受 |
| 1M 节点 | ~400 MB | 需要评估，对服务器仍可行 |

### 5.7 邻接索引与 BTree 索引的关系

**两者不是替代关系而是互补**:

- **BTree 索引**: 加速属性获取阶段的 WHERE IN 查询
- **邻接索引**: 消除拓扑探索阶段的 I/O

组合使用可达到最优效果。

---

## 六、综合优化效果预估

假设 10K 节点、30K 边的图:

| 操作 | 当前 (无索引) | +BTree | +BTree+度数物化 | +全部方案 |
|------|-------------|--------|----------------|----------|
| `node_degree(1个)` | ~15ms | ~2ms | **~0.3ms** | ~0.3ms |
| `node_degrees_batch(100个)` | ~20ms | ~5ms | **~1ms** | ~1ms |
| `get_node_edges(1个)` | ~15ms | ~2ms | ~2ms | **~0.01ms** (内存) |
| `get_popular_labels(300)` | **~50ms** | ~50ms | **~3ms** | ~3ms |
| `search_labels("abc")` | ~30ms | ~30ms | ~30ms | **~1ms** (FTS) |
| BFS depth=3 | ~90ms | ~12ms | ~10ms | **~0.1ms** (内存) |
| **RAG local 查询总图开销** | **~120ms** | **~25ms** | **~12ms** | **~6ms** |

---

## 七、实施路线图

```
Phase 2 (Week 1)           Phase 3 (Week 2)          Phase 4 (Week 3)
┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐
│ BTree 标量索引      │   │ 度数物化列          │   │ 内存邻接索引       │
│ edge_degrees_batch │──>│ FTS 全文索引        │──>│ 双向边索引表(可选) │
│ 冗余查询消除       │   │ file_path 索引      │   │ 2-hop RAG 扩展     │
│ N+1 查询修复       │   │ BFS 投影优化        │   │ 性能基准测试       │
└────────────────────┘   └────────────────────┘   └────────────────────┘
      P0 高优先级              P1 中优先级              P2 可选优化
```

---

## 八、参考资料

- [LanceDB 标量索引文档](https://lancedb.com/docs/indexing/scalar-index/)
- [Lance BTree 格式规范](https://lance.org/format/table/index/scalar/btree/)
- [Lance Bitmap 格式规范](https://lance.org/format/table/index/scalar/bitmap/)
- [LanceDB FTS 索引文档](https://lancedb.com/docs/indexing/fts-index/)
- [Lance 随机访问性能基准](https://blog.lancedb.com/benchmarking-random-access-in-lance/)
- [LightRAG 查询流程分析](./operate_py_analysis.md)
- [LightRAG 项目结构](./ProjectStructure.md)
- [LanceDB 优化计划书](./lancedb_optimization_plan.md)
