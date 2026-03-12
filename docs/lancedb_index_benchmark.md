# LanceDB 索引基准测试报告（BTree / Bitmap / take_row_ids）

本报告整理了在 LightRAG 图数据（node/edge/adj 表）上，对 LanceDB 标量索引（BTree、Bitmap）与 `take_row_ids` 的性能对比结果。

## 环境

- Python: 3.10.12
- LanceDB: 0.29.2
- PyArrow: 23.0.1
- Pytest: 9.0.2

## 测试脚本

- 基准脚本：`tests/test_lancedb_index_benchmark.py`
- 运行方式（预构建 DB）：

```bash
PERF_DB_DIR=/data/lightrag/_prebuilt_db \
  python -m pytest tests/test_lancedb_index_benchmark.py -v -s -k "TestPrebuilt or TestTake"
```

说明：测试会在同一张表上依次执行 **NoIndex → BTree → Bitmap** 的索引创建/删除，并对同一条 WHERE 查询多次执行，取最小耗时。

## 数据规模

测试使用两套预构建库：

- **100K 规模**：`tests/_prebuilt_db_100k`
  - `n_nodes=100000`
  - `n_extra_edges=300000`
- **1M 规模**：`/data/lightrag/_prebuilt_db`
  - `n_nodes=1000000`
  - `n_extra_edges=6000000`

表规模（1M 库中）：

- node 表：约 1,000,000 行
- edge 表：约 7,000,000 行（链式边 + 随机额外边）
- adj 表：约 14,000,000 行（每条边双向两行）

## 结果汇总（1M 规模）

> 单位：ms（越小越好）；括号内为相对 NoIndex 的加速比。

### Edge 表（~7M 行）

| 查询 | NoIndex | BTree | Bitmap |
|---|---:|---:|---:|
| `edge.source_node_id WHERE IN(50)` | 735.7 | 132.0 (5.6x) | 135.0 (5.4x) |
| `edge.target_node_id WHERE IN(50)` | 789.3 | 142.7 (5.5x) | 141.2 (5.6x) |
| `edge._id WHERE IN(50)` | 722.4 | 14.6 (49.6x) | 7.3 (99.6x) |
| `edge._id = '...'` (single EQ) | 399.9 | 7.7 (52.0x) | 8.9 (44.9x) |
| `edge.source_node_id = '...'` (single EQ) | 418.9 | 11.1 (37.7x) | 10.4 (40.4x) |

### Node 表（~1M 行）

| 查询 | NoIndex | BTree | Bitmap |
|---|---:|---:|---:|
| `node._id WHERE IN(50)` | 104.3 | 12.9 (8.1x) | 5.7 (18.3x) |
| `node._id = '...'` (single EQ) | 54.3 | 4.8 (11.2x) | 5.6 (9.7x) |
| `node._id WHERE IN(500)` | 124.6 | 78.5 (1.6x) | 10.6 (11.8x) |

### Adj 表（~14M 行）

| 查询 | NoIndex | BTree | Bitmap |
|---|---:|---:|---:|
| `adj.entity_id WHERE IN(50)` | 1057.8 | 95.9 (11.0x) | 84.1 (12.6x) |
| `adj.entity_id = '...'` (single EQ) | 363.7 | 11.0 (33.1x) | 10.2 (35.6x) |
| `adj.entity_id WHERE IN(200)` | 1144.0 | 361.7 (3.2x) | 280.3 (4.1x) |

### `take_row_ids` vs `WHERE IN`（Edge 表，参考上限）

| 查询 | take_row_ids | WHERE IN（NoIndex） |
|---|---:|---:|
| 连续 50 行 | 7.3 | 728.2 |
| 连续 200 行 | 10.1 | 894.9 |
| 分散 50 行 | 78.3 | 1052.8 |

结论：`take_row_ids` 在“连续/局部” rowid 访问时极快，但 rowid 分散时会显著退化。

## 结果对比（100K 规模，摘要）

在 100K 库上（`tests/_prebuilt_db_100k`），索引收益整体更弱：

- 单值查询通常只有 ~1x（几乎无差别）
- 小规模 `WHERE IN(50~100)` 多值查询通常 2–5x

而在 1M/7M 规模下，索引收益非常明显（最高接近 100x）。

## 结论与建议

1. **数据规模是决定性因素**
   - 100K 规模下索引收益不稳定、可能接近 1x。
   - 1M/千万级行数下索引收益巨大（5x–100x）。

2. **Bitmap 在多值过滤上更有优势**
   - 多值过滤（`WHERE IN(...)`）下，Bitmap 经常优于 BTree。
   - 大 IN 列表（例如 `IN(500)`）下，BTree 明显退化，Bitmap 仍有明显优势。

3. **单值等值查询（`col = 'x'`）两者差距不大**
   - BTree 往往略优或相近；Bitmap 也可接近同等水平。

4. **`take_row_ids` 是极限快路径，但依赖 rowid 局部性**
   - 如果能在图遍历中保持 rowid 连续/局部（例如同一 fragment/相近 offset），性能非常好。
   - rowid 分散时，`take_row_ids` 的优势会缩小，但通常仍优于无索引 WHERE。

## 复现提示

- 1M 规模基准耗时较长（约 10 分钟级别），属于正常现象。
- 如需更激进的压测：可调大 `PERF_IDX_IN_SIZE`，例如 200/500/1000：

```bash
PERF_DB_DIR=/data/lightrag/_prebuilt_db \
  PERF_IDX_REPEATS=5 PERF_IDX_IN_SIZE=200 \
  python -m pytest tests/test_lancedb_index_benchmark.py -v -s -k "TestPrebuilt"
```
