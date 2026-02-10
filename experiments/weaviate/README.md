# Weaviate 混合查询测试

## 📋 概述

本项目包含完整的 Weaviate 混合查询测试环境，支持：
- 向量搜索（语义搜索）
- 关键词搜索（BM25）
- 混合搜索（结合向量和关键词）

## 🚀 快速开始

### 1. 启动 Weaviate 服务

```bash
cd /home/liuxuan/LanceTest/Lance/hybrid_search/test_weaviate
docker compose up -d
```

等待服务启动完成（约 30 秒）。

### 2. 导入数据

```bash
python import_json_data.py
```

这个脚本会：
- 自动创建 Schema（Author 和 Document）
- 从 `authors.json` 导入 50 个作者
- 从 `documents.json` 导入 1000 个文档
- 验证导入结果

**预期输出：**
```
✓ 所有数据导入成功！
  - Author: 50 个
  - Document: 1000 个
```

### 3. 运行混合查询测试

```bash
python test_hybrid_search.py
```

这个脚本会测试：
- 纯向量搜索
- 纯关键词搜索
- 混合搜索（alpha=0.5）
- 混合搜索（alpha=0.7，偏向向量）
- 带过滤条件的混合搜索

## 📁 文件说明

### 核心文件

- **`import_json_data.py`** - 数据导入脚本（推荐使用）
  - 从 JSON 文件导入数据到 Weaviate
  - 自动创建 Schema
  - 包含验证步骤

- **`test_hybrid_search.py`** - 完整的混合查询测试脚本
  - 对比不同搜索方法
  - 性能测试
  - 结果展示

- **`quick_hybrid_test.py`** - 快速测试脚本
  - 自动导入少量测试数据
  - 快速验证功能

### 数据文件

- **`authors.json`** - 作者数据（50 个）
- **`documents.json`** - 文档数据（1000 个）
- **`create_dataset.py`** - 数据生成脚本（可选）

### 配置文件

- **`docker-compose.yml`** - Weaviate 服务配置
  - Weaviate 1.32.2
  - text2vec-model2vec 向量化模型

## 🔍 查询示例

### 1. 纯向量搜索

```python
query = {
    "query": """
    {
        Get {
            Document(
                nearText: {
                    concepts: ["AI chip"]
                }
                limit: 5
            ) {
                title
                content
                _additional {
                    distance
                    certainty
                }
            }
        }
    }
    """
}
```

### 2. 纯关键词搜索

```python
query = {
    "query": """
    {
        Get {
            Document(
                bm25: {
                    query: "AI chip"
                }
                limit: 5
            ) {
                title
                content
                _additional {
                    score
                }
            }
        }
    }
    """
}
```

### 3. 混合搜索

```python
query = {
    "query": """
    {
        Get {
            Document(
                hybrid: {
                    query: "AI chip"
                    alpha: 0.5
                }
                limit: 5
            ) {
                title
                content
                _additional {
                    score
                }
            }
        }
    }
    """
}
```

### 4. 带过滤条件的混合搜索

```python
query = {
    "query": """
    {
        Get {
            Document(
                hybrid: {
                    query: "AI chip"
                    alpha: 0.5
                }
                where: {
                    path: ["category"]
                    operator: Equal
                    valueString: "Hardware"
                }
                limit: 5
            ) {
                title
                content
                category
            }
        }
    }
    """
}
```

## 📊 性能对比

根据测试结果（1000 个文档）：

| 搜索方法 | 平均耗时 | 特点 |
|---------|---------|------|
| 关键词搜索 | ~5 ms | 最快，精确匹配 |
| 向量搜索 | ~4 ms | 语义理解，找到相关结果 |
| 混合搜索 (α=0.5) | ~5 ms | 平衡，综合效果最好 |
| 混合搜索 (α=0.7) | ~5 ms | 偏向语义理解 |

## 🔧 故障排除

### 问题：数据导入失败

**检查步骤：**
1. 确认 Weaviate 服务运行：`docker compose ps`
2. 检查服务日志：`docker compose logs weaviate`
3. 验证 JSON 文件格式是否正确

**解决方案：**
```bash
# 重新导入
python import_json_data.py
```

### 问题：查询返回空结果

**可能原因：**
1. 数据未正确导入
2. 向量索引还在构建中

**解决方案：**
```bash
# 检查数据量
curl http://localhost:8080/v1/graphql -X POST -H "Content-Type: application/json" \
  -d '{"query": "{ Aggregate { Document { meta { count } } } }"}'

# 等待几秒后重试查询
```

### 问题：向量搜索不工作

**检查步骤：**
1. 确认 text2vec-model2vec 服务运行：`docker compose ps`
2. 检查 Schema 配置中的 vectorizer

**解决方案：**
```bash
# 重启服务
docker compose restart
```

## 📈 使用建议

1. **首次使用**：运行 `import_json_data.py` 导入数据
2. **快速测试**：使用 `quick_hybrid_test.py` 快速验证
3. **完整测试**：使用 `test_hybrid_search.py` 进行完整测试
4. **调整参数**：根据数据特点调整 `alpha` 值（0.0-1.0）

## 🎯 Alpha 参数说明

- **alpha = 0.0**: 纯关键词搜索
- **alpha = 0.3-0.4**: 偏向关键词搜索
- **alpha = 0.5**: 平衡混合（推荐）
- **alpha = 0.6-0.7**: 偏向向量搜索
- **alpha = 1.0**: 纯向量搜索

## 📚 更多资源

- [Weaviate 官方文档](https://weaviate.io/developers/weaviate)
- [GraphQL API 参考](https://weaviate.io/developers/weaviate/api/graphql)
- [混合搜索指南](https://weaviate.io/developers/weaviate/search/hybrid)

## ✅ 验证清单

- [x] Weaviate 服务启动成功
- [x] 数据导入成功（50 个作者，1000 个文档）
- [x] 向量搜索正常工作
- [x] 关键词搜索正常工作
- [x] 混合搜索正常工作
- [x] 带过滤条件的查询正常工作

---

**最后更新：** 2025-11-23
**状态：** ✅ 所有功能测试通过

