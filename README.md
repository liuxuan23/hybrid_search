# Hybrid Search 性能测试项目

LanceDB 和 Neo4j 在知识图谱查询场景下的性能对比测试项目。

## 项目结构

```
hybrid_search/
├── pyproject.toml          # 项目配置和依赖管理
├── README.md               # 项目说明文档
├── .gitignore             # Git 忽略文件
├── query_lancedb.py        # LanceDB 查询工具（测试用）
│
├── scripts/                # 数据导入和工具脚本
│   ├── import_lance.py     # 导入数据到 LanceDB
│   ├── import_neo4j.py     # 导入数据到 Neo4j
│   ├── export_triples.py   # 导出三元组数据
│   ├── download_data.py    # 下载 HuggingFace 数据集
│   └── clear_neo4j.sh      # 清空 Neo4j 数据库
│
├── benchmarks/             # 性能测试脚本
│   ├── lancedb_vs_neo4j.py          # LanceDB vs Neo4j 性能对比
│   ├── write_performance.py         # 写入性能测试
│   ├── query_performance.py         # 查询性能测试
│   ├── update_performance.py        # 更新性能测试
│   ├── multi_hop_query.py           # 多跳查询测试
│   ├── scheme2_hop_analysis.py      # 方案二多跳分析
│   └── analyze_node_hop_distribution.py  # 节点跳数分布分析
│
├── experiments/            # 实验性代码
│   └── weaviate/           # Weaviate 相关实验
│
├── data/                   # 数据文件
│   └── huggingkg_tiny/     # HuggingKG 数据集
│
└── storage/                # 数据库存储目录
    └── lance/              # LanceDB 数据库文件
```

## 环境设置

### 使用 uv 管理依赖（推荐）

```bash
# 安装依赖
uv sync

# 运行脚本
uv run python scripts/import_lance.py
uv run python benchmarks/lancedb_vs_neo4j.py
```

### 使用传统方式

```bash
# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# 或 .venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt
# 或使用 uv
uv pip install -r requirements.txt
```

## 主要功能

### 数据导入

- `scripts/import_lance.py` - 将 TSV 文件导入 LanceDB
- `scripts/import_neo4j.py` - 将 TSV 文件导入 Neo4j
- `scripts/export_triples.py` - 导出三元组数据为 TSV 格式
- `scripts/download_data.py` - 从 HuggingFace 下载数据集

### 性能测试

- `benchmarks/lancedb_vs_neo4j.py` - LanceDB 和 Neo4j 性能对比
- `benchmarks/write_performance.py` - 写入性能测试
- `benchmarks/query_performance.py` - 查询性能测试
- `benchmarks/update_performance.py` - 更新性能测试
- `benchmarks/multi_hop_query.py` - 多跳查询测试
- `benchmarks/scheme2_hop_analysis.py` - 方案二多跳分析

### 查询工具

- `query_lancedb.py` - LanceDB 查询工具函数（供 benchmark 脚本使用）

## 依赖

- `lancedb>=0.5.0` - LanceDB 数据库
- `pandas>=2.0.0` - 数据处理
- `pyarrow>=10.0.0` - Arrow 数据格式
- `neo4j>=5.0.0` - Neo4j 数据库驱动
- `datasets>=2.0.0` - HuggingFace datasets
- `tqdm>=4.60.0` - 进度条

## 使用示例

```bash
# 1. 下载数据集
uv run python scripts/download_data.py

# 2. 导出三元组数据
uv run python scripts/export_triples.py

# 3. 导入到 LanceDB
uv run python scripts/import_lance.py

# 4. 导入到 Neo4j（需要先启动 Neo4j 服务）
uv run python scripts/import_neo4j.py

# 5. 运行性能测试
uv run python benchmarks/lancedb_vs_neo4j.py
```

## 注意事项

- **Neo4j**: 需要单独安装和启动 Neo4j 服务
- **数据库存储**: 数据库文件存储在 `storage/` 目录下
- **数据文件**: 大文件数据已添加到 `.gitignore`，不会提交到版本控制

## 开发

```bash
# 安装开发依赖
uv sync --extra dev

# 代码格式化
uv run black .

# 代码检查
uv run ruff check .
```

## 项目状态

本项目主要用于测试和性能对比，代码结构以测试脚本为主。
