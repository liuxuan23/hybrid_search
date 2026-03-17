# LanceDB 图存储实验

本目录用于实现和评估基于 `LanceDB` 的图存储方案，基础数据模型固定为：

- 节点表 `nodes`
- 边表 `edges`

当前正在进行阶段一，目标是搭建最小可运行闭环：

- 从原始图数据生成节点表和边表
- 将两张表写入 LanceDB
- 提供基础单跳和 `k-hop` 查询
- 用 smoke benchmark 验证链路可用

## 当前目录

```text
experiments/lancedb_graph/
├── EXECUTION_PLAN.md
├── PHASE1_PLAN.md
├── README.md
├── config.py
├── data_prep/
├── storage_models/
├── query_engines/
├── benchmarks/
└── utils/
```

## 基础 Schema

节点表 `nodes`：

- `node_id`
- `node_type`
- `degree_out`
- `degree_in`
- `attrs_json`

边表 `edges`：

- `edge_id`
- `src_id`
- `dst_id`
- `edge_type`
- `src_type`
- `dst_type`
- `attrs_json`

## 数据来源

阶段一优先复用项目现有的图数据输入，默认从 `hybrid_search/data/huggingkg_tiny/triples.tsv` 读取。

## 计划中的首批脚本

- `data_prep/generate_synthetic_graph.py`: 生成可控分布的图 `.tsv` 数据
- `data_prep/build_graph_tables.py`: 构建节点表和边表
- `data_prep/sample_graph.tsv`: 本地验证用小样本数据
- `storage_models/lancedb_graph_basic.py`: 基础 LanceDB 图存储实现
- `query_engines/basic_queries.py`: 基础查询逻辑
- `benchmarks/smoke_benchmark.py`: 最小可运行验证脚本
- `benchmarks/local_validation.py`: 基于假数据的小样本本地验证脚本

## 当前状态

阶段一骨架已开始落地，后续将逐步补全数据构建、查询和 smoke benchmark。

## 小样本验证

如果当前没有真实 `triples.tsv` 数据，可以先使用小样本脚本验证阶段一链路：

```bash
cd /home/liuxuan/workplace/hybrid_search
python -m experiments.lancedb_graph.benchmarks.local_validation
```

## 可控图生成

如果现有 `triples.tsv` 不适合后续实验，可以先生成可控分布的图数据：

```bash
cd /home/liuxuan/workplace/hybrid_search
python -m experiments.lancedb_graph.data_prep.generate_synthetic_graph --graph-mode powerlaw --num-nodes 1000 --num-edges 5000
```

生成说明见 [DATA_GENERATION.md](/home/liuxuan/workplace/hybrid_search/experiments/lancedb_graph/DATA_GENERATION.md)。
