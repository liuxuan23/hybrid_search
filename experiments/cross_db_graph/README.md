# Cross-DB Graph Benchmark

最小可行的跨数据库图查询对比框架。

当前目标：

- 在同一数据集上对比 `LanceDB`、`PostgreSQL`、`ArangoDB`
- 只关注图查询 workload
- 支持 `1-hop`、`2-hop`、`3-hop`、`batch 1-hop`

后续将逐步补充：

- 数据导入脚本
- 一致性校验
- 统一结果汇总

当前已具备：

- `LanceDB` adapter
- `lance_graph` adapter（phase 1）
- `PostgreSQL` adapter
- `ArangoDB` adapter
- `import_postgres.py`
- `import_arangodb.py`

补充文档：

- `LANCE_GRAPH_BENCHMARK_PLAN.md`：`lance_graph` 与 adjacency 路径的大图 benchmark 设计与分阶段实施计划
