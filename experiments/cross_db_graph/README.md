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
