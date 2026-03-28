# 阶段 3.5 计划：冷 / 热缓存基准测试

## 目标

扩展阶段 3 的基准测试框架，使局部性与聚类带来的收益可以分别在以下两种条件下评估：

- 冷缓存条件
- 热缓存条件

本阶段的重点是让基准结果更容易解释，尤其关注以下指标：

- 延迟（`avg_time_ms`、`p50`、`p95`）
- 吞吐（`throughput_qps`）
- 磁盘 IO（`avg_read_bytes`、`total_read_bytes`）
- 局部性指标

## 背景与动机

当前的基准执行流程会把首次触盘读取和后续缓存命中混在一起，这会导致很难判断性能收益究竟来自：

- 聚类带来的物理 IO 减少
- 内存局部性与执行调度的改善

阶段 3.5 的目标就是把这两类效应尽量拆分开。

## 范围

### 范围内

- 为局部性基准增加显式缓存模式：
  - `cold`
  - `warm`
  - `mixed`（默认，保持向后兼容）
- 为热缓存基准增加预热支持
- 为冷缓存基准增加可选的清缓存钩子
- 保留固定样本集的测试方式，以保证 clustered / unclustered 对比公平
- 扩展 runner 脚本，使其支持执行冷 / 热两类基准流程
- 在基准输出中暴露缓存相关元数据

### 范围外

- 严格保证硬件层面的绝对冷启动语义
- 超出外部命令钩子范围的内核级缓存管理
- 第一版就支持“每个 query 都强制清缓存”
- 一次性集成到仓库中所有 benchmark 脚本

## 基准模式

### 1. 冷缓存基准

目的：

- 测量首次访问延迟
- 测量真实磁盘读取行为
- 测量冷 IO 条件下聚类布局的收益

执行模型：

1. 在每个 benchmark case 开始前，尝试清理缓存
2. 执行该 case
3. 记录延迟、吞吐、读取字节数和局部性指标

说明：

- 清缓存通常需要更高权限
- 如果无法清缓存，benchmark 仍应继续执行，并在输出中明确报告这一点

### 2. 热缓存基准

目的：

- 测量稳定态延迟
- 测量内存局部性带来的收益
- 测量数据已经进入缓存后，cluster-aware 执行的收益

执行模型：

1. 固定一组采样节点
2. 先执行预热查询
3. 预热结果不计入最终统计
4. 再执行正式计量查询

### 3. 混合模式

目的：

- 保留当前行为，保证向后兼容

执行模型：

- 不强制清缓存
- 不强制要求预热
- 保持现有 repeat 循环逻辑不变

## 采样策略

默认策略：

- 只采样一次节点 ID
- 并在以下维度上复用同一批样本：
  - clustered / unclustered
  - single-hop / k-hop
  - repeat

原因：

- 对比更公平
- 方差更低
- 结果更容易解释

未来可选扩展：

- 每次 repeat 重新采样，以模拟更随机的工作负载

## 实施计划

### A. 扩展 `benchmark_cluster_locality.py`

新增参数：

- `--cache-mode cold|warm|mixed`
- `--warmup-runs N`
- `--drop-cache-command "..."`
- `--resample-per-repeat`（可选，默认关闭）

行为定义：

- `cold`：每个 benchmark case 前尝试清缓存
- `warm`：先预热，再统计正式结果
- `mixed`：保持当前行为

### B. 增加缓存工具辅助模块

新增一个用于缓存控制与状态报告的辅助模块。

职责：

- 执行可选的清缓存命令
- 记录是否支持清缓存、是否执行成功
- 为 benchmark 输出提供结构化缓存元数据

### C. 扩展 shell runner

更新 `scripts/run_lancedb_graph_phase3_benchmarks.sh`，使其支持：

- 执行热缓存 benchmark
- 执行冷缓存 benchmark
- 配置缓存模式和 warmup 次数

## 输出要求

每个 benchmark 结果都应包含：

- `cache_mode`
- `warmup_runs`
- `sample_strategy`
- `cache_drop_supported`
- `cache_drop_success`
- `cache_drop_error`（如有）
- `avg_time_ms`
- `p50_time_ms`
- `p95_time_ms`
- `throughput_qps`
- `avg_read_bytes`
- `total_read_bytes`
- 局部性指标

## 预期结论

### 冷缓存条件下

更容易观察到：

- 存储布局带来的收益
- 聚类减少磁盘读取的效果
- clustered 与 unclustered 之间更明显的差距

### 热缓存条件下

更容易观察到：

- 执行侧局部性收益
- 较小但仍可见的 clustered 优势
- 更弱的磁盘读取信号

## 风险

1. 清缓存可能需要 root 权限。
2. 操作系统层面的清缓存不一定会清掉所有库内部缓存。
3. 冷缓存与热缓存下的 repeat 语义不同，必须在结果中明确标注。

## 验收标准

当满足以下条件时，可认为阶段 3.5 完成：

1. `benchmark_cluster_locality.py` 支持 `cold`、`warm`、`mixed`
2. `warm` 模式支持预热，并且预热不计入正式统计
3. `cold` 模式会报告清缓存是否成功
4. benchmark 输出中清楚标注缓存模式与缓存控制状态
5. runner 脚本可以直接发起冷 / 热局部性 benchmark
6. 实现经过基础执行或测试验证

## 推荐首批交付项

1. 为局部性 benchmark 增加缓存模式
2. 增加缓存工具辅助模块
3. 更新 runner 脚本
4. 分别运行一次 warm 和 mixed/cold 的验证 benchmark

## 当前状态

- 阶段 3：community clustering + execution-side frontier reorder 已完成
- 阶段 3.5：面向冷 / 热缓存分析的 benchmark 方法学细化已启动
