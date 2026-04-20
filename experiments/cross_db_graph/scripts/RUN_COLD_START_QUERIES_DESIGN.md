# `run_cold_start_queries.py` 实现设计

本文档给出基于当前仓库的 `experiments/cross_db_graph/scripts/run_cold_start_queries.py` 实现方案。

目标不是做一个高度通用的 cold-start 框架，而是为当前实验固定出一套**可复现、可落地、便于解释结果**的执行流程。

---

## 1. 设计目标

新脚本职责固定为：

1. 从 `seeds.json` 读取待测 seed
2. 每次只选取一个 seed
3. 对该 seed 的每一个 query 单独执行一次冷启动准备动作
4. 每一个 query 都单独调用一次现有 `run_single_seed_queries.py`
5. 从子进程返回结果中只提取当前目标 query 的结果
6. 将多次运行结果落盘并汇总

该脚本本质上是一个**per-query cold-start 编排器**，而不是新的 benchmark runner。

---

## 2. 当前仓库约束

当前仓库已经有：

- `experiments/cross_db_graph/scripts/run_single_seed_queries.py`
  - 支持单个 seed 的三类查询
  - 支持 `--engine`、`--seed`、`--direction`、`--materialize`、`--json`、`--db-path`
- `experiments/cross_db_graph/scripts/clear_service_caches.sh`
  - 支持 PostgreSQL / ArangoDB 服务重启
  - 提供 Linux page cache 清理提示

因此新脚本不需要重复实现 query 逻辑，只需要：

- 负责 seed 选择
- 负责冷启动动作
- 负责子进程调用与结果收集

---

## 3. 简化后的核心设计原则

根据新的要求，**不再支持复杂可组合的 cold mode**，而是把每个引擎的行为固定下来。

### 3.1 固定冷启动策略

- `lancedb`
  - 只做：**清理 OS page cache**
- `lance_graph`
  - 只做：**清理 OS page cache**
- `postgres`
  - 做：**重启服务 + 清理 OS page cache**
- `arangodb`
  - 做：**重启服务 + 清理 OS page cache**

这样做的好处：

1. 结果定义更稳定
2. CLI 更简单
3. 不需要讨论太多 mode 组合语义
4. 与当前实验目标一致

### 3.2 计时口径固定

这是新的关键要求。

**统计查询时间时，不包含前面的冷启动准备动作。**

也就是说：

- 重启服务时间：**不计入查询时间**
- `drop_caches` 时间：**不计入查询时间**
- 可选等待服务恢复时间：**不计入查询时间**
- 子进程真正开始执行 `run_single_seed_queries.py` 的时刻，才是本次统计的起点

因此需要区分两类时间：

### A. 冷启动准备时间

记为：

- `prep_time_ms`

它包括：

- service restart
- `sync`
- `drop_caches`
- restart 后等待

### B. 查询调用时间

记为：

- `invoke_time_ms`

它从**启动 `run_single_seed_queries.py` 子进程**开始，到该子进程结束为止。

另外，子进程返回的每条 query 自己还有：

- `time_ms`

因此最终会有三层时间：

1. `prep_time_ms`
   - 冷启动准备动作耗时
2. `invoke_time_ms`
   - 从查询脚本调用开始到结束的总耗时
3. `query_results[*].time_ms`
   - `neighbor` / `k_hop` 的内部执行时间

其中本轮设计要求重点保证：

> 对“查询调用”的统计起点，是调用查询脚本开始，而不是前面的清缓存/重启步骤开始。

---

## 4. 脚本定位

建议新文件：

- `experiments/cross_db_graph/scripts/run_cold_start_queries.py`

其定位为：

> 针对单一 engine，从 `seeds.json` 中选取多个 seed；对每个 seed 的 `neighbor`、`k_hop(2)`、`k_hop(3)` 分别执行独立的冷启动动作和独立的 Python 子进程调用，并记录结果。

---

## 5. 建议 CLI

由于不再支持复杂 cold mode，CLI 可大幅简化。

建议保留：

```bash
uv run python -m experiments.cross_db_graph.scripts.run_cold_start_queries \
  --engine postgres \
  --seeds-file experiments/cross_db_graph/seeds.json \
  --seed-group high_degree \
  --sample-size 20 \
  --output-dir experiments/cross_db_graph/results/cold_postgres_001
```

### 5.1 必要参数

- `--engine`
  - `lancedb`
  - `lance_graph`
  - `postgres`
  - `arangodb`

### 5.2 常规参数

- `--seeds-file`
  - 默认：`experiments/cross_db_graph/seeds.json`
- `--seed-group`
  - 指定从哪个分组中取 seed
- `--all-degree-groups`
  - 一次同时跑 `low_degree`、`medium_degree`、`high_degree`
  - 每个 group 都按同样的 `sample-size` 和 `sample-strategy` 独立取样
- `--sample-size`
  - 本次抽多少个 seed
- `--sample-strategy`
  - 可保留简单版本：`head` / `random`
- `--random-seed`
  - 随机采样可复现
- `--direction`
  - 默认 `out`
- `--materialize`
  - `true` / `false`，透传给 `run_single_seed_queries.py`
- `--db-path`
  - 给 `lancedb` / `lance_graph` 使用
- `--output-dir`
  - 输出目录
- `--timeout-seconds`
  - 单个 seed 查询子进程超时
- `--continue-on-error`
  - 某个 seed 失败时是否继续
- `--restart-wait-seconds`
  - 仅对 PostgreSQL / ArangoDB 生效
  - 用于服务重启后等待恢复

不再需要：

- `--cold-mode`
- 各种可组合 cache/restart mode

---

## 6. 固定冷启动行为定义

### 6.1 `lancedb`

每个 seed 运行前执行：

1. `sync`
2. 写入 `/proc/sys/vm/drop_caches` 值 `3`
3. 可选 sleep 一个很短的稳定时间，比如 `0.2 ~ 0.5s`

不涉及服务重启。

### 6.2 `lance_graph`

与 `lancedb` 相同：

1. `sync`
2. `drop_caches`
3. 可选短暂等待

### 6.3 `postgres`

每个 seed 运行前执行：

1. `systemctl restart postgresql`
2. 确认服务 active
3. sleep `restart_wait_seconds`
4. `sync`
5. `drop_caches`
6. 可再 sleep 一个很短的稳定时间

### 6.4 `arangodb`

每个 seed 运行前执行：

1. `systemctl restart arangodb3`
2. 确认服务 active
3. sleep `restart_wait_seconds`
4. `sync`
5. `drop_caches`
6. 可再 sleep 一个很短的稳定时间

---

## 7. 为什么要把时间拆开记录

如果把“重启服务 + 清缓存 + 查询”全部混在一个时间里，那么结果会非常难解释。

例如 PostgreSQL：

- 服务重启可能花 1~2 秒
- 真正 query 可能只有几毫秒

如果把它们加总，那么你会得到一个看起来完全失真的 latency。

因此应明确：

### 应被汇总比较的时间

优先比较：

1. `invoke_time_ms`
   - 查询脚本从启动到结束的时间
2. `query_results[*].time_ms`
   - 内部单查询执行时间

### 应被单独展示、但不混入查询延迟对比的时间

- `prep_time_ms`

这样可以区分：

- 冷准备成本高不高
- 真正查询调用慢不慢
- 单条 query 本身慢不慢

---

## 8. 调用方式设计

新脚本不要直接 import `run_single_seed_queries.main()`，而是统一走子进程调用。

推荐命令模板：

```bash
uv run python -m experiments.cross_db_graph.scripts.run_single_seed_queries \
  --engine <engine> \
  --seed <seed> \
  --direction <direction> \
  --json \
  [--materialize true|false] \
  [--db-path <path>]
```

### 这样做的原因

1. 保证每次 seed 都是全新 Python 进程
2. 避免当前进程内残留缓存影响结果
3. 复用已有逻辑，降低实现风险

### 8.1 当前实现中的 root 前提

当前实现已经按严格 cold-start 语义执行，不提供自动降级：

- `lancedb` / `lance_graph`：必须真实执行 page cache 清理
- `postgres` / `arangodb`：必须真实执行服务重启和 page cache 清理

因此脚本会在启动时直接检查 root 权限。

如果当前进程不是 root：

- 脚本会立即失败
- 不会自动退化成 warm run
- 不会产生语义不清晰的“伪 cold-start”结果

这个约束是为了保证结果可解释性。

---

## 9. 建议的数据结构

### 9.1 `SeedRecord`

```python
@dataclass
class SeedRecord:
    seed: str
    group: str
    metadata: dict[str, Any]
```

### 9.2 `PrepActionRecord`

```python
@dataclass
class PrepActionRecord:
    engine: str
    restarted_service: bool
    service_name: str | None
    dropped_page_cache: bool
    prep_time_ms: float
```

### 9.3 `SingleRunRecord`

```python
@dataclass
class SingleRunRecord:
    run_index: int
    engine: str
    seed: str
    group: str
    direction: str
    materialize: bool | None
    prep: dict[str, Any]
    invoke_time_ms: float
    success: bool
    returncode: int | None
    query_results: list[dict[str, Any]]
    error: str | None
```
```

注意：

- `prep_time_ms` 单独记录
- `invoke_time_ms` 单独记录
- `query_results[*].time_ms` 保持原始值

---

## 10. seed 处理设计

建议继续做成兼容型解析，而不是假设 `seeds.json` 只有一种格式。

统一转换成：

```python
dict[str, list[SeedRecord]]
```

然后：

1. 按 `seed_group` 取目标集合
2. 根据 `sample_strategy` 采样
3. 把最终选中的 seed 写到：
   - `selected_seeds.json`

这样结果可复现。

---

## 11. 每个 seed 的执行流程

建议固定为：

对于每个 seed，要依次执行 3 个独立 run：

1. `neighbor`
2. `k_hop(2)`
3. `k_hop(3)`

每个 run 都有自己独立的：

- 冷启动准备动作
- 查询子进程
- `prep_time_ms`
- `invoke_time_ms`

### Step 1. 准备 run 元信息

记录：

- `run_index`
- `seed`
- `group`
- `engine`

### Step 2. 为当前 query 执行冷启动准备动作

按 engine 固定规则执行：

- `lancedb` / `lance_graph`
  - `drop_caches`
- `postgres` / `arangodb`
  - `restart service + drop_caches`

这一步计入：

- `prep_time_ms`

### Step 3. 为当前 query 启动查询子进程

从这里开始计时：

- `invoke_started_at`

调用：

- `run_single_seed_queries.py --json`

虽然子进程仍会返回 `neighbor`、`k_hop(2)`、`k_hop(3)` 三条结果，但当前 orchestration run 只提取本轮目标 query 对应的那一条记录。

到子进程结束时停止计时，得到：

- `invoke_time_ms`

### Step 4. 解析 JSON 并提取当前 query

从返回的 3 条 query 记录中，仅提取本轮目标 query：

- `neighbor`
- `k_hop(2)`
- `k_hop(3)`

### Step 5. 立即落盘

写入：

- `per_run/*.json`
- `runs.jsonl`
- `raw/*.stdout.txt`
- `raw/*.stderr.txt`

这样即使中途中断，也能保留已完成部分。

---

## 12. 输出目录结构

建议：

```text
experiments/cross_db_graph/results/cold_<engine>_<timestamp>/
  run_config.json
  selected_seeds.json
  runs.jsonl
  summary.json
  summary.md
  per_run/
    0001_seed_xxx.json
    0002_seed_yyy.json
  raw/
    0001.stdout.txt
    0001.stderr.txt
```

---

## 13. 汇总统计设计

### 13.1 overall 层

记录：

- `total_runs`
- `successful_runs`
- `failed_runs`
- `avg_prep_time_ms`
- `avg_invoke_time_ms`

其中：

- `avg_prep_time_ms` 仅作为辅助信息
- `avg_invoke_time_ms` 才是“从查询调用开始”的总调用成本

### 13.2 query 层

对以下三类分别聚合：

- `neighbor`
- `k_hop_2`
- `k_hop_3`

统计：

- avg
- median
- p95
- min
- max
- avg_result_count

使用字段：

- `query_results[*].time_ms`

### 13.3 group 层

当前实现已经支持在同一次运行中同时覆盖：

- `low_degree`
- `medium_degree`
- `high_degree`

当使用：

- `--all-degree-groups`

时，summary 会自动增加：

- `by_group`

并分别汇总每个 group 下的：

- `successful_runs`
- `avg_prep_time_ms`
- `avg_invoke_time_ms`
- `neighbor`
- `k_hop_2`
- `k_hop_3`

这样就不需要手工拆成 3 次运行再做汇总。

---

## 14. 失败处理设计

可能失败的地方：

1. service restart 失败
2. `drop_caches` 失败
3. 子进程超时
4. 子进程退出码非 0
5. stdout 不是合法 JSON

建议：

- 每个 run 无论成功失败，都写入 `runs.jsonl`
- `continue_on_error=false` 时，遇到失败立即终止
- `continue_on_error=true` 时，继续后续 seed

失败记录中至少包含：

- `run_index`
- `seed`
- `phase`
- `error`
- `returncode`

---

## 15. root 权限下的实现建议

由于当前用户有 root 权限，建议直接在 Python 中执行：

### 清理 page cache

```python
subprocess.run(["sync"], check=True)
with open("/proc/sys/vm/drop_caches", "w", encoding="utf-8") as f:
    f.write("3\n")
```

### 重启服务

优先：

```python
subprocess.run(["systemctl", "restart", service_name], check=True)
subprocess.run(["systemctl", "is-active", "--quiet", service_name], check=True)
```

如果系统环境特殊，再 fallback 到：

```python
subprocess.run(["service", service_name, "restart"], check=True)
```

---

## 16. 推荐函数划分

建议至少包含这些函数：

- `parse_args()`
- `load_seed_candidates()`
- `select_seeds()`
- `make_output_dir()`
- `drop_linux_page_caches()`
- `restart_service(service_name)`
- `prepare_engine_for_cold_query(engine, restart_wait_seconds)`
- `build_single_seed_command(...)`
- `run_single_seed_subprocess(...)`
- `append_jsonl(...)`
- `summarize_runs(...)`
- `render_summary_markdown(...)`
- `main()`

其中：

### `prepare_engine_for_cold_query(engine, restart_wait_seconds)`

负责：

- 根据 engine 固定执行预处理动作
- 返回 `PrepActionRecord`

### `run_single_seed_subprocess(...)`

负责：

- 构造命令
- 从子进程启动开始计 `invoke_time_ms`
- 捕获 stdout/stderr
- 解析 JSON

---

## 17. `summary.md` 推荐内容

建议生成的人读版 summary 明确区分两类时间：

```markdown
# Cold Start Query Summary

- engine: `postgres`
- seed_group: `high_degree`
- sample_size: `20`
- successful_runs: `20/20`

## Timing Semantics

- `prep_time_ms`: 服务重启 / page cache 清理等预处理耗时，不计入查询调用时间
- `invoke_time_ms`: 从启动 `run_single_seed_queries.py` 开始，到其结束为止的总调用耗时
- `query time_ms`: 查询脚本内部记录的单条查询执行时间

## Overall

- avg prep_time_ms: `1451.2`
- avg invoke_time_ms: `182.4`

## Query Metrics

| query | avg_ms | median_ms | p95_ms | avg_count |
|---|---:|---:|---:|---:|
| neighbor | 1.7 | 1.6 | 2.5 | 43.2 |
| k_hop_2 | 12.4 | 12.1 | 15.8 | 305.8 |
| k_hop_3 | 51.9 | 50.7 | 61.4 | 1910.4 |
```

---

## 18. 当前实现状态

当前实现已经按更严格的 per-query 方式落地：

1. 单 engine
2. 单 seed -> 三次独立调用 -> 分别对应 `neighbor`、`k_hop(2)`、`k_hop(3)`
3. 每个 query 都有自己独立的冷启动准备过程
4. 固定冷启动规则，不提供复杂 mode 组合
5. 区分 `prep_time_ms` 与 `invoke_time_ms`
6. 可靠落盘与 summary 输出

当前输出记录也相应变为“单条 query 一条 run record”，而不是“单个 seed 一条 run record”。

暂不做：

- 多 engine 混跑
- 并发执行
- 每条 query 单独一个子进程
- 自动画图

---

## 19. 结论

整合新要求后，`run_cold_start_queries.py` 的实现重点变成两件事：

1. **每个引擎的冷启动动作固定化**
   - `lancedb` / `lance_graph`：清 page cache
   - `postgres` / `arangodb`：重启服务 + 清 page cache
2. **查询计时口径严格从查询调用开始**
   - 不把前置 restart / drop cache 计入查询调用时间

因此最终结果中必须至少同时保留：

- `prep_time_ms`
- `invoke_time_ms`
- `query_results[*].time_ms`

这样后续分析时，既能看 cold-start 准备成本，也能公平比较“真正查询调用开始之后”的表现。

另外，当前实现还固定了一个实践原则：

- **如果无法完成冷启动前置动作，就直接失败**

也就是说：

- 没有 root 权限时，不允许继续跑 `drop_caches`
- 不能成功重启服务时，不允许继续伪装成 cold-start 测试

这样可以避免输出带有歧义的 benchmark 结果。

---

## 20. 后续配套更新建议

实现完成后，建议同步更新：

- `experiments/cross_db_graph/scripts/README.md`

新增 `run_cold_start_queries.py` 小节，明确写出：

- 每个引擎固定的冷启动规则
- 时间统计口径
- 推荐调用方式
- 输出目录位置
- 需要 root 权限的原因

这样后续实验时不会再对 timing semantics 产生歧义。
","contentType":"markdown"}ാപാത്ര to=functions.manage_todo_list  北京赛车女json string={