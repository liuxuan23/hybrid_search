#!/usr/bin/env python
"""
评估 Adj 邻接索引表优化在不同节点度数下的效果。

按度数将节点分为若干桶（低度 / 中度 / 高度 / 超高度），
在每个桶内采样种子节点，分别测量以下操作的耗时：

  1. 度数查询 (node_degrees_batch)
     Base:  WHERE (source IN seeds) OR (target IN seeds) 全表扫描
     Adj:   WHERE entity_id IN seeds → len(out)+len(in)

  2. 首跳邻居发现 (WHERE 路径)
     Base:  WHERE (source IN seeds) OR (target IN seeds) → 提取邻居
     Adj:   WHERE entity_id IN seeds → out+in → take_row_ids → entity_id

  3. BFS 遍历 (get_knowledge_graph, depth=2)
     Base:  每层 WHERE 扫描 edge 表
     Adj:   首跳 WHERE + 后续跳 take 路径

运行（需要已有 1M 规模预构建 DB）：
  PERF_DB_DIR=/data/lightrag/_prebuilt_db \
  pytest tests/test_lancedb_adj_degree_impact.py -v -s

可调环境变量：
  PERF_DB_DIR              预构建 DB 根目录（需含 base/ 和 opt/）
  PERF_DEG_REPEATS         每项查询重复次数，取最小值（默认 3）
  PERF_DEG_SAMPLE_N        从 adj 表采样多少节点用于分桶统计（默认 50000）
  PERF_DEG_SEEDS_PER_BKT   每个桶采样多少种子节点（默认 30）
  PERF_DEG_BFS_DEPTH       BFS 测试深度（默认 2）
  PERF_DEG_BFS_MAX_NODES   BFS max_nodes（默认 500）
"""

from __future__ import annotations

import asyncio
import gc
import os
import random
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------------------
# 可配置参数
# ---------------------------------------------------------------------------
_PERF_DB_DIR = os.environ.get("PERF_DB_DIR", "")
_REPEATS = int(os.environ.get("PERF_DEG_REPEATS", "3"))
_SAMPLE_N = int(os.environ.get("PERF_DEG_SAMPLE_N", "50000"))
_SEEDS_PER_BKT = int(os.environ.get("PERF_DEG_SEEDS_PER_BKT", "30"))
_BFS_DEPTH = int(os.environ.get("PERF_DEG_BFS_DEPTH", "2"))
_BFS_MAX_NODES = int(os.environ.get("PERF_DEG_BFS_MAX_NODES", "500"))
_NAMESPACE = os.environ.get("PERF_NAMESPACE", "perf_graph")

# 度数桶边界（左闭右开）：
#   自适应分桶 — 根据实际数据的分位点动态计算。
#   静态桶仅作为 fallback。
_STATIC_BUCKET_BOUNDS = [1, 5, 15, 50, 100]


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------

def _build_where_in(column: str, values: list[str]) -> str:
    escaped = ", ".join(f"'{v}'" for v in values)
    return f"{column} IN ({escaped})"


async def _time_async(coro_func, repeats: int = _REPEATS) -> float:
    """重复执行取最小耗时。"""
    best = float("inf")
    for _ in range(repeats):
        gc.collect()
        t0 = time.perf_counter()
        try:
            await coro_func()
        except (MemoryError, RuntimeError, OSError) as e:
            print(f"\n  ⚠ OOM/资源不足: {type(e).__name__}: {e}")
            return float("inf")
        best = min(best, time.perf_counter() - t0)
    return best


def _fmt(t: float) -> str:
    if t == float("inf"):
        return "OOM"
    if t < 0.001:
        return f"{t * 1_000_000:.0f}µs"
    if t < 1.0:
        return f"{t * 1000:.1f}ms"
    return f"{t:.3f}s"


def _speedup(t_base: float, t_opt: float) -> str:
    if t_opt <= 0 or t_opt == float("inf") or t_base == float("inf"):
        return "N/A"
    return f"{t_base / t_opt:.1f}x"


def _bucket_label(deg: int, bounds: list[int]) -> str:
    """根据度数和桶边界返回所属桶标签。"""
    if not bounds:
        return "all"
    if deg < bounds[0]:
        return f"[0,{bounds[0]})"
    for i in range(len(bounds) - 1):
        lo, hi = bounds[i], bounds[i + 1]
        if lo <= deg < hi:
            return f"[{lo},{hi})"
    return f"[{bounds[-1]},+∞)"


@dataclass
class SeedNode:
    entity_id: str
    degree: int  # len(out) + len(in)


# ---------------------------------------------------------------------------
# 模块级缓存：度数分桶结果（只计算一次，所有测试共享）
# ---------------------------------------------------------------------------
_degree_buckets_cache: dict[str, list[SeedNode]] | None = None


async def _compute_degree_buckets(namespace: str = _NAMESPACE) -> dict[str, list[SeedNode]]:
    """从预构建 adj 表采样节点 → 计算度数 → 按桶分组（带缓存）。

    使用四分位分桶：按度数 P25/P50/P75 划分为四个桶，
    确保即使度数分布窄也能产生有意义的桶。
    """
    global _degree_buckets_cache
    if _degree_buckets_cache is not None:
        return _degree_buckets_cache

    import lancedb

    db = await lancedb.connect_async(os.path.join(_PERF_DB_DIR, "opt"))
    adj_tbl = await db.open_table(f"{namespace}_adj_idx")

    total = await adj_tbl.count_rows()
    sample_n = min(_SAMPLE_N, total)

    # 随机采样 adj 行（每个节点一行）
    rowids = random.sample(range(total), k=sample_n)
    rows = await adj_tbl.take_row_ids(rowids).select(["entity_id", "out", "in"]).to_list()

    # 计算所有采样节点的度数
    all_seeds: list[SeedNode] = []
    for r in rows:
        out_len = len(r.get("out") or [])
        in_len = len(r.get("in") or [])
        deg = out_len + in_len
        if deg == 0:
            continue
        all_seeds.append(SeedNode(entity_id=r["entity_id"], degree=deg))

    if not all_seeds:
        _degree_buckets_cache = {}
        return {}

    # 自适应分桶：用四分位点
    degs_sorted = sorted(s.degree for s in all_seeds)
    n = len(degs_sorted)
    p25 = degs_sorted[n // 4]
    p50 = degs_sorted[n // 2]
    p75 = degs_sorted[3 * n // 4]
    p_max = degs_sorted[-1]

    # 去重相邻桶边界（度数分布窄时可能 P25==P50）
    raw_bounds = [1, p25, p50, p75]
    bounds: list[int] = []
    for b in raw_bounds:
        if not bounds or b > bounds[-1]:
            bounds.append(b)

    # 分桶
    buckets: dict[str, list[SeedNode]] = defaultdict(list)
    for seed in all_seeds:
        label = _bucket_label(seed.degree, bounds)
        buckets[label].append(seed)

    # 每桶随机采样固定数量
    chosen: dict[str, list[SeedNode]] = {}
    for label, nodes in buckets.items():
        if len(nodes) <= _SEEDS_PER_BKT:
            chosen[label] = nodes
        else:
            chosen[label] = random.sample(nodes, k=_SEEDS_PER_BKT)

    # 打印桶分布信息
    print(f"\n{'='*70}")
    print(f"度数分桶统计（采样 {sample_n} / {total} 节点）")
    print(f"分桶边界（四分位自适应）: {bounds}")
    print(f"度数范围: [{degs_sorted[0]}, {p_max}]  P25={p25}  P50={p50}  P75={p75}")
    print(f"{'='*70}")
    for label in sorted(chosen.keys()):
        seeds = chosen[label]
        degs = [s.degree for s in seeds]
        avg = sum(degs) / len(degs) if degs else 0
        pct = len(buckets[label]) / n * 100
        print(f"  {label:12s}: {len(seeds):4d} seeds ({pct:5.1f}% of total), "
              f"avg_deg={avg:.1f}, min={min(degs)}, max={max(degs)}")
    print(f"{'='*70}")

    _degree_buckets_cache = chosen
    return chosen


# ---------------------------------------------------------------------------
# Storage 初始化辅助（与 test_lancedb_graph_optimizer.py 对齐）
# ---------------------------------------------------------------------------

async def _open_base(db_path: str, namespace: str):
    from lightrag.kg.lancedb_impl import LanceDBGraphStorage, ClientManager
    from lightrag.kg.shared_storage import initialize_share_data
    initialize_share_data(workers=1)
    ClientManager._instances = {"db": None, "ref_count": 0}
    os.environ["LANCEDB_URI"] = db_path
    s = LanceDBGraphStorage(
        namespace=namespace,
        global_config={"max_graph_nodes": 100000},
        embedding_func=None,
        workspace="",
    )
    await s.initialize()
    return s


async def _open_adj_only(db_path: str, namespace: str):
    from lightrag.kg.lancedb_graph_optimizer import OptimizedLanceDBGraphStorage
    from lightrag.kg.lancedb_impl import ClientManager
    from lightrag.kg.shared_storage import initialize_share_data
    initialize_share_data(workers=1)
    ClientManager._instances = {"db": None, "ref_count": 0}
    os.environ["LANCEDB_URI"] = db_path
    s = OptimizedLanceDBGraphStorage(
        namespace=namespace,
        global_config={"max_graph_nodes": 100000},
        embedding_func=None,
        workspace="",
        enable_adj_index_table=True,
        enable_physical_clustering=False,
    )
    # 禁用内存缓存 → 强制走邻接索引表
    async def _noop():
        pass
    s._ensure_adj_cache = _noop
    s._cache_loaded = False
    s._adj_cache = {}
    s._node_edges_cache = {}
    await s.initialize()
    return s


# ---------------------------------------------------------------------------
# 测试类
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not _PERF_DB_DIR or not os.path.isdir(_PERF_DB_DIR),
    reason="PERF_DB_DIR 未设置或不存在。先运行 prebuild_graph_db.py",
)
class TestAdjDegreeImpact:
    """按节点度数分桶，评估邻接索引表优化在不同度数下的效果。"""

    NAMESPACE = _NAMESPACE

    # ---- fixtures ----

    @pytest.fixture
    async def degree_buckets(self) -> dict[str, list[SeedNode]]:
        """从缓存获取度数分桶结果（首次调用时计算）。"""
        return await _compute_degree_buckets(self.NAMESPACE)

    @pytest.fixture
    async def base_storage(self):
        s = await _open_base(os.path.join(_PERF_DB_DIR, "base"), self.NAMESPACE)
        yield s
        await s.finalize()

    @pytest.fixture
    async def adj_storage(self):
        s = await _open_adj_only(os.path.join(_PERF_DB_DIR, "opt"), self.NAMESPACE)
        yield s
        await s.finalize()

    # ---- 辅助 ----

    @staticmethod
    def _print_table(title: str, rows: list[tuple[str, int, float, float, float, str]]):
        """打印对比结果表格。"""
        print(f"\n{'─'*78}")
        print(f"  {title}")
        print(f"{'─'*78}")
        print(f"  {'桶':12s} {'seeds':>5s} {'avg_deg':>8s} {'Base':>10s} {'AdjTable':>10s} {'加速比':>8s}")
        print(f"  {'─'*12} {'─'*5} {'─'*8} {'─'*10} {'─'*10} {'─'*8}")
        for label, n_seeds, avg_deg, t_base, t_adj, sp in rows:
            print(f"  {label:12s} {n_seeds:5d} {avg_deg:8.1f} {_fmt(t_base):>10s} {_fmt(t_adj):>10s} {sp:>8s}")
        print(f"{'─'*78}")

    # ---- 测试 1: 度数查询 ----

    @pytest.mark.asyncio
    async def test_degree_query_by_bucket(self, degree_buckets, base_storage, adj_storage):
        """
        按桶对比度数查询性能：
          Base:  edge 表 WHERE (source IN seeds) OR (target IN seeds)
          Adj:   adj 表 WHERE entity_id IN seeds → len(out)+len(in)
        """
        rows = []
        for label in sorted(degree_buckets.keys()):
            seeds = degree_buckets[label]
            if not seeds:
                continue
            seed_ids = [s.entity_id for s in seeds]
            avg_deg = sum(s.degree for s in seeds) / len(seeds)

            t_base = await _time_async(
                lambda ids=seed_ids: base_storage.node_degrees_batch(ids)
            )
            t_adj = await _time_async(
                lambda ids=seed_ids: adj_storage.node_degrees_batch(ids)
            )
            rows.append((label, len(seeds), avg_deg, t_base, t_adj, _speedup(t_base, t_adj)))

        self._print_table("度数查询 node_degrees_batch (per bucket)", rows)

    # ---- 测试 2: 首跳邻居发现 ----

    @pytest.mark.asyncio
    async def test_neighbor_discovery_by_bucket(self, degree_buckets, base_storage, adj_storage):
        """
        按桶对比首跳邻居发现性能：
          Base:  edge 表 WHERE (source IN seeds) OR (target IN seeds) → 提取邻居
          Adj:   adj 表 WHERE entity_id IN seeds → out+in → take_row_ids → entity_id
        """
        rows = []
        for label in sorted(degree_buckets.keys()):
            seeds = degree_buckets[label]
            if not seeds:
                continue
            seed_ids = [s.entity_id for s in seeds]
            avg_deg = sum(s.degree for s in seeds) / len(seeds)

            # Base: 用 get_nodes_edges_batch 模拟首跳邻居发现
            async def base_neighbors(ids=seed_ids):
                return await base_storage.get_nodes_edges_batch(ids)

            # Adj: 用 _query_neighbors_with_adj_rowids (WHERE → out+in → take)
            async def adj_neighbors(ids=seed_ids):
                return await adj_storage._query_neighbors_with_adj_rowids(ids)

            t_base = await _time_async(base_neighbors)
            t_adj = await _time_async(adj_neighbors)
            rows.append((label, len(seeds), avg_deg, t_base, t_adj, _speedup(t_base, t_adj)))

        self._print_table("首跳邻居发现 (per bucket)", rows)

    # ---- 测试 3: BFS 遍历 ----

    @pytest.mark.asyncio
    async def test_bfs_by_bucket(self, degree_buckets, base_storage, adj_storage):
        """
        按桶对比 BFS 遍历性能（depth=2, max_nodes=500）。

        从每个桶的第一个 seed 出发执行 BFS，高度数 seed 会在每层
        展开更多邻居，更能体现 adj 表 take 路径的优势。
        """
        rows = []
        for label in sorted(degree_buckets.keys()):
            seeds = degree_buckets[label]
            if not seeds:
                continue
            # 选桶中度数最接近中位数的 seed 作为起点
            sorted_seeds = sorted(seeds, key=lambda s: s.degree)
            mid_seed = sorted_seeds[len(sorted_seeds) // 2]
            avg_deg = sum(s.degree for s in seeds) / len(seeds)

            t_base = await _time_async(
                lambda sid=mid_seed.entity_id: base_storage.get_knowledge_graph(
                    sid, max_depth=_BFS_DEPTH, max_nodes=_BFS_MAX_NODES
                )
            )
            t_adj = await _time_async(
                lambda sid=mid_seed.entity_id: adj_storage.get_knowledge_graph(
                    sid, max_depth=_BFS_DEPTH, max_nodes=_BFS_MAX_NODES
                )
            )

            rows.append((label, 1, avg_deg, t_base, t_adj, _speedup(t_base, t_adj)))

        self._print_table(
            f"BFS 遍历 depth={_BFS_DEPTH} max_nodes={_BFS_MAX_NODES} (per bucket, median-degree seed)",
            rows,
        )

    # ---- 测试 4: 单节点度数查询（逐个 vs 批量） ----

    @pytest.mark.asyncio
    async def test_single_node_degree_by_bucket(self, degree_buckets, base_storage, adj_storage):
        """
        逐桶对比单节点度数查询：
        高度数节点在 edge 表上命中更多行，Base 开销更大；
        Adj 表只取一行再算 len(out)+len(in)，不受度数影响。
        """
        rows = []
        for label in sorted(degree_buckets.keys()):
            seeds = degree_buckets[label]
            if not seeds:
                continue
            # 用桶内所有 seed 逐个查一遍，取平均
            avg_deg = sum(s.degree for s in seeds) / len(seeds)

            async def base_single(seed_list=seeds):
                for s in seed_list:
                    await base_storage.node_degrees_batch([s.entity_id])

            async def adj_single(seed_list=seeds):
                for s in seed_list:
                    await adj_storage.node_degrees_batch([s.entity_id])

            t_base = await _time_async(base_single)
            t_adj = await _time_async(adj_single)
            rows.append((label, len(seeds), avg_deg, t_base, t_adj, _speedup(t_base, t_adj)))

        self._print_table("单节点度数查询（逐个，per bucket）", rows)

    # ---- 测试 5: adj 表原始操作拆解（纯 WHERE vs take） ----

    @pytest.mark.asyncio
    async def test_adj_raw_where_vs_take_by_bucket(self, degree_buckets, adj_storage):
        """
        拆解 adj 表两步操作在不同度数下的耗时：
          Step A: 定位 adj 行（WHERE entity_id IN seeds → 取 out/in）
          Step B: take 邻居 _rowid → entity_id

        高度数节点的 out+in 列表更长，Step B 的 take 量更大。
        """
        adj_tbl = adj_storage._adj_index_table
        node_tbl = adj_storage._node_table
        if not adj_tbl:
            pytest.skip("adj table not available")

        print(f"\n{'─'*78}")
        print("  adj 表操作拆解：WHERE 定位 + take 邻居（per bucket）")
        print(f"{'─'*78}")
        print(f"  {'桶':12s} {'seeds':>5s} {'avg_deg':>8s} "
              f"{'WHERE':>10s} {'take_nei':>10s} {'总计':>10s} {'avg_nei':>8s}")
        print(f"  {'─'*12} {'─'*5} {'─'*8} {'─'*10} {'─'*10} {'─'*10} {'─'*8}")

        for label in sorted(degree_buckets.keys()):
            seeds = degree_buckets[label]
            if not seeds:
                continue
            seed_ids = [s.entity_id for s in seeds]
            avg_deg = sum(s.degree for s in seeds) / len(seeds)

            # 收集邻居 _rowid 以便第二步 take
            neighbor_rids_cache: list[int] = []

            async def step_where(ids=seed_ids):
                nonlocal neighbor_rids_cache
                where = _build_where_in("entity_id", ids)
                rows = (
                    await adj_tbl.query()
                    .where(where)
                    .select(["out", "in"])
                    .to_list()
                )
                rids: set[int] = set()
                for r in rows:
                    for v in (r.get("out") or []):
                        rids.add(int(v))
                    for v in (r.get("in") or []):
                        rids.add(int(v))
                neighbor_rids_cache = list(rids)

            async def step_take():
                if not neighbor_rids_cache:
                    return
                await node_tbl.take_row_ids(neighbor_rids_cache).select(["_id"]).to_list()

            t_where = await _time_async(step_where)
            # step_where 最后一次执行填充了 neighbor_rids_cache
            n_neighbors = len(neighbor_rids_cache)
            t_take = await _time_async(step_take)

            print(f"  {label:12s} {len(seeds):5d} {avg_deg:8.1f} "
                  f"{_fmt(t_where):>10s} {_fmt(t_take):>10s} "
                  f"{_fmt(t_where + t_take):>10s} {n_neighbors:>8d}")

        print(f"{'─'*78}")
