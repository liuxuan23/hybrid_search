#!/usr/bin/env python
"""
评估 Adj 邻接索引表优化在不同 BFS 深度下的效果。

以若干随机种子节点为起点，分别在 depth=1..MAX_DEPTH 下执行
get_knowledge_graph，对比 Base（edge 表逐层 WHERE）与 AdjTable
（首跳 WHERE + 后续跳 take）的端到端耗时，并关注：

  1. 端到端时间随深度的增长曲线
  2. 逐跳边际代价（通过相邻深度差分估算）
  3. 子图规模（节点数/边数）随深度的增长
  4. 不同 max_nodes 预算下的深度性能
  5. 高/低度种子节点在不同深度下的差异

运行（需要已有 1M 规模预构建 DB）：
  PERF_DB_DIR=/data/lightrag/_prebuilt_db \
  pytest tests/test_lancedb_adj_bfs_depth.py -v -s

可调环境变量：
  PERF_DB_DIR              预构建 DB 根目录（需含 base/ 和 opt/）
  PERF_BFS_REPEATS         每项查询重复次数，取最小值（默认 3）
  PERF_BFS_SEED_COUNT      BFS 测试种子节点数（默认 10）
  PERF_BFS_MAX_DEPTH       测试最大深度（默认 5）
  PERF_BFS_MAX_NODES       BFS max_nodes（默认 1000）
  PERF_BFS_SAMPLE_N        从 adj 表采样多少节点用于种子选择（默认 50000）
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
_REPEATS = int(os.environ.get("PERF_BFS_REPEATS", "3"))
_SEED_COUNT = int(os.environ.get("PERF_BFS_SEED_COUNT", "10"))
_MAX_DEPTH = int(os.environ.get("PERF_BFS_MAX_DEPTH", "5"))
_MAX_NODES = int(os.environ.get("PERF_BFS_MAX_NODES", "1000"))
_SAMPLE_N = int(os.environ.get("PERF_BFS_SAMPLE_N", "50000"))
_NAMESPACE = os.environ.get("PERF_NAMESPACE", "perf_graph")


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------

@dataclass
class SeedNode:
    entity_id: str
    degree: int  # len(out) + len(in)


@dataclass
class BFSResult:
    """单次 BFS 的测量结果。"""
    time_sec: float
    n_nodes: int
    n_edges: int


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

async def _time_bfs(storage, seed_id: str, depth: int, max_nodes: int,
                    repeats: int = _REPEATS) -> BFSResult:
    """重复执行 BFS 取最小耗时，同时记录子图规模。"""
    best_time = float("inf")
    last_result = None
    for _ in range(repeats):
        gc.collect()
        t0 = time.perf_counter()
        try:
            kg = await storage.get_knowledge_graph(
                seed_id, max_depth=depth, max_nodes=max_nodes
            )
        except (MemoryError, RuntimeError, OSError) as e:
            print(f"\n  ⚠ OOM/资源不足: {type(e).__name__}: {e}")
            return BFSResult(time_sec=float("inf"), n_nodes=0, n_edges=0)
        elapsed = time.perf_counter() - t0
        if elapsed < best_time:
            best_time = elapsed
            last_result = kg
    return BFSResult(
        time_sec=best_time,
        n_nodes=len(last_result.nodes) if last_result else 0,
        n_edges=len(last_result.edges) if last_result else 0,
    )


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


# ---------------------------------------------------------------------------
# 模块级缓存：种子节点（只计算一次，所有测试共享）
# ---------------------------------------------------------------------------
_seed_nodes_cache: list[SeedNode] | None = None
_high_degree_seeds_cache: list[SeedNode] | None = None
_low_degree_seeds_cache: list[SeedNode] | None = None


async def _select_seed_nodes(namespace: str = _NAMESPACE) -> list[SeedNode]:
    """从预构建 adj 表中选择种子节点（中等度数，带缓存）。"""
    global _seed_nodes_cache
    if _seed_nodes_cache is not None:
        return _seed_nodes_cache

    import lancedb

    db = await lancedb.connect_async(os.path.join(_PERF_DB_DIR, "opt"))
    adj_tbl = await db.open_table(f"{namespace}_adj_idx")

    total = await adj_tbl.count_rows()
    sample_n = min(_SAMPLE_N, total)

    # 随机采样 adj 行
    rowids = random.sample(range(total), k=sample_n)
    rows = await adj_tbl.take_row_ids(rowids).select(["entity_id", "out", "in"]).to_list()

    all_nodes: list[SeedNode] = []
    for r in rows:
        out_len = len(r.get("out") or [])
        in_len = len(r.get("in") or [])
        deg = out_len + in_len
        if deg == 0:
            continue
        all_nodes.append(SeedNode(entity_id=r["entity_id"], degree=deg))

    # 按度数排序，选取中位数附近的节点作为"典型"种子
    all_nodes.sort(key=lambda s: s.degree)
    n = len(all_nodes)
    mid_start = max(0, n // 2 - _SEED_COUNT // 2)
    mid_end = min(n, mid_start + _SEED_COUNT)
    seeds = all_nodes[mid_start:mid_end]

    # 打印种子信息
    degs = [s.degree for s in seeds]
    print(f"\n{'='*70}")
    print(f"BFS 深度测试 — 种子节点（从 {sample_n}/{total} 节点中选取中位数附近）")
    print(f"种子数: {len(seeds)}, 度数: avg={sum(degs)/len(degs):.1f}, "
          f"min={min(degs)}, max={max(degs)}")
    print(f"{'='*70}")

    _seed_nodes_cache = seeds
    return seeds


async def _select_stratified_seeds(namespace: str = _NAMESPACE) -> tuple[list[SeedNode], list[SeedNode]]:
    """选择高度数和低度数两组种子节点（各 SEED_COUNT/2 个）。"""
    global _high_degree_seeds_cache, _low_degree_seeds_cache
    if _high_degree_seeds_cache is not None and _low_degree_seeds_cache is not None:
        return _high_degree_seeds_cache, _low_degree_seeds_cache

    import lancedb

    db = await lancedb.connect_async(os.path.join(_PERF_DB_DIR, "opt"))
    adj_tbl = await db.open_table(f"{namespace}_adj_idx")

    total = await adj_tbl.count_rows()
    sample_n = min(_SAMPLE_N, total)

    rowids = random.sample(range(total), k=sample_n)
    rows = await adj_tbl.take_row_ids(rowids).select(["entity_id", "out", "in"]).to_list()

    all_nodes: list[SeedNode] = []
    for r in rows:
        out_len = len(r.get("out") or [])
        in_len = len(r.get("in") or [])
        deg = out_len + in_len
        if deg == 0:
            continue
        all_nodes.append(SeedNode(entity_id=r["entity_id"], degree=deg))

    all_nodes.sort(key=lambda s: s.degree)
    n = len(all_nodes)
    half = max(1, _SEED_COUNT // 2)

    # 低度数：取 P10 附近
    low_start = max(0, n // 10 - half // 2)
    low_seeds = all_nodes[low_start:low_start + half]

    # 高度数：取 P90 附近
    high_start = max(0, 9 * n // 10 - half // 2)
    high_seeds = all_nodes[high_start:high_start + half]

    print(f"\n{'='*70}")
    print(f"BFS 深度×度数测试 — 分层种子节点（从 {sample_n}/{total} 节点中选取）")
    low_degs = [s.degree for s in low_seeds]
    high_degs = [s.degree for s in high_seeds]
    print(f"  低度数组（P10 附近）: {len(low_seeds)} seeds, "
          f"avg_deg={sum(low_degs)/len(low_degs):.1f}, "
          f"range=[{min(low_degs)},{max(low_degs)}]")
    print(f"  高度数组（P90 附近）: {len(high_seeds)} seeds, "
          f"avg_deg={sum(high_degs)/len(high_degs):.1f}, "
          f"range=[{min(high_degs)},{max(high_degs)}]")
    print(f"{'='*70}")

    _high_degree_seeds_cache = high_seeds
    _low_degree_seeds_cache = low_seeds
    return high_seeds, low_seeds


# ---------------------------------------------------------------------------
# Storage 初始化辅助
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
class TestAdjBFSDepth:
    """评估邻接索引表优化在不同 BFS 深度下的效果。"""

    NAMESPACE = _NAMESPACE

    # ---- fixtures ----

    @pytest.fixture
    async def seeds(self) -> list[SeedNode]:
        return await _select_seed_nodes(self.NAMESPACE)

    @pytest.fixture
    async def stratified_seeds(self) -> tuple[list[SeedNode], list[SeedNode]]:
        return await _select_stratified_seeds(self.NAMESPACE)

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

    # ---- 测试 1: 端到端 BFS 深度扫描 ----

    @pytest.mark.asyncio
    async def test_bfs_depth_sweep(self, seeds, base_storage, adj_storage):
        """
        对每个种子节点在 depth=1..MAX_DEPTH 下执行 BFS，
        取所有种子的平均耗时，对比 Base vs AdjTable 端到端性能。

        预期：depth 越深，AdjTable 的 take 路径优势越大（避免逐层 WHERE 扫描）。
        """
        depths = list(range(1, _MAX_DEPTH + 1))
        seed_ids = [s.entity_id for s in seeds]

        print(f"\n{'─'*90}")
        print(f"  BFS 深度扫描（{len(seed_ids)} seeds, max_nodes={_MAX_NODES}）")
        print(f"{'─'*90}")
        print(f"  {'depth':>5s} {'Base_avg':>10s} {'Adj_avg':>10s} {'加速比':>8s} "
              f"{'avg_nodes':>10s} {'avg_edges':>10s}")
        print(f"  {'─'*5} {'─'*10} {'─'*10} {'─'*8} {'─'*10} {'─'*10}")

        for depth in depths:
            base_times = []
            adj_times = []
            adj_nodes_sum = 0
            adj_edges_sum = 0

            for sid in seed_ids:
                br = await _time_bfs(base_storage, sid, depth, _MAX_NODES)
                ar = await _time_bfs(adj_storage, sid, depth, _MAX_NODES)
                base_times.append(br.time_sec)
                adj_times.append(ar.time_sec)
                adj_nodes_sum += ar.n_nodes
                adj_edges_sum += ar.n_edges

            avg_base = sum(base_times) / len(base_times)
            avg_adj = sum(adj_times) / len(adj_times)
            avg_nodes = adj_nodes_sum / len(seed_ids)
            avg_edges = adj_edges_sum / len(seed_ids)
            sp = _speedup(avg_base, avg_adj)

            print(f"  {depth:5d} {_fmt(avg_base):>10s} {_fmt(avg_adj):>10s} {sp:>8s} "
                  f"{avg_nodes:>10.0f} {avg_edges:>10.0f}")

        print(f"{'─'*90}")

    # ---- 测试 2: 子图规模随深度增长 ----

    @pytest.mark.asyncio
    async def test_subgraph_growth(self, seeds, adj_storage):
        """
        测量子图（节点数/边数）随 BFS 深度的增长规律。

        这有助于理解 BFS 扩展的工作量如何随深度变化：
        - 均匀随机图中增长近似指数（受 max_nodes 截断）
        - 子图越大，Base 的 WHERE 扫描越慢，AdjTable 的 take 优势越明显。
        """
        depths = list(range(1, _MAX_DEPTH + 1))
        seed_ids = [s.entity_id for s in seeds]

        print(f"\n{'─'*78}")
        print(f"  子图规模随 BFS 深度增长（{len(seed_ids)} seeds, max_nodes={_MAX_NODES}）")
        print(f"{'─'*78}")
        print(f"  {'depth':>5s} {'avg_nodes':>10s} {'avg_edges':>10s} "
              f"{'max_nodes':>10s} {'max_edges':>10s} {'truncated':>10s}")
        print(f"  {'─'*5} {'─'*10} {'─'*10} {'─'*10} {'─'*10} {'─'*10}")

        for depth in depths:
            nodes_list = []
            edges_list = []
            truncated_count = 0

            for sid in seed_ids:
                kg = await adj_storage.get_knowledge_graph(
                    sid, max_depth=depth, max_nodes=_MAX_NODES
                )
                nodes_list.append(len(kg.nodes))
                edges_list.append(len(kg.edges))
                if kg.is_truncated:
                    truncated_count += 1

            avg_n = sum(nodes_list) / len(nodes_list)
            avg_e = sum(edges_list) / len(edges_list)
            max_n = max(nodes_list)
            max_e = max(edges_list)
            trunc_pct = truncated_count / len(seed_ids) * 100

            print(f"  {depth:5d} {avg_n:>10.0f} {avg_e:>10.0f} "
                  f"{max_n:>10d} {max_e:>10d} {trunc_pct:>9.0f}%")

        print(f"{'─'*78}")

    # ---- 测试 3: 逐跳边际代价估算 ----

    @pytest.mark.asyncio
    async def test_per_hop_marginal_cost(self, seeds, base_storage, adj_storage):
        """
        通过相邻深度的差分估算每一跳的边际代价：

          marginal_cost(hop=k) ≈ time(depth=k) - time(depth=k-1)

        直观地展示：
          - Base 方法每增加一跳需要额外多少时间（主要是 WHERE 扫描代价）
          - AdjTable 每增加一跳的额外代价（后续跳走 take 路径）
          - 加速比在哪一跳最为显著
        """
        depths = list(range(1, _MAX_DEPTH + 1))
        seed_ids = [s.entity_id for s in seeds]

        # 收集每个深度的平均耗时
        base_avg_by_depth: dict[int, float] = {}
        adj_avg_by_depth: dict[int, float] = {}

        for depth in depths:
            base_times = []
            adj_times = []
            for sid in seed_ids:
                br = await _time_bfs(base_storage, sid, depth, _MAX_NODES)
                ar = await _time_bfs(adj_storage, sid, depth, _MAX_NODES)
                base_times.append(br.time_sec)
                adj_times.append(ar.time_sec)
            base_avg_by_depth[depth] = sum(base_times) / len(base_times)
            adj_avg_by_depth[depth] = sum(adj_times) / len(adj_times)

        print(f"\n{'─'*90}")
        print(f"  逐跳边际代价（{len(seed_ids)} seeds, max_nodes={_MAX_NODES}）")
        print(f"{'─'*90}")
        print(f"  {'hop':>5s} {'Base_total':>10s} {'Base_Δ':>10s} "
              f"{'Adj_total':>10s} {'Adj_Δ':>10s} {'总加速比':>8s} {'Δ加速比':>8s}")
        print(f"  {'─'*5} {'─'*10} {'─'*10} {'─'*10} {'─'*10} {'─'*8} {'─'*8}")

        prev_base = 0.0
        prev_adj = 0.0
        for depth in depths:
            bt = base_avg_by_depth[depth]
            at = adj_avg_by_depth[depth]
            delta_base = bt - prev_base
            delta_adj = at - prev_adj
            sp_total = _speedup(bt, at)
            sp_delta = _speedup(delta_base, delta_adj) if delta_adj > 0 else "N/A"

            print(f"  {depth:5d} {_fmt(bt):>10s} {_fmt(delta_base):>10s} "
                  f"{_fmt(at):>10s} {_fmt(delta_adj):>10s} {sp_total:>8s} {sp_delta:>8s}")
            prev_base = bt
            prev_adj = at

        print(f"{'─'*90}")

    # ---- 测试 4: 不同 max_nodes 预算下的深度性能 ----

    @pytest.mark.asyncio
    async def test_bfs_depth_vs_max_nodes(self, seeds, base_storage, adj_storage):
        """
        在固定深度=3 下，变化 max_nodes 预算（100, 500, 1000, 2000），
        观察 Base 和 AdjTable 的性能变化。

        max_nodes 越大，BFS 扩展的节点越多：
          - Base 的 WHERE IN (...) 子句更长，扫描代价更高
          - AdjTable 的 take 批量更大，但仍是点查
        """
        test_depth = min(3, _MAX_DEPTH)
        max_nodes_list = [100, 500, 1000, 2000]
        seed_ids = [s.entity_id for s in seeds]

        print(f"\n{'─'*90}")
        print(f"  不同 max_nodes 预算（depth={test_depth}, {len(seed_ids)} seeds）")
        print(f"{'─'*90}")
        print(f"  {'max_nodes':>10s} {'Base_avg':>10s} {'Adj_avg':>10s} {'加速比':>8s} "
              f"{'avg_nodes':>10s} {'avg_edges':>10s}")
        print(f"  {'─'*10} {'─'*10} {'─'*10} {'─'*8} {'─'*10} {'─'*10}")

        for mn in max_nodes_list:
            base_times = []
            adj_times = []
            nodes_sum = 0
            edges_sum = 0

            for sid in seed_ids:
                br = await _time_bfs(base_storage, sid, test_depth, mn)
                ar = await _time_bfs(adj_storage, sid, test_depth, mn)
                base_times.append(br.time_sec)
                adj_times.append(ar.time_sec)
                nodes_sum += ar.n_nodes
                edges_sum += ar.n_edges

            avg_base = sum(base_times) / len(base_times)
            avg_adj = sum(adj_times) / len(adj_times)
            avg_n = nodes_sum / len(seed_ids)
            avg_e = edges_sum / len(seed_ids)
            sp = _speedup(avg_base, avg_adj)

            print(f"  {mn:>10d} {_fmt(avg_base):>10s} {_fmt(avg_adj):>10s} {sp:>8s} "
                  f"{avg_n:>10.0f} {avg_e:>10.0f}")

        print(f"{'─'*90}")

    # ---- 测试 5: 高/低度种子在不同深度下的对比 ----

    @pytest.mark.asyncio
    async def test_bfs_depth_by_degree_group(self, stratified_seeds, base_storage, adj_storage):
        """
        将种子按度数分为高/低两组，在每个深度下分别测量 BFS 性能。

        高度数种子每层展开更多邻居 → 子图增长更快、更早触达 max_nodes：
          - Base 的 WHERE 成本增长更快
          - AdjTable 的 take 成本也增长，但斜率更缓
        低度数种子每层展开较少邻居，BFS 更"稀疏"。
        """
        high_seeds, low_seeds = stratified_seeds
        depths = list(range(1, _MAX_DEPTH + 1))

        groups = [
            ("低度数", low_seeds),
            ("高度数", high_seeds),
        ]

        print(f"\n{'─'*100}")
        print(f"  BFS 深度×度数分组（max_nodes={_MAX_NODES}）")
        print(f"{'─'*100}")
        print(f"  {'组':8s} {'depth':>5s} {'seeds':>5s} {'avg_deg':>8s} "
              f"{'Base_avg':>10s} {'Adj_avg':>10s} {'加速比':>8s} "
              f"{'avg_nodes':>10s} {'avg_edges':>10s}")
        print(f"  {'─'*8} {'─'*5} {'─'*5} {'─'*8} "
              f"{'─'*10} {'─'*10} {'─'*8} {'─'*10} {'─'*10}")

        for group_name, group_seeds in groups:
            seed_ids = [s.entity_id for s in group_seeds]
            avg_deg = sum(s.degree for s in group_seeds) / len(group_seeds)

            for depth in depths:
                base_times = []
                adj_times = []
                nodes_sum = 0
                edges_sum = 0

                for sid in seed_ids:
                    br = await _time_bfs(base_storage, sid, depth, _MAX_NODES)
                    ar = await _time_bfs(adj_storage, sid, depth, _MAX_NODES)
                    base_times.append(br.time_sec)
                    adj_times.append(ar.time_sec)
                    nodes_sum += ar.n_nodes
                    edges_sum += ar.n_edges

                avg_base = sum(base_times) / len(base_times)
                avg_adj = sum(adj_times) / len(adj_times)
                avg_n = nodes_sum / len(seed_ids)
                avg_e = edges_sum / len(seed_ids)
                sp = _speedup(avg_base, avg_adj)

                print(f"  {group_name:8s} {depth:5d} {len(seed_ids):5d} {avg_deg:8.1f} "
                      f"{_fmt(avg_base):>10s} {_fmt(avg_adj):>10s} {sp:>8s} "
                      f"{avg_n:>10.0f} {avg_e:>10.0f}")

        print(f"{'─'*100}")
