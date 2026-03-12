#!/usr/bin/env python
"""
OptimizedLanceDBGraphStorage 功能测试 + 性能对比测试。

功能测试：验证优化层的所有公开接口与父类行为一致。
性能测试：对比 OptimizedLanceDBGraphStorage vs LanceDBGraphStorage 在
          度数查询、BFS 遍历、边过滤、get_knowledge_graph 上的耗时。

运行方式：
    # 仅功能测试（快速）
    pytest tests/test_lancedb_graph_optimizer.py -v -k "not perf"

    # 仅性能测试（每次现场构图）
    pytest tests/test_lancedb_graph_optimizer.py -v -k "perf and not Prebuild"

    # 使用预构建 DB 的大规模性能测试（聚焦查询阶段）
    # 先构建：python tests/prebuild_graph_db.py --n_nodes 5000 --n_extra_edges 15000
    # 再测试：PERF_DB_DIR=tests/_prebuilt_db pytest -v -k "TestPerformancePrebuild" tests/test_lancedb_graph_optimizer.py -s

    # 大图（百万节点级别）跳过内存缓存测试（避免 OOM）：
    # PERF_DB_DIR=/data/_prebuilt_db PERF_SKIP_MEMCACHE=1 pytest -v -k "TestPerformancePrebuild" tests/test_lancedb_graph_optimizer.py -s

    # 全部
    pytest tests/test_lancedb_graph_optimizer.py -v
"""

import asyncio
import os
import shutil
import time
import sys
import uuid

import pytest

# 确保项目根目录在 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------- 环境准备 ----------

TEST_DB_DIR = os.path.join(os.path.dirname(__file__), "_test_optimizer_db")


@pytest.fixture(scope="session", autouse=True)
def _cleanup_test_db_root():
    """确保测试中产生的本地 DB 目录最终被清理，避免磁盘逐步被占满。"""
    os.makedirs(TEST_DB_DIR, exist_ok=True)
    yield
    shutil.rmtree(TEST_DB_DIR, ignore_errors=True)


@pytest.fixture(autouse=True)
def _setup_env():
    """每个测试用例使用独立的 LanceDB 目录（项目内），测试后自动清理。"""
    # 初始化 shared_storage（创建锁等共享状态），必须在任何 storage.initialize() 之前调用
    from lightrag.kg.shared_storage import initialize_share_data
    initialize_share_data(workers=1)

    # 使用项目内目录，绕开 /tmp 空间不足问题。
    # 每个测试用例创建独立子目录，避免互相污染。
    db_path = os.path.join(TEST_DB_DIR, str(uuid.uuid4()))
    os.makedirs(db_path, exist_ok=True)
    os.environ["LANCEDB_URI"] = db_path
    # 清除 ClientManager 的单例状态，确保每个测试用例独立
    from lightrag.kg.lancedb_impl import ClientManager
    ClientManager._instances = {"db": None, "ref_count": 0}
    yield
    # 清理
    ClientManager._instances = {"db": None, "ref_count": 0}
    if "LANCEDB_URI" in os.environ:
        del os.environ["LANCEDB_URI"]
    shutil.rmtree(db_path, ignore_errors=True)


def _make_global_config(**overrides):
    cfg = {"max_graph_nodes": 1000}
    cfg.update(overrides)
    return cfg


async def _make_base_storage(namespace="test_graph"):
    """创建并初始化 LanceDBGraphStorage 实例。"""
    from lightrag.kg.lancedb_impl import LanceDBGraphStorage
    s = LanceDBGraphStorage(
        namespace=namespace,
        global_config=_make_global_config(),
        embedding_func=None,
        workspace="",
    )
    await s.initialize()
    return s


async def _make_optimized_storage(
    namespace="test_graph",
    enable_adj_index_table=False,
    enable_physical_clustering=False,
):
    """创建并初始化 OptimizedLanceDBGraphStorage 实例。"""
    from lightrag.kg.lancedb_graph_optimizer import OptimizedLanceDBGraphStorage
    s = OptimizedLanceDBGraphStorage(
        namespace=namespace,
        global_config=_make_global_config(),
        embedding_func=None,
        workspace="",
        enable_adj_index_table=enable_adj_index_table,
        enable_physical_clustering=enable_physical_clustering,
    )
    await s.initialize()
    return s


async def _insert_test_graph(storage, num_nodes=10, chain=True, extra_edges=None):
    """
    插入测试图数据。

    chain=True 时构建链式图：0-1-2-...-N
    extra_edges 可追加额外边: [(src, tgt), ...]
    """
    # 插入节点
    for i in range(num_nodes):
        await storage.upsert_node(f"node_{i}", {
            "entity_id": f"node_{i}",
            "entity_type": "test",
            "description": f"Test node {i}",
        })
    # 插入链式边
    if chain:
        for i in range(num_nodes - 1):
            await storage.upsert_edge(f"node_{i}", f"node_{i+1}", {
                "relationship": "next",
                "weight": 1.0,
                "description": f"Edge {i}->{i+1}",
            })
    # 插入额外边
    if extra_edges:
        for src, tgt in extra_edges:
            await storage.upsert_edge(src, tgt, {
                "relationship": "extra",
                "weight": 1.0,
                "description": f"Extra edge {src}->{tgt}",
            })


async def _insert_star_graph(storage, center="hub", num_spokes=20):
    """插入星形图：一个中心节点连接 num_spokes 个叶节点。"""
    await storage.upsert_node(center, {
        "entity_id": center,
        "entity_type": "hub",
        "description": "Hub node",
    })
    for i in range(num_spokes):
        leaf = f"leaf_{i}"
        await storage.upsert_node(leaf, {
            "entity_id": leaf,
            "entity_type": "leaf",
            "description": f"Leaf node {i}",
        })
        await storage.upsert_edge(center, leaf, {
            "relationship": "spoke",
            "weight": 1.0,
        })


# =====================================================================
# Part 1: 功能测试
# =====================================================================


class TestOptimizedStorageBasic:
    """基本 CRUD + 缓存行为测试。"""

    @pytest.mark.asyncio
    async def test_initialize_and_finalize(self):
        """确认初始化和释放不抛异常。"""
        s = await _make_optimized_storage()
        assert s._node_table is not None
        assert s._edge_table is not None
        await s.finalize()

    @pytest.mark.asyncio
    async def test_upsert_and_query_nodes(self):
        """基本节点增删查。"""
        s = await _make_optimized_storage()
        await s.upsert_node("A", {"entity_id": "A", "entity_type": "person", "description": "Alice"})
        assert await s.has_node("A")
        assert not await s.has_node("Z")
        node = await s.get_node("A")
        assert node is not None
        assert node["entity_type"] == "person"
        await s.finalize()

    @pytest.mark.asyncio
    async def test_upsert_and_query_edges(self):
        """基本边增删查。"""
        s = await _make_optimized_storage()
        await s.upsert_node("A", {"entity_id": "A"})
        await s.upsert_node("B", {"entity_id": "B"})
        await s.upsert_edge("A", "B", {"relationship": "knows", "weight": 2.0})
        assert await s.has_edge("A", "B")
        # 无向边：B-A 也应存在
        assert await s.has_edge("B", "A")
        edge = await s.get_edge("A", "B")
        assert edge is not None
        assert edge["relationship"] == "knows"
        await s.finalize()


class TestAdjCache:
    """内存邻接缓存功能测试。"""

    @pytest.mark.asyncio
    async def test_node_degree_from_cache(self):
        """度数通过缓存正确计算。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=5, chain=True)
        # 链式图: 0-1-2-3-4
        # degree: node_0=1, node_1=2, node_2=2, node_3=2, node_4=1
        assert await s.node_degree("node_0") == 1
        assert await s.node_degree("node_2") == 2
        assert await s.node_degree("node_4") == 1
        # 不存在的节点度数为 0
        assert await s.node_degree("nonexistent") == 0
        assert s._cache_loaded is True
        await s.finalize()

    @pytest.mark.asyncio
    async def test_node_degrees_batch(self):
        """批量度数查询。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=5, chain=True)
        degrees = await s.node_degrees_batch(["node_0", "node_2", "node_4", "missing"])
        assert degrees["node_0"] == 1
        assert degrees["node_2"] == 2
        assert degrees["node_4"] == 1
        assert degrees["missing"] == 0
        await s.finalize()

    @pytest.mark.asyncio
    async def test_get_node_edges_from_cache(self):
        """节点边列表通过缓存正确返回。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=4, chain=True)
        edges = await s.get_node_edges("node_1")
        assert edges is not None
        # node_1 有 2 条边: (node_0, node_1) 和 (node_1, node_2)
        assert len(edges) == 2
        await s.finalize()

    @pytest.mark.asyncio
    async def test_edge_degrees_batch(self):
        """边的度数（两端节点度数之和）。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=5, chain=True)
        result = await s.edge_degrees_batch([("node_0", "node_1"), ("node_2", "node_3")])
        # node_0: deg=1, node_1: deg=2 → edge_degree = 3
        assert result[("node_0", "node_1")] == 3
        # node_2: deg=2, node_3: deg=2 → edge_degree = 4
        assert result[("node_2", "node_3")] == 4
        await s.finalize()

    @pytest.mark.asyncio
    async def test_cache_invalidation(self):
        """缓存失效后重新加载。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=3, chain=True)
        assert await s.node_degree("node_1") == 2
        assert s._cache_loaded is True

        # 失效缓存
        s._invalidate_adj_cache()
        assert s._cache_loaded is False

        # 再次查询会重新加载
        assert await s.node_degree("node_1") == 2
        assert s._cache_loaded is True
        await s.finalize()

    @pytest.mark.asyncio
    async def test_cache_after_new_edge(self):
        """插入新边后，缓存失效后能反映新数据。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=3, chain=True)
        assert await s.node_degree("node_0") == 1

        # 新增一条边
        await s.upsert_edge("node_0", "node_2", {"relationship": "shortcut"})
        # 缓存未失效，仍看到旧值
        assert await s.node_degree("node_0") == 1

        # 手动失效缓存
        s._invalidate_adj_cache()
        # 现在看到新值
        assert await s.node_degree("node_0") == 2
        await s.finalize()


class TestBFSOptimization:
    """BFS 和多跳查询测试。"""

    @pytest.mark.asyncio
    async def test_bfs_single_node(self):
        """单节点 BFS。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=5, chain=True)
        kg = await s.get_knowledge_graph("node_0", max_depth=1, max_nodes=100)
        # depth=1: node_0 + node_1 = 2 nodes
        assert len(kg.nodes) == 2
        node_ids = {n.id for n in kg.nodes}
        assert "node_0" in node_ids
        assert "node_1" in node_ids
        await s.finalize()

    @pytest.mark.asyncio
    async def test_bfs_depth(self):
        """BFS 深度限制。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=10, chain=True)
        kg = await s.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        # depth=3: node_0 -> node_1 -> node_2 -> node_3 = 4 nodes
        assert len(kg.nodes) == 4
        node_ids = {n.id for n in kg.nodes}
        for i in range(4):
            assert f"node_{i}" in node_ids
        await s.finalize()

    @pytest.mark.asyncio
    async def test_bfs_max_nodes(self):
        """BFS 节点数量限制。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=20, chain=True)
        kg = await s.get_knowledge_graph("node_0", max_depth=100, max_nodes=5)
        # 不超过 max_nodes + 1（因为超限后才停止）
        assert len(kg.nodes) <= 6
        await s.finalize()

    @pytest.mark.asyncio
    async def test_bfs_includes_edges(self):
        """BFS 结果包含边。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=5, chain=True)
        kg = await s.get_knowledge_graph("node_0", max_depth=2, max_nodes=100)
        # 3 nodes → 2 edges in chain
        assert len(kg.nodes) == 3
        assert len(kg.edges) >= 2
        await s.finalize()

    @pytest.mark.asyncio
    async def test_bfs_star_graph(self):
        """星形图 BFS。"""
        s = await _make_optimized_storage()
        await _insert_star_graph(s, center="hub", num_spokes=10)
        kg = await s.get_knowledge_graph("hub", max_depth=1, max_nodes=100)
        # hub + 10 leaves = 11 nodes
        assert len(kg.nodes) == 11
        assert len(kg.edges) == 10
        await s.finalize()

    @pytest.mark.asyncio
    async def test_bfs_consistency_with_base(self):
        """优化层 BFS 结果与父类一致。"""
        # 构建两个独立的存储实例，插入相同数据
        base = await _make_base_storage(namespace="base_graph")
        opt = await _make_optimized_storage(namespace="opt_graph")

        for store in [base, opt]:
            await _insert_test_graph(store, num_nodes=8, chain=True,
                                     extra_edges=[("node_0", "node_3"), ("node_2", "node_5")])

        kg_base = await base.get_knowledge_graph("node_0", max_depth=2, max_nodes=100)
        kg_opt = await opt.get_knowledge_graph("node_0", max_depth=2, max_nodes=100)

        # 节点集合应一致
        base_node_ids = sorted(n.id for n in kg_base.nodes)
        opt_node_ids = sorted(n.id for n in kg_opt.nodes)
        assert base_node_ids == opt_node_ids, f"Nodes differ: {base_node_ids} vs {opt_node_ids}"

        # 边集合应一致
        base_edge_ids = sorted(e.id for e in kg_base.edges)
        opt_edge_ids = sorted(e.id for e in kg_opt.edges)
        assert base_edge_ids == opt_edge_ids, f"Edges differ: {base_edge_ids} vs {opt_edge_ids}"

        await base.finalize()
        await opt.finalize()


class TestGetKnowledgeGraphAllByDegree:
    """全图度数排序子图查询测试。"""

    @pytest.mark.asyncio
    async def test_all_by_degree_small_graph(self):
        """小图不裁剪。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=5, chain=True)
        kg = await s.get_knowledge_graph("*", max_depth=3, max_nodes=100)
        assert len(kg.nodes) == 5
        assert kg.is_truncated is False
        await s.finalize()

    @pytest.mark.asyncio
    async def test_all_by_degree_truncated(self):
        """大图按度数裁剪。"""
        s = await _make_optimized_storage()
        await _insert_star_graph(s, center="hub", num_spokes=20)
        kg = await s.get_knowledge_graph("*", max_depth=3, max_nodes=5)
        assert kg.is_truncated is True
        assert len(kg.nodes) <= 5
        # hub 度数最高（20），应在结果中
        node_ids = {n.id for n in kg.nodes}
        assert "hub" in node_ids
        await s.finalize()


class TestFetchEdgesBetweenNodes:
    """_fetch_edges_between_nodes 测试。"""

    @pytest.mark.asyncio
    async def test_fetch_edges_between_subset(self):
        """仅返回子集内部的边。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=5, chain=True)
        edges = await s._fetch_edges_between_nodes(["node_1", "node_2", "node_3"])
        # 应包含: node_1-node_2, node_2-node_3
        assert len(edges) == 2
        await s.finalize()

    @pytest.mark.asyncio
    async def test_fetch_edges_empty(self):
        """空输入返回空。"""
        s = await _make_optimized_storage()
        edges = await s._fetch_edges_between_nodes([])
        assert edges == []
        await s.finalize()


class TestAdjIndexTable:
    """邻接索引表功能测试（enable_adj_index_table=True）。"""

    @pytest.mark.asyncio
    async def test_adj_index_table_created(self):
        """索引表正确创建。"""
        s = await _make_optimized_storage(enable_adj_index_table=True)
        assert s._adj_index_table is not None
        await s.finalize()

    @pytest.mark.asyncio
    async def test_rebuild_adj_index(self):
        """重建索引表后可查询。"""
        s = await _make_optimized_storage(enable_adj_index_table=True)
        await _insert_test_graph(s, num_nodes=5, chain=True)
        await s._rebuild_adj_index_table()

        # 通过辅助方法验证
        neighbors = await s._query_neighbors_from_adj_table(["node_2"])
        assert "node_1" in neighbors.get("node_2", set())
        assert "node_3" in neighbors.get("node_2", set())
        await s.finalize()

    @pytest.mark.asyncio
    async def test_query_edges_from_adj_table(self):
        """新版 adj 表不存 edge 信息，应返回空列表。"""
        s = await _make_optimized_storage(enable_adj_index_table=True)
        await _insert_test_graph(s, num_nodes=5, chain=True)
        await s._rebuild_adj_index_table()

        edge_ids, edge_row_ids = await s._query_edges_from_adj_table(["node_1", "node_2", "node_3"])
        # 新版 per-node-one-row 不存 edge 信息，返回空
        assert edge_ids == []
        assert edge_row_ids == []
        await s.finalize()

    @pytest.mark.asyncio
    async def test_query_degree_from_adj_table(self):
        """通过索引表计算度数。"""
        s = await _make_optimized_storage(enable_adj_index_table=True)
        await _insert_test_graph(s, num_nodes=5, chain=True)
        await s._rebuild_adj_index_table()

        degrees = await s._query_degree_from_adj_table(["node_0", "node_2"])
        assert degrees.get("node_0") == 1
        assert degrees.get("node_2") == 2
        await s.finalize()

    @pytest.mark.asyncio
    async def test_query_all_degrees_from_adj_table(self):
        """通过索引表计算全图度数。"""
        s = await _make_optimized_storage(enable_adj_index_table=True)
        await _insert_test_graph(s, num_nodes=5, chain=True)
        await s._rebuild_adj_index_table()

        all_degrees = await s._query_all_degrees_from_adj_table()
        assert all_degrees.get("node_0") == 1
        assert all_degrees.get("node_2") == 2
        assert all_degrees.get("node_4") == 1
        await s.finalize()

    @pytest.mark.asyncio
    async def test_bfs_fallback_to_adj_table(self):
        """
        当内存缓存不可用但邻接索引表可用时，BFS 通过索引表执行。
        """
        s = await _make_optimized_storage(enable_adj_index_table=True)
        await _insert_test_graph(s, num_nodes=6, chain=True)
        await s._rebuild_adj_index_table()

        # 强制禁用内存缓存
        s._cache_loaded = False
        s._adj_cache = {}
        s._node_edges_cache = {}
        # 猴子补丁阻止缓存重建
        s._ensure_adj_cache = lambda: asyncio.coroutine(lambda: None)()

        kg = await s.get_knowledge_graph("node_0", max_depth=2, max_nodes=100)
        assert len(kg.nodes) == 3  # depth=2: node_0, node_1, node_2
        node_ids = {n.id for n in kg.nodes}
        assert "node_0" in node_ids
        assert "node_1" in node_ids
        assert "node_2" in node_ids
        await s.finalize()

    @pytest.mark.asyncio
    async def test_take_adj_neighbors(self):
        """测试 take 路径：通过 node_row_ids 获取下一跳邻居。"""
        s = await _make_optimized_storage(enable_adj_index_table=True)
        await _insert_test_graph(s, num_nodes=6, chain=True)
        await s._rebuild_adj_index_table()

        # 先用 WHERE 路径获取 node_0 的邻居和 node_row_ids
        neighbors, node_row_ids = await s._query_neighbors_with_adj_rowids(["node_0"])
        assert "node_1" in neighbors.get("node_0", set())
        assert len(node_row_ids) > 0

        # 用 take 路径获取下一跳邻居
        neighbors2, node_row_ids2 = await s._take_adj_neighbors(node_row_ids)
        # node_0 的邻居是 node_1，node_1 的邻居应包含 node_0 和 node_2
        all_next_neighbors = set()
        for nset in neighbors2.values():
            all_next_neighbors.update(nset)
        assert "node_0" in all_next_neighbors or "node_2" in all_next_neighbors
        await s.finalize()

    @pytest.mark.asyncio
    async def test_adj_records_have_out_in_lists(self):
        """验证 _rebuild 后每个节点有 out / in 列表，且 _rowid 可 take 到正确的节点。"""
        s = await _make_optimized_storage(enable_adj_index_table=True)
        await _insert_test_graph(s, num_nodes=4, chain=True)
        await s._rebuild_adj_index_table()

        # 读取邻接索引表全部记录（每个节点一行）
        adj_rows = await s._adj_index_table.query().to_list()
        assert len(adj_rows) == 4  # 4 个节点 → 4 行

        # 构建 entity_id → row 映射
        adj_by_id = {r["entity_id"]: r for r in adj_rows}

        # chain: node_0→node_1→node_2→node_3
        # node_0: out=[node_1_rid], in=[]
        assert len(adj_by_id["node_0"]["out"]) == 1
        assert len(adj_by_id["node_0"]["in"]) == 0
        # node_1: out=[node_2_rid], in=[node_0_rid]
        assert len(adj_by_id["node_1"]["out"]) == 1
        assert len(adj_by_id["node_1"]["in"]) == 1
        # node_3: out=[], in=[node_2_rid]
        assert len(adj_by_id["node_3"]["out"]) == 0
        assert len(adj_by_id["node_3"]["in"]) == 1

        # 验证 out 列表中的 _rowid 可 take 到正确的节点
        out_rids = [int(v) for v in adj_by_id["node_1"]["out"]]
        node_result = await s._node_table.take_row_ids(out_rids).to_list()
        assert len(node_result) == 1
        assert node_result[0]["_id"] == "node_2"
        await s.finalize()

    @pytest.mark.asyncio
    async def test_bfs_take_path_consistency(self):
        """验证 BFS take 路径与 WHERE 路径结果一致。"""
        s = await _make_optimized_storage(enable_adj_index_table=True)
        await _insert_test_graph(s, num_nodes=8, chain=True)
        await s._rebuild_adj_index_table()

        # 禁用内存缓存，强制走邻接索引表路径
        s._cache_loaded = False
        s._adj_cache = {}
        s._node_edges_cache = {}
        s._ensure_adj_cache = lambda: asyncio.coroutine(lambda: None)()

        kg = await s.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        node_ids = {n.id for n in kg.nodes}
        # depth=3 从 node_0 出发: node_0, node_1, node_2, node_3
        assert "node_0" in node_ids
        assert "node_1" in node_ids
        assert "node_2" in node_ids
        assert "node_3" in node_ids
        assert len(kg.nodes) == 4
        await s.finalize()


class TestIndexDoneCallback:
    """index_done_callback 和生命周期测试。"""

    @pytest.mark.asyncio
    async def test_callback_invalidates_cache(self):
        """index_done_callback 后缓存被失效。"""
        s = await _make_optimized_storage()
        await _insert_test_graph(s, num_nodes=3, chain=True)
        # 预热缓存
        await s.node_degree("node_0")
        assert s._cache_loaded is True

        await s.index_done_callback()
        assert s._cache_loaded is False
        await s.finalize()


# =====================================================================
# Part 2: 性能对比测试
# =====================================================================


async def _build_large_graph(storage, n_nodes, n_extra_edges):
    """构建有 n_nodes 节点的链式图 + n_extra_edges 条随机边。"""
    import random
    random.seed(42)
    await _insert_test_graph(storage, num_nodes=n_nodes, chain=True)
    existing = set()
    for i in range(n_nodes - 1):
        existing.add((f"node_{i}", f"node_{i+1}"))
        existing.add((f"node_{i+1}", f"node_{i}"))
    count = 0
    while count < n_extra_edges:
        a = random.randint(0, n_nodes - 1)
        b = random.randint(0, n_nodes - 1)
        if a == b:
            continue
        src, tgt = f"node_{a}", f"node_{b}"
        if (src, tgt) in existing:
            continue
        existing.add((src, tgt))
        existing.add((tgt, src))
        await storage.upsert_edge(src, tgt, {"relationship": "random", "weight": 1.0})
        count += 1


class TestPerformanceComparison:
    """
    性能对比测试：OptimizedLanceDBGraphStorage vs LanceDBGraphStorage。

    使用 200 节点 + 300 条额外边的图进行测试。
    """

    N_NODES = 200
    N_EXTRA_EDGES = 300

    @pytest.fixture
    async def base_storage(self):
        s = await _make_base_storage(namespace="perf_base")
        await _build_large_graph(s, self.N_NODES, self.N_EXTRA_EDGES)
        yield s
        await s.finalize()

    @pytest.fixture
    async def opt_storage(self):
        s = await _make_optimized_storage(
            namespace="perf_opt",
            enable_adj_index_table=True,
        )
        await _build_large_graph(s, self.N_NODES, self.N_EXTRA_EDGES)
        # 重建邻接索引表
        await s._rebuild_adj_index_table()
        yield s
        await s.finalize()

    @staticmethod
    async def _time_async(coro_func, repeats=3):
        """多次执行异步函数，返回最小耗时。"""
        times = []
        for _ in range(repeats):
            start = time.perf_counter()
            await coro_func()
            elapsed = time.perf_counter() - start
            times.append(elapsed)
        return min(times)

    @pytest.mark.asyncio
    async def test_perf_node_degree_batch(self, base_storage, opt_storage):
        """性能对比：批量度数查询。"""
        node_ids = [f"node_{i}" for i in range(self.N_NODES)]

        t_base = await self._time_async(
            lambda: base_storage.node_degrees_batch(node_ids)
        )
        t_opt = await self._time_async(
            lambda: opt_storage.node_degrees_batch(node_ids)
        )

        # 验证结果一致
        d_base = await base_storage.node_degrees_batch(node_ids)
        d_opt = await opt_storage.node_degrees_batch(node_ids)
        for nid in node_ids:
            assert d_base[nid] == d_opt[nid], f"Degree mismatch for {nid}"

        speedup = t_base / t_opt if t_opt > 0 else float("inf")
        print(f"\n[node_degrees_batch] Base: {t_base:.4f}s | Opt: {t_opt:.4f}s | Speedup: {speedup:.2f}x")

    @pytest.mark.asyncio
    async def test_perf_bfs_traversal(self, base_storage, opt_storage):
        """性能对比：BFS 遍历。"""
        t_base = await self._time_async(
            lambda: base_storage.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        )
        t_opt = await self._time_async(
            lambda: opt_storage.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        )

        # 验证结果大致一致（max_nodes 截断时 BFS 访问顺序可能不同，
        # 导致截断后的子集有差异，这是正常行为）
        kg_base = await base_storage.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        kg_opt = await opt_storage.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        base_nodes = sorted(n.id for n in kg_base.nodes)
        opt_nodes = sorted(n.id for n in kg_opt.nodes)
        # 种子节点必须在结果中
        assert "node_0" in base_nodes
        assert "node_0" in opt_nodes
        # 节点数量应大致相近（允许 20% 偏差）
        ratio = len(opt_nodes) / max(len(base_nodes), 1)
        assert 0.8 <= ratio <= 1.2, (
            f"Node count mismatch: base={len(base_nodes)}, opt={len(opt_nodes)}"
        )

        speedup = t_base / t_opt if t_opt > 0 else float("inf")
        print(f"\n[BFS depth=3] Base: {t_base:.4f}s | Opt: {t_opt:.4f}s | Speedup: {speedup:.2f}x")

    @pytest.mark.asyncio
    async def test_perf_fetch_edges_between_nodes(self, base_storage, opt_storage):
        """性能对比：子集边过滤。"""
        subset = [f"node_{i}" for i in range(50)]

        t_base = await self._time_async(
            lambda: base_storage._fetch_edges_between_nodes(subset)
        )
        t_opt = await self._time_async(
            lambda: opt_storage._fetch_edges_between_nodes(subset)
        )

        # 验证结果一致
        e_base = await base_storage._fetch_edges_between_nodes(subset)
        e_opt = await opt_storage._fetch_edges_between_nodes(subset)
        base_ids = sorted(e["_id"] for e in e_base)
        opt_ids = sorted(e["_id"] for e in e_opt)
        assert base_ids == opt_ids, f"Edge IDs differ: {base_ids} vs {opt_ids}"

        speedup = t_base / t_opt if t_opt > 0 else float("inf")
        print(f"\n[_fetch_edges_between_nodes(50)] Base: {t_base:.4f}s | Opt: {t_opt:.4f}s | Speedup: {speedup:.2f}x")

    @pytest.mark.asyncio
    async def test_perf_get_knowledge_graph_star(self, base_storage, opt_storage):
        """性能对比：全图度数排序。"""
        t_base = await self._time_async(
            lambda: base_storage.get_knowledge_graph("*", max_depth=3, max_nodes=50)
        )
        t_opt = await self._time_async(
            lambda: opt_storage.get_knowledge_graph("*", max_depth=3, max_nodes=50)
        )

        speedup = t_base / t_opt if t_opt > 0 else float("inf")
        print(f"\n[get_knowledge_graph(*) max_nodes=50] Base: {t_base:.4f}s | Opt: {t_opt:.4f}s | Speedup: {speedup:.2f}x")

    @pytest.mark.asyncio
    async def test_perf_edge_degrees_batch(self, base_storage, opt_storage):
        """性能对比：边度数批量查询。"""
        edge_pairs = [(f"node_{i}", f"node_{i+1}") for i in range(self.N_NODES - 1)]

        t_base = await self._time_async(
            lambda: base_storage.edge_degrees_batch(edge_pairs)
        )
        t_opt = await self._time_async(
            lambda: opt_storage.edge_degrees_batch(edge_pairs)
        )

        speedup = t_base / t_opt if t_opt > 0 else float("inf")
        print(f"\n[edge_degrees_batch({len(edge_pairs)} edges)] Base: {t_base:.4f}s | Opt: {t_opt:.4f}s | Speedup: {speedup:.2f}x")


# =====================================================================
# Part 3: 邻接索引表专项性能测试
# =====================================================================


class TestPerformanceAdjIndexTable:
    """
    专门测试邻接索引表路径（三级回退中的第二级）的性能。

    通过将 _ensure_adj_cache 替换为 no-op async 函数使 _cache_loaded 始终
    保持 False，强制 BFS / 边过滤 / 全图度数排序走「邻接索引表」路径，
    与父类的全量 edge 表扫描进行对比。

    仅以下三个方法实现了邻接索引表回退路径（其他方法在缓存未命中时直接
    回退父类，不经过邻接索引表）：
      - get_knowledge_graph / _bidirectional_bfs_nodes
      - _fetch_edges_between_nodes
      - get_knowledge_graph("*") / get_knowledge_graph_all_by_degree
    """

    N_NODES = 100
    N_EXTRA_EDGES = 150

    @pytest.fixture
    async def base_storage(self):
        s = await _make_base_storage(namespace="perf_adj_base")
        await _build_large_graph(s, self.N_NODES, self.N_EXTRA_EDGES)
        yield s
        await s.finalize()

    @pytest.fixture
    async def adj_only_storage(self):
        """
        仅启用邻接索引表、禁用内存邻接缓存的存储实例。

        构建方式：
          1. enable_adj_index_table=True → 在 initialize() 时创建 adj_idx 表
          2. _rebuild_adj_index_table() → 将当前图写入邻接索引表
          3. 将 _ensure_adj_cache 替换为 async no-op，使 _cache_loaded 始终
             为 False，迫使代码进入邻接索引表分支
        """
        s = await _make_optimized_storage(
            namespace="perf_adj_opt",
            enable_adj_index_table=True,
        )
        await _build_large_graph(s, self.N_NODES, self.N_EXTRA_EDGES)
        await s._rebuild_adj_index_table()

        # 禁用内存缓存：no-op 使 _cache_loaded 保持 False，
        # 下游逻辑走 use_adj_table=True 分支
        async def _noop_cache():
            pass
        s._ensure_adj_cache = _noop_cache

        yield s
        await s.finalize()

    @staticmethod
    async def _time_async(coro_func, repeats=3):
        """多次执行异步函数，返回最小耗时。"""
        times = []
        for _ in range(repeats):
            start = time.perf_counter()
            await coro_func()
            elapsed = time.perf_counter() - start
            times.append(elapsed)
        return min(times)

    @pytest.mark.asyncio
    async def test_perf_adj_bfs_traversal(self, base_storage, adj_only_storage):
        """邻接索引表 BFS 遍历 vs 父类全量 edge 表扫描。"""
        t_base = await self._time_async(
            lambda: base_storage.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        )
        t_adj = await self._time_async(
            lambda: adj_only_storage.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        )

        # 种子节点必须在两者的结果中，节点数量大致相近（允许 20% 偏差）
        kg_base = await base_storage.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        kg_adj = await adj_only_storage.get_knowledge_graph("node_0", max_depth=3, max_nodes=100)
        assert "node_0" in {n.id for n in kg_base.nodes}
        assert "node_0" in {n.id for n in kg_adj.nodes}
        ratio = len(kg_adj.nodes) / max(len(kg_base.nodes), 1)
        assert 0.8 <= ratio <= 1.2, (
            f"Node count mismatch: base={len(kg_base.nodes)}, adj={len(kg_adj.nodes)}"
        )

        speedup = t_base / t_adj if t_adj > 0 else float("inf")
        print(
            f"\n[AdjTable BFS depth=3] "
            f"Base: {t_base:.4f}s | AdjTable: {t_adj:.4f}s | Speedup: {speedup:.2f}x"
        )

    @pytest.mark.asyncio
    async def test_perf_adj_fetch_edges_between_nodes(self, base_storage, adj_only_storage):
        """邻接索引表边过滤 vs 父类全量 edge 表扫描。"""
        subset = [f"node_{i}" for i in range(50)]

        t_base = await self._time_async(
            lambda: base_storage._fetch_edges_between_nodes(subset)
        )
        t_adj = await self._time_async(
            lambda: adj_only_storage._fetch_edges_between_nodes(subset)
        )

        # 验证结果一致
        e_base = await base_storage._fetch_edges_between_nodes(subset)
        e_adj = await adj_only_storage._fetch_edges_between_nodes(subset)
        assert sorted(e["_id"] for e in e_base) == sorted(e["_id"] for e in e_adj), (
            "Edge IDs differ between base and adj-table path"
        )

        speedup = t_base / t_adj if t_adj > 0 else float("inf")
        print(
            f"\n[AdjTable _fetch_edges_between_nodes(50)] "
            f"Base: {t_base:.4f}s | AdjTable: {t_adj:.4f}s | Speedup: {speedup:.2f}x"
        )

    @pytest.mark.asyncio
    async def test_perf_adj_get_knowledge_graph_star(self, base_storage, adj_only_storage):
        """邻接索引表全图度数排序 vs 父类全量 edge 表扫描。"""
        t_base = await self._time_async(
            lambda: base_storage.get_knowledge_graph("*", max_depth=3, max_nodes=50)
        )
        t_adj = await self._time_async(
            lambda: adj_only_storage.get_knowledge_graph("*", max_depth=3, max_nodes=50)
        )

        # 两者均应截断，且节点数相同
        kg_base = await base_storage.get_knowledge_graph("*", max_depth=3, max_nodes=50)
        kg_adj = await adj_only_storage.get_knowledge_graph("*", max_depth=3, max_nodes=50)
        assert kg_base.is_truncated is True
        assert kg_adj.is_truncated is True
        assert len(kg_adj.nodes) == len(kg_base.nodes), (
            f"Node count differs: base={len(kg_base.nodes)}, adj={len(kg_adj.nodes)}"
        )

        speedup = t_base / t_adj if t_adj > 0 else float("inf")
        print(
            f"\n[AdjTable get_knowledge_graph(*) max_nodes=50] "
            f"Base: {t_base:.4f}s | AdjTable: {t_adj:.4f}s | Speedup: {speedup:.2f}x"
        )


# =====================================================================
# Part 4: 预构建 DB 性能测试 —— 聚焦查询阶段，跳过 upsert
# =====================================================================
#
# 使用方法：
#   1. 先构建 DB：
#      python tests/prebuild_graph_db.py --n_nodes 5000 --n_extra_edges 15000
#   2. 再运行测试：
#      PERF_DB_DIR=tests/_prebuilt_db pytest -v -k "TestPerformancePrebuild" \
#          tests/test_lancedb_graph_optimizer.py -s
# =====================================================================

# 预构建 DB 根目录，通过环境变量指定
_PERF_DB_DIR = os.environ.get("PERF_DB_DIR", "")

# 测试重复次数（可配置）
_PREBUILD_REPEATS = int(os.environ.get("PERF_PREBUILD_REPEATS", "3"))

# 是否跳过内存缓存测试（大图场景避免 OOM）
_SKIP_MEMCACHE = os.environ.get("PERF_SKIP_MEMCACHE", "").strip().lower() in (
    "1", "true", "yes", "on",
)

# 采样上限（避免 WHERE IN (...) 子句过大 / 全表扫描导致 OOM）
_SAMPLE_NODES = int(os.environ.get("PERF_SAMPLE_NODES", "5000"))
_SAMPLE_EDGES = int(os.environ.get("PERF_SAMPLE_EDGES", "500"))


def _read_prebuild_meta(db_dir: str) -> dict[str, int]:
    """读取 prebuild_graph_db.py 写入的 meta.txt。"""
    meta_path = os.path.join(db_dir, "meta.txt")
    result = {}
    with open(meta_path) as f:
        for line in f:
            k, v = line.strip().split("=")
            result[k] = int(v)
    return result


async def _open_base_from_prebuilt(db_path: str, namespace: str):
    """从预构建目录打开 LanceDBGraphStorage（只读查询）。"""
    from lightrag.kg.lancedb_impl import LanceDBGraphStorage, ClientManager
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


async def _open_opt_from_prebuilt(
    db_path: str, namespace: str, disable_memory_cache: bool = False
):
    """从预构建目录打开 OptimizedLanceDBGraphStorage（只读查询）。

    当 disable_memory_cache=True 时，在 initialize() **之前** 就替换
    _ensure_adj_cache 为 noop，防止初始化或后续查询过程中触发全量边
    加载到内存（百万级边时会导致 OOM）。
    """
    from lightrag.kg.lancedb_graph_optimizer import OptimizedLanceDBGraphStorage
    from lightrag.kg.lancedb_impl import ClientManager
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

    # 关键：在 initialize() 之前就禁用内存缓存，
    # 防止任何代码路径（包括 initialize 内部）触发全量边表加载
    if disable_memory_cache:
        async def _noop():
            pass
        s._ensure_adj_cache = _noop
        s._cache_loaded = False
        s._adj_cache = {}
        s._node_edges_cache = {}

    await s.initialize()
    return s


@pytest.mark.skipif(
    not _PERF_DB_DIR or not os.path.isdir(_PERF_DB_DIR),
    reason="PERF_DB_DIR 未设置或不存在，跳过预构建 DB 性能测试。"
           " 先运行: python tests/prebuild_graph_db.py"
)
class TestPerformancePrebuild:
    """
    使用预构建 DB 的大规模性能测试，纯聚焦查询阶段。

    测试维度（每种查询对比 2~3 个实现）：
      - base:     LanceDBGraphStorage（父类，全量 edge 表扫描）
      - opt:      OptimizedLanceDBGraphStorage（内存缓存优先）—— 大图可跳过
      - adj_only: OptimizedLanceDBGraphStorage（仅邻接索引表，禁用内存缓存）

    环境变量：
      - PERF_DB_DIR:            预构建 DB 根目录（必需）
      - PERF_PREBUILD_REPEATS:  每项查询重复次数（默认 3）
      - PERF_SKIP_MEMCACHE:     设为 1 跳过内存缓存测试（大图避免 OOM）
      - PERF_SAMPLE_NODES:      批量度数查询采样节点数（默认 5000，避免 WHERE IN 过大）
      - PERF_SAMPLE_EDGES:      边度数查询采样边数（默认 500）

    大图使用方式（百万节点级别）：
      PERF_DB_DIR=/data/_prebuilt_db PERF_SKIP_MEMCACHE=1 \\
      pytest -v -k "TestPerformancePrebuild" tests/test_lancedb_graph_optimizer.py -s
    """

    NAMESPACE = "perf_graph"

    @pytest.fixture
    async def base_storage(self):
        from lightrag.kg.shared_storage import initialize_share_data
        initialize_share_data(workers=1)
        s = await _open_base_from_prebuilt(
            os.path.join(_PERF_DB_DIR, "base"), self.NAMESPACE
        )
        yield s
        await s.finalize()

    @pytest.fixture
    async def opt_storage(self):
        """打开预构建 opt DB（内存缓存 + 邻接索引表）。
        当 PERF_SKIP_MEMCACHE=1 时返回 None，避免大图 OOM。"""
        if _SKIP_MEMCACHE:
            yield None
            return
        from lightrag.kg.shared_storage import initialize_share_data
        initialize_share_data(workers=1)
        s = await _open_opt_from_prebuilt(
            os.path.join(_PERF_DB_DIR, "opt"), self.NAMESPACE,
            disable_memory_cache=False,
        )
        yield s
        await s.finalize()

    @pytest.fixture
    async def adj_only_storage(self):
        """打开预构建 opt DB，禁用内存缓存 → 强制走邻接索引表。"""
        from lightrag.kg.shared_storage import initialize_share_data
        initialize_share_data(workers=1)
        s = await _open_opt_from_prebuilt(
            os.path.join(_PERF_DB_DIR, "opt"), self.NAMESPACE,
            disable_memory_cache=True,
        )
        yield s
        await s.finalize()

    @staticmethod
    async def _time_async(coro_func, repeats=3):
        """多次执行异步函数，返回最小耗时。OOM / 内存不足时返回 None。"""
        import gc
        times = []
        for _ in range(repeats):
            gc.collect()  # 每次迭代前回收垃圾，降低内存峰值
            start = time.perf_counter()
            try:
                await coro_func()
            except (MemoryError, RuntimeError, OSError) as e:
                # LanceDB 内存不足时可能抛 RuntimeError 或 OSError
                print(f"\n  ⚠ OOM/资源不足: {type(e).__name__}: {e}")
                return None
            elapsed = time.perf_counter() - start
            times.append(elapsed)
        return min(times)

    @staticmethod
    def _fmt(label: str, t, t_base=None):
        """格式化单项计时输出。t 为 None 表示 OOM/SKIPPED。"""
        if t is None:
            return f"{label}: OOM "
        if t_base is not None and t_base is not None and t > 0:
            return f"{label}: {t:.4f}s ({t_base/t:.1f}x) "
        return f"{label}: {t:.4f}s "

    def _meta(self) -> dict[str, int]:
        return _read_prebuild_meta(_PERF_DB_DIR)

    # ---- 批量度数 ----
    @pytest.mark.asyncio
    async def test_perf_prebuild_node_degree_batch(
        self, base_storage, opt_storage, adj_only_storage
    ):
        """预构建 DB：批量度数查询（2~3 路对比）。"""
        meta = self._meta()
        # 采样：避免 WHERE IN (node_0, ..., node_999999) 过大导致 OOM
        sample_n = min(meta["n_nodes"], _SAMPLE_NODES)
        node_ids = [f"node_{i}" for i in range(sample_n)]
        repeats = _PREBUILD_REPEATS

        t_base = await self._time_async(
            lambda: base_storage.node_degrees_batch(node_ids), repeats
        )
        t_adj = await self._time_async(
            lambda: adj_only_storage.node_degrees_batch(node_ids), repeats
        )
        t_opt = None
        if opt_storage is not None:
            t_opt = await self._time_async(
                lambda: opt_storage.node_degrees_batch(node_ids), repeats
            )

        f = self._fmt
        parts = [f("Base", t_base)]
        parts.append(f("MemCache", t_opt, t_base) if opt_storage is not None else "MemCache: SKIPPED ")
        parts.append(f("AdjTable", t_adj, t_base))
        print(f"\n[Prebuild node_degrees_batch({sample_n})] " + "| ".join(parts))

    # ---- BFS 遍历 ----
    @pytest.mark.asyncio
    async def test_perf_prebuild_bfs(
        self, base_storage, opt_storage, adj_only_storage
    ):
        """预构建 DB：BFS 遍历（2~3 路对比）。"""
        repeats = _PREBUILD_REPEATS

        t_base = await self._time_async(
            lambda: base_storage.get_knowledge_graph(
                "node_0", max_depth=3, max_nodes=200
            ),
            repeats,
        )
        t_adj = await self._time_async(
            lambda: adj_only_storage.get_knowledge_graph(
                "node_0", max_depth=3, max_nodes=200
            ),
            repeats,
        )
        t_opt = None
        if opt_storage is not None:
            t_opt = await self._time_async(
                lambda: opt_storage.get_knowledge_graph(
                    "node_0", max_depth=3, max_nodes=200
                ),
                repeats,
            )

        f = self._fmt
        parts = [f("Base", t_base)]
        parts.append(f("MemCache", t_opt, t_base) if opt_storage is not None else "MemCache: SKIPPED ")
        parts.append(f("AdjTable", t_adj, t_base))
        print(f"\n[Prebuild BFS depth=3 max_nodes=200] " + "| ".join(parts))

    # ---- 子集边过滤 ----
    @pytest.mark.asyncio
    async def test_perf_prebuild_fetch_edges(
        self, base_storage, opt_storage, adj_only_storage
    ):
        """预构建 DB：子集边过滤（2~3 路对比）。"""
        subset = [f"node_{i}" for i in range(100)]
        repeats = _PREBUILD_REPEATS

        t_base = await self._time_async(
            lambda: base_storage._fetch_edges_between_nodes(subset), repeats
        )
        t_adj = await self._time_async(
            lambda: adj_only_storage._fetch_edges_between_nodes(subset), repeats
        )
        t_opt = None
        if opt_storage is not None:
            t_opt = await self._time_async(
                lambda: opt_storage._fetch_edges_between_nodes(subset), repeats
            )

        f = self._fmt
        parts = [f("Base", t_base)]
        parts.append(f("MemCache", t_opt, t_base) if opt_storage is not None else "MemCache: SKIPPED ")
        parts.append(f("AdjTable", t_adj, t_base))
        print(f"\n[Prebuild _fetch_edges_between_nodes(100)] " + "| ".join(parts))

    # ---- 全图度数排序 ----
    @pytest.mark.asyncio
    async def test_perf_prebuild_all_by_degree(
        self, base_storage, opt_storage, adj_only_storage
    ):
        """预构建 DB：全图度数排序取 Top-K（2~3 路对比）。
        注意：base 路径会全表扫描 edge 表，百万级边时可能 OOM。"""
        repeats = _PREBUILD_REPEATS

        t_base = await self._time_async(
            lambda: base_storage.get_knowledge_graph("*", max_depth=3, max_nodes=100),
            repeats,
        )
        t_adj = await self._time_async(
            lambda: adj_only_storage.get_knowledge_graph("*", max_depth=3, max_nodes=100),
            repeats,
        )
        t_opt = None
        if opt_storage is not None:
            t_opt = await self._time_async(
                lambda: opt_storage.get_knowledge_graph("*", max_depth=3, max_nodes=100),
                repeats,
            )

        f = self._fmt
        parts = [f("Base", t_base)]
        parts.append(f("MemCache", t_opt, t_base) if opt_storage is not None else "MemCache: SKIPPED ")
        parts.append(f("AdjTable", t_adj, t_base))
        print(f"\n[Prebuild get_knowledge_graph(*) max_nodes=100] " + "| ".join(parts))

    # ---- 边度数批量 ----
    @pytest.mark.asyncio
    async def test_perf_prebuild_edge_degrees(
        self, base_storage, opt_storage, adj_only_storage
    ):
        """预构建 DB：边度数批量查询（2~3 路对比）。"""
        meta = self._meta()
        sample_e = min(meta["n_nodes"] - 1, _SAMPLE_EDGES)
        edge_pairs = [
            (f"node_{i}", f"node_{i+1}")
            for i in range(sample_e)
        ]
        repeats = _PREBUILD_REPEATS

        t_base = await self._time_async(
            lambda: base_storage.edge_degrees_batch(edge_pairs), repeats
        )
        t_adj = await self._time_async(
            lambda: adj_only_storage.edge_degrees_batch(edge_pairs), repeats
        )
        t_opt = None
        if opt_storage is not None:
            t_opt = await self._time_async(
                lambda: opt_storage.edge_degrees_batch(edge_pairs), repeats
            )

        f = self._fmt
        parts = [f("Base", t_base)]
        parts.append(f("MemCache", t_opt, t_base) if opt_storage is not None else "MemCache: SKIPPED ")
        parts.append(f("AdjTable", t_adj, t_base))
        print(f"\n[Prebuild edge_degrees_batch({sample_e} edges)] " + "| ".join(parts))
