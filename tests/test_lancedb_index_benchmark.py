#!/usr/bin/env python
"""
LanceDB 标量索引基准测试：对比 NoIndex / BTree / Bitmap / FTS 对 WHERE 查询的加速效果。

本文件独立于 OptimizedLanceDBGraphStorage，直接操作 LanceDB AsyncTable，
聚焦于索引本身的性能差异，不涉及优化层逻辑。

使用预构建 DB（与 prebuild_graph_db.py 共享），也可自建临时表。

运行方式：
    # 使用预构建 DB（推荐，数据量大、结果更稳定）
    PERF_DB_DIR=tests/_prebuilt_db_100k \
    pytest tests/test_lancedb_index_benchmark.py -v -s

    # 仅运行自建数据的小规模测试（无需预构建 DB）
    pytest tests/test_lancedb_index_benchmark.py -v -s -k "TestSelfBuilt"

    # 调整参数
    PERF_DB_DIR=tests/_prebuilt_db_100k \
    PERF_IDX_REPEATS=10 PERF_IDX_IN_SIZE=100 \
    pytest tests/test_lancedb_index_benchmark.py -v -s -k "TestPrebuilt"

环境变量：
    PERF_DB_DIR          预构建 DB 根目录（TestPrebuilt* 系列需要）
    PERF_IDX_REPEATS     每项查询重复次数，取最小值（默认 5）
    PERF_IDX_IN_SIZE     WHERE IN (...) 子句元素数量（默认 50）
"""

import asyncio
import gc
import os
import random
import shutil
import sys
import tempfile
import time
from typing import Any

import pyarrow as pa
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

_PERF_DB_DIR = os.environ.get("PERF_DB_DIR", "")
_REPEATS = int(os.environ.get("PERF_IDX_REPEATS", "5"))
_IN_SIZE = int(os.environ.get("PERF_IDX_IN_SIZE", "50"))
_NAMESPACE = "perf_graph"


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

async def _time_async(coro_func, repeats: int = _REPEATS) -> float:
    """多次执行异步函数，返回最小耗时（秒）。"""
    times: list[float] = []
    for _ in range(repeats):
        gc.collect()
        t0 = time.perf_counter()
        await coro_func()
        times.append(time.perf_counter() - t0)
    return min(times)


def _fmt(label: str, t: float, t_base: float | None = None) -> str:
    if t_base is not None and t_base > 0 and t > 0:
        return f"{label}: {t*1000:.1f}ms ({t_base/t:.1f}x)"
    return f"{label}: {t*1000:.1f}ms"


def _build_where_in(column: str, values: list[str]) -> str:
    """构建 WHERE column IN ('v1','v2',...) 子句。"""
    escaped = ", ".join(f"'{v}'" for v in values)
    return f"{column} IN ({escaped})"


async def _drop_all_indices(table: Any) -> None:
    """删除表上的所有索引。"""
    indices = await table.list_indices()
    for idx in indices:
        try:
            await table.drop_index(idx.name)
        except Exception:
            pass


async def _create_index(table: Any, column: str, config: Any, name: str) -> bool:
    """创建索引，失败时返回 False。"""
    try:
        await table.create_index(column, config=config, name=name, replace=True)
        return True
    except Exception as e:
        print(f"  ⚠ create_index({name}) failed: {e}")
        return False


async def _bench_index_types(
    table: Any,
    column: str,
    where_clause: str,
    select_cols: list[str],
    label: str,
    repeats: int = _REPEATS,
) -> dict[str, float | None]:
    """
    对同一张表、同一条 WHERE 查询，依次测试 NoIndex / BTree / Bitmap 的耗时。

    返回 {index_type_name: time_seconds}，None 表示不支持或创建失败。
    """
    from lancedb.index import BTree, Bitmap

    async def query():
        await table.query().where(where_clause).select(select_cols).to_list()

    results: dict[str, float | None] = {}

    # ---- 1) NoIndex ----
    await _drop_all_indices(table)
    results["NoIndex"] = await _time_async(query, repeats)

    # ---- 2) BTree ----
    ok = await _create_index(table, column, BTree(), f"bench_btree_{column}")
    if ok:
        results["BTree"] = await _time_async(query, repeats)
    else:
        results["BTree"] = None
    await _drop_all_indices(table)

    # ---- 3) Bitmap ----
    ok = await _create_index(table, column, Bitmap(), f"bench_bitmap_{column}")
    if ok:
        results["Bitmap"] = await _time_async(query, repeats)
    else:
        results["Bitmap"] = None
    await _drop_all_indices(table)

    # ---- 打印 ----
    t_base = results["NoIndex"]
    parts = [_fmt("NoIndex", t_base)]
    for idx_type in ["BTree", "Bitmap"]:
        t = results.get(idx_type)
        if t is None:
            parts.append(f"{idx_type}: N/A")
        else:
            parts.append(_fmt(idx_type, t, t_base))
    print(f"\n[{label}] {' | '.join(parts)}")

    return results


async def _bench_single_eq(
    table: Any,
    column: str,
    value: str,
    select_cols: list[str],
    label: str,
    repeats: int = _REPEATS,
) -> dict[str, float | None]:
    """
    单值等值查询 WHERE column = 'value' 的索引对比。
    """
    from lancedb.index import BTree, Bitmap

    where_clause = f"{column} = '{value}'"

    async def query():
        await table.query().where(where_clause).select(select_cols).to_list()

    results: dict[str, float | None] = {}

    await _drop_all_indices(table)
    results["NoIndex"] = await _time_async(query, repeats)

    ok = await _create_index(table, column, BTree(), f"bench_btree_{column}")
    if ok:
        results["BTree"] = await _time_async(query, repeats)
    else:
        results["BTree"] = None
    await _drop_all_indices(table)

    ok = await _create_index(table, column, Bitmap(), f"bench_bitmap_{column}")
    if ok:
        results["Bitmap"] = await _time_async(query, repeats)
    else:
        results["Bitmap"] = None
    await _drop_all_indices(table)

    t_base = results["NoIndex"]
    parts = [_fmt("NoIndex", t_base)]
    for idx_type in ["BTree", "Bitmap"]:
        t = results.get(idx_type)
        if t is None:
            parts.append(f"{idx_type}: N/A")
        else:
            parts.append(_fmt(idx_type, t, t_base))
    print(f"\n[{label}] {' | '.join(parts)}")

    return results


# ---------------------------------------------------------------------------
# Part 1: 使用预构建 DB 的大规模测试
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _PERF_DB_DIR or not os.path.isdir(_PERF_DB_DIR),
    reason="PERF_DB_DIR 未设置或不存在。先运行: python tests/prebuild_graph_db.py",
)
class TestPrebuiltEdgeTable:
    """edge 表（~400K 行）上的索引基准。"""

    @pytest.fixture
    async def edge_table(self):
        import lancedb
        db = await lancedb.connect_async(os.path.join(_PERF_DB_DIR, "base"))
        tbl = await db.open_table(f"{_NAMESPACE}_edges")
        # 测试完成后恢复原索引
        orig_indices = await tbl.list_indices()
        yield tbl
        # 恢复
        await _drop_all_indices(tbl)
        from lancedb.index import BTree
        for idx in orig_indices:
            try:
                config = BTree()  # 原始 DB 默认用 BTree
                await _create_index(tbl, idx.columns[0], config, idx.name)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_source_node_id_where_in(self, edge_table):
        """edge.source_node_id — WHERE IN(N) 多值查询。"""
        sample = [f"node_{i}" for i in range(_IN_SIZE)]
        where = _build_where_in("source_node_id", sample)
        await _bench_index_types(
            edge_table, "source_node_id", where, ["_id"],
            f"edge.source_node_id WHERE IN({_IN_SIZE})",
        )

    @pytest.mark.asyncio
    async def test_target_node_id_where_in(self, edge_table):
        """edge.target_node_id — WHERE IN(N) 多值查询。"""
        sample = [f"node_{i}" for i in range(_IN_SIZE)]
        where = _build_where_in("target_node_id", sample)
        await _bench_index_types(
            edge_table, "target_node_id", where, ["_id"],
            f"edge.target_node_id WHERE IN({_IN_SIZE})",
        )

    @pytest.mark.asyncio
    async def test_edge_id_where_in(self, edge_table):
        """edge._id — WHERE IN(N) 多值查询。"""
        sample = [f"node_{i}||node_{i+1}" for i in range(_IN_SIZE)]
        where = _build_where_in("_id", sample)
        await _bench_index_types(
            edge_table, "_id", where,
            ["_id", "source_node_id", "target_node_id"],
            f"edge._id WHERE IN({_IN_SIZE})",
        )

    @pytest.mark.asyncio
    async def test_edge_id_single_eq(self, edge_table):
        """edge._id — 单值等值查询。"""
        await _bench_single_eq(
            edge_table, "_id", "node_500||node_501",
            ["_id", "source_node_id", "target_node_id"],
            "edge._id single EQ",
        )

    @pytest.mark.asyncio
    async def test_source_node_single_eq(self, edge_table):
        """edge.source_node_id — 单值等值查询。"""
        await _bench_single_eq(
            edge_table, "source_node_id", "node_500",
            ["_id"],
            "edge.source_node_id single EQ",
        )


@pytest.mark.skipif(
    not _PERF_DB_DIR or not os.path.isdir(_PERF_DB_DIR),
    reason="PERF_DB_DIR 未设置或不存在。",
)
class TestPrebuiltNodeTable:
    """node 表（~100K 行）上的索引基准。"""

    @pytest.fixture
    async def node_table(self):
        import lancedb
        db = await lancedb.connect_async(os.path.join(_PERF_DB_DIR, "base"))
        tbl = await db.open_table(_NAMESPACE)
        orig_indices = await tbl.list_indices()
        yield tbl
        await _drop_all_indices(tbl)
        from lancedb.index import BTree
        for idx in orig_indices:
            try:
                await _create_index(tbl, idx.columns[0], BTree(), idx.name)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_node_id_where_in(self, node_table):
        """node._id — WHERE IN(N) 多值查询。"""
        sample = [f"node_{i}" for i in range(_IN_SIZE)]
        where = _build_where_in("_id", sample)
        await _bench_index_types(
            node_table, "_id", where, ["_id", "entity_type"],
            f"node._id WHERE IN({_IN_SIZE})",
        )

    @pytest.mark.asyncio
    async def test_node_id_single_eq(self, node_table):
        """node._id — 单值等值查询。"""
        await _bench_single_eq(
            node_table, "_id", "node_5000",
            ["_id", "entity_type", "description"],
            "node._id single EQ",
        )

    @pytest.mark.asyncio
    async def test_node_id_where_in_large(self, node_table):
        """node._id — WHERE IN(500) 大批量查询。"""
        sample = [f"node_{i}" for i in range(500)]
        where = _build_where_in("_id", sample)
        await _bench_index_types(
            node_table, "_id", where, ["_id"],
            "node._id WHERE IN(500)",
        )


@pytest.mark.skipif(
    not _PERF_DB_DIR or not os.path.isdir(_PERF_DB_DIR),
    reason="PERF_DB_DIR 未设置或不存在。",
)
class TestPrebuiltAdjTable:
    """adj 邻接索引表（~800K 行）上的索引基准。"""

    @pytest.fixture
    async def adj_table(self):
        import lancedb
        db = await lancedb.connect_async(os.path.join(_PERF_DB_DIR, "opt"))
        try:
            tbl = await db.open_table(f"{_NAMESPACE}_adj_idx")
        except ValueError:
            pytest.skip("adj index table not found in prebuilt DB")
        orig_indices = await tbl.list_indices()
        yield tbl
        await _drop_all_indices(tbl)
        from lancedb.index import BTree
        for idx in orig_indices:
            try:
                await _create_index(tbl, idx.columns[0], BTree(), idx.name)
            except Exception:
                pass

    @pytest.mark.asyncio
    async def test_entity_id_where_in(self, adj_table):
        """adj.entity_id — WHERE IN(N) 多值查询。"""
        sample = [f"node_{i}" for i in range(_IN_SIZE)]
        where = _build_where_in("entity_id", sample)
        await _bench_index_types(
            adj_table, "entity_id", where,
            ["entity_id", "out", "in"],
            f"adj.entity_id WHERE IN({_IN_SIZE})",
        )

    @pytest.mark.asyncio
    async def test_entity_id_single_eq(self, adj_table):
        """adj.entity_id — 单值等值查询。"""
        await _bench_single_eq(
            adj_table, "entity_id", "node_500",
            ["entity_id", "out", "in"],
            "adj.entity_id single EQ",
        )

    @pytest.mark.asyncio
    async def test_entity_id_where_in_large(self, adj_table):
        """adj.entity_id — WHERE IN(200) 大批量查询。"""
        sample = [f"node_{i}" for i in range(200)]
        where = _build_where_in("entity_id", sample)
        await _bench_index_types(
            adj_table, "entity_id", where,
            ["entity_id", "out", "in"],
            "adj.entity_id WHERE IN(200)",
        )


# ---------------------------------------------------------------------------
# Part 2: 自建数据的小规模测试（无需预构建 DB）
# ---------------------------------------------------------------------------


class TestSelfBuiltSmallTable:
    """在临时目录自建小表（1K/10K 行），测试索引对小数据集的效果。"""

    @pytest.fixture
    async def tables_1k(self, tmp_path):
        """创建 1,000 行的表。"""
        import lancedb
        db = await lancedb.connect_async(str(tmp_path / "db_1k"))
        data = pa.table({
            "_id": [f"id_{i}" for i in range(1000)],
            "category": [f"cat_{i % 20}" for i in range(1000)],
            "value": list(range(1000)),
        })
        tbl = await db.create_table("bench_1k", data)
        yield tbl

    @pytest.fixture
    async def tables_10k(self, tmp_path):
        """创建 10,000 行的表。"""
        import lancedb
        db = await lancedb.connect_async(str(tmp_path / "db_10k"))
        data = pa.table({
            "_id": [f"id_{i}" for i in range(10000)],
            "category": [f"cat_{i % 100}" for i in range(10000)],
            "value": list(range(10000)),
        })
        tbl = await db.create_table("bench_10k", data)
        yield tbl

    @pytest.mark.asyncio
    async def test_1k_id_where_in(self, tables_1k):
        """1K 行: _id WHERE IN(20)。"""
        sample = [f"id_{i}" for i in range(20)]
        where = _build_where_in("_id", sample)
        await _bench_index_types(
            tables_1k, "_id", where, ["_id"],
            "1K rows: _id WHERE IN(20)",
        )

    @pytest.mark.asyncio
    async def test_1k_category_where_in(self, tables_1k):
        """1K 行: category WHERE IN(5)（低基数列，适合 Bitmap）。"""
        sample = [f"cat_{i}" for i in range(5)]
        where = _build_where_in("category", sample)
        await _bench_index_types(
            tables_1k, "category", where, ["_id", "category"],
            "1K rows: category WHERE IN(5) [low cardinality]",
        )

    @pytest.mark.asyncio
    async def test_10k_id_where_in(self, tables_10k):
        """10K 行: _id WHERE IN(50)。"""
        sample = [f"id_{i}" for i in range(50)]
        where = _build_where_in("_id", sample)
        await _bench_index_types(
            tables_10k, "_id", where, ["_id"],
            "10K rows: _id WHERE IN(50)",
        )

    @pytest.mark.asyncio
    async def test_10k_category_where_in(self, tables_10k):
        """10K 行: category WHERE IN(10)（低基数列）。"""
        sample = [f"cat_{i}" for i in range(10)]
        where = _build_where_in("category", sample)
        await _bench_index_types(
            tables_10k, "category", where, ["_id", "category"],
            "10K rows: category WHERE IN(10) [low cardinality]",
        )

    @pytest.mark.asyncio
    async def test_10k_category_single_eq(self, tables_10k):
        """10K 行: category 单值等值（低基数列）。"""
        await _bench_single_eq(
            tables_10k, "category", "cat_5",
            ["_id", "category"],
            "10K rows: category single EQ [low cardinality]",
        )

    @pytest.mark.asyncio
    async def test_10k_id_single_eq(self, tables_10k):
        """10K 行: _id 单值等值（高基数列）。"""
        await _bench_single_eq(
            tables_10k, "_id", "id_5000",
            ["_id", "category", "value"],
            "10K rows: _id single EQ [high cardinality]",
        )


class TestSelfBuiltLargeTable:
    """自建 100K 行表，模拟更接近实际的规模。"""

    @pytest.fixture
    async def table_100k(self, tmp_path):
        """创建 100,000 行的表。"""
        import lancedb
        db = await lancedb.connect_async(str(tmp_path / "db_100k"))
        n = 100_000
        data = pa.table({
            "_id": [f"id_{i}" for i in range(n)],
            "source": [f"src_{i % 5000}" for i in range(n)],
            "target": [f"tgt_{i % 5000}" for i in range(n)],
            "type": [f"type_{i % 10}" for i in range(n)],
        })
        tbl = await db.create_table("bench_100k", data)
        yield tbl

    @pytest.mark.asyncio
    async def test_100k_id_where_in(self, table_100k):
        """100K 行: _id WHERE IN(50)（高基数）。"""
        sample = [f"id_{i}" for i in range(50)]
        where = _build_where_in("_id", sample)
        await _bench_index_types(
            table_100k, "_id", where, ["_id"],
            "100K rows: _id WHERE IN(50) [high cardinality]",
        )

    @pytest.mark.asyncio
    async def test_100k_source_where_in(self, table_100k):
        """100K 行: source WHERE IN(50)（中等基数 5000）。"""
        sample = [f"src_{i}" for i in range(50)]
        where = _build_where_in("source", sample)
        await _bench_index_types(
            table_100k, "source", where, ["_id", "source"],
            "100K rows: source WHERE IN(50) [mid cardinality 5000]",
        )

    @pytest.mark.asyncio
    async def test_100k_type_where_in(self, table_100k):
        """100K 行: type WHERE IN(3)（极低基数 10）。"""
        sample = [f"type_{i}" for i in range(3)]
        where = _build_where_in("type", sample)
        await _bench_index_types(
            table_100k, "type", where, ["_id", "type"],
            "100K rows: type WHERE IN(3) [low cardinality 10]",
        )

    @pytest.mark.asyncio
    async def test_100k_type_single_eq(self, table_100k):
        """100K 行: type 单值等值（极低基数）。"""
        await _bench_single_eq(
            table_100k, "type", "type_3",
            ["_id", "type"],
            "100K rows: type single EQ [low cardinality 10]",
        )

    @pytest.mark.asyncio
    async def test_100k_source_single_eq(self, table_100k):
        """100K 行: source 单值等值（中等基数）。"""
        await _bench_single_eq(
            table_100k, "source", "src_2500",
            ["_id", "source"],
            "100K rows: source single EQ [mid cardinality 5000]",
        )


# ---------------------------------------------------------------------------
# Part 3: take_row_ids vs WHERE 对比（索引无关，作为参考基线）
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _PERF_DB_DIR or not os.path.isdir(_PERF_DB_DIR),
    reason="PERF_DB_DIR 未设置或不存在。",
)
class TestTakeVsWhere:
    """对比 take_row_ids（O(1) 直接定位）与 WHERE 查询，量化索引的理论上限。"""

    @pytest.fixture
    async def edge_table(self):
        import lancedb
        db = await lancedb.connect_async(os.path.join(_PERF_DB_DIR, "base"))
        tbl = await db.open_table(f"{_NAMESPACE}_edges")
        yield tbl

    @pytest.mark.asyncio
    async def test_take_vs_where_small(self, edge_table):
        """50 行: take_row_ids vs WHERE _id IN(50)。"""
        # 获取前 50 行的 _rowid
        rows = (
            await edge_table.query()
            .with_row_id()
            .select(["_id"])
            .limit(50)
            .to_list()
        )
        row_ids = [int(r["_rowid"]) for r in rows]
        edge_ids = [r["_id"] for r in rows]

        async def via_take():
            await edge_table.take_row_ids(row_ids).to_list()

        async def via_where():
            where = _build_where_in("_id", edge_ids)
            await edge_table.query().where(where).to_list()

        t_take = await _time_async(via_take, _REPEATS)
        t_where = await _time_async(via_where, _REPEATS)

        print(f"\n[take_row_ids vs WHERE IN(50)] "
              f"{_fmt('take', t_take)} | {_fmt('WHERE', t_where, t_take)}")

    @pytest.mark.asyncio
    async def test_take_vs_where_medium(self, edge_table):
        """200 行: take_row_ids vs WHERE _id IN(200)。"""
        rows = (
            await edge_table.query()
            .with_row_id()
            .select(["_id"])
            .limit(200)
            .to_list()
        )
        row_ids = [int(r["_rowid"]) for r in rows]
        edge_ids = [r["_id"] for r in rows]

        async def via_take():
            await edge_table.take_row_ids(row_ids).to_list()

        async def via_where():
            where = _build_where_in("_id", edge_ids)
            await edge_table.query().where(where).to_list()

        t_take = await _time_async(via_take, _REPEATS)
        t_where = await _time_async(via_where, _REPEATS)

        print(f"\n[take_row_ids vs WHERE IN(200)] "
              f"{_fmt('take', t_take)} | {_fmt('WHERE', t_where, t_take)}")

    @pytest.mark.asyncio
    async def test_take_vs_where_scattered(self, edge_table):
        """50 行（分散 _rowid）: take_row_ids vs WHERE。"""
        # 获取总行数，均匀采样 50 个 _rowid
        count = await edge_table.count_rows()
        step = max(1, count // 50)
        # 先取一批有 _rowid 的行
        rows = (
            await edge_table.query()
            .with_row_id()
            .select(["_id"])
            .limit(step * 50)
            .to_list()
        )
        sampled = rows[::step][:50]
        row_ids = [int(r["_rowid"]) for r in sampled]
        edge_ids = [r["_id"] for r in sampled]

        async def via_take():
            await edge_table.take_row_ids(row_ids).to_list()

        async def via_where():
            where = _build_where_in("_id", edge_ids)
            await edge_table.query().where(where).to_list()

        t_take = await _time_async(via_take, _REPEATS)
        t_where = await _time_async(via_where, _REPEATS)

        print(f"\n[take(scattered 50) vs WHERE IN(50)] "
              f"{_fmt('take', t_take)} | {_fmt('WHERE', t_where, t_take)}")
