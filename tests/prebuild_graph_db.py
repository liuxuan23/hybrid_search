#!/usr/bin/env python
"""
预构建大规模测试图 DB，供性能测试直接复用（跳过 upsert 阶段）。

此脚本一次性生成两个独立的 LanceDB 数据集：
  - base/    : LanceDBGraphStorage（父类）
  - opt/     : OptimizedLanceDBGraphStorage（内存缓存 + 邻接索引表）

核心优化：使用 PyArrow 批量写入（table.add），而非逐条 upsert，
构建速度提升 10~100 倍。

用法：
    # 默认 1000 节点 + 3000 额外边 → tests/_prebuilt_db/
    python tests/prebuild_graph_db.py

    # 自定义规模和输出目录
    python tests/prebuild_graph_db.py --n_nodes 5000 --n_extra_edges 15000

    # 大规模压测
    python tests/prebuild_graph_db.py --n_nodes 20000 --n_extra_edges 60000

在测试中使用预构建数据：
    PERF_DB_DIR=tests/_prebuilt_db pytest -v -k "TestPerformancePrebuild" tests/test_lancedb_graph_optimizer.py -s
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import os
import random
import shutil
import sys
import time
from typing import Any

import pyarrow as pa

# 确保项目根目录在 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ---------------------------------------------------------------------------
# 纯 Python 生成图数据（不依赖任何 storage 对象）
# ---------------------------------------------------------------------------

def _generate_graph_data(
    n_nodes: int,
    n_extra_edges: int,
    seed: int = 42,
) -> tuple[list[dict], list[dict]]:
    """
    在内存中生成链式图 + 随机边，返回 (node_records, edge_records)。
    与测试中 _build_large_graph 逻辑对齐（同 seed 产生完全相同的边）。

    返回值直接可喂给 pa.Table.from_pylist()。
    """
    from lightrag.kg.lancedb_impl import _make_edge_id

    # ---- 节点 ----
    node_records: list[dict] = []
    for i in range(n_nodes):
        nid = f"node_{i}"
        node_records.append({
            "_id": nid,
            "entity_id": nid,
            "entity_type": "test",
            "description": f"Test node {i}",
            # 以下字段填 None（schema 中存在但此处无需）
            "keywords": None,
            "source_id": None,
            "file_path": None,
            "source_ids": None,
            "created_at": None,
        })

    # ---- 边（去重用 int-pair，比 str-pair 快且省内存）----
    rng = random.Random(seed)
    # 无向去重集：保存 (min_i, max_i)
    existing: set[tuple[int, int]] = set()
    for i in range(n_nodes - 1):
        existing.add((i, i + 1))

    # 先收集所有 (a, b, relationship)
    all_pairs: list[tuple[int, int, str]] = []
    # 链式边
    for i in range(n_nodes - 1):
        all_pairs.append((i, i + 1, "next"))
    # 随机边
    count = 0
    while count < n_extra_edges:
        a = rng.randint(0, n_nodes - 1)
        b = rng.randint(0, n_nodes - 1)
        if a == b:
            continue
        lo, hi = (a, b) if a < b else (b, a)
        if (lo, hi) in existing:
            continue
        existing.add((lo, hi))
        all_pairs.append((a, b, "random"))
        count += 1

    # 转成 edge records
    edge_records: list[dict] = []
    for a, b, rel in all_pairs:
        src, tgt = f"node_{a}", f"node_{b}"
        edge_records.append({
            "_id": _make_edge_id(src, tgt),
            "source_node_id": src,
            "target_node_id": tgt,
            "relationship": rel,
            "weight": 1.0,
            "description": f"Edge {a}->{b}" if rel == "next" else None,
            "keywords": None,
            "source_id": None,
            "file_path": None,
            "source_ids": None,
            "created_at": None,
        })

    return node_records, edge_records


def _generate_adj_index_records(
    node_records: list[dict],
    edge_records: list[dict],
) -> list[dict]:
    """
    根据已生成的 node/edge records 在内存中直接构造邻接索引表 records，
    避免写入 edge 表后再 _rebuild_adj_index_table() 全表扫描一次。
    """
    from lightrag.kg.lancedb_impl import _make_edge_id

    node_id_to_row = {r["_id"]: i for i, r in enumerate(node_records)}

    adj_records: list[dict] = []
    for ei, e in enumerate(edge_records):
        src = e["source_node_id"]
        tgt = e["target_node_id"]
        eid = e["_id"]
        w = float(e.get("weight") or 0.0)
        ns = node_id_to_row.get(src, -1)
        nt = node_id_to_row.get(tgt, -1)
        adj_records.append({
            "entity_id": src, "next_hop_id": tgt, "edge_id": eid,
            "edge_row_id": ei, "node_row_id": ns, "weight": w,
        })
        adj_records.append({
            "entity_id": tgt, "next_hop_id": src, "edge_id": eid,
            "edge_row_id": ei, "node_row_id": nt, "weight": w,
        })
    return adj_records


# ---------------------------------------------------------------------------
# 批量写入 LanceDB
# ---------------------------------------------------------------------------

async def _maybe_await(v: Any) -> Any:
    if inspect.isawaitable(v):
        return await v
    return v


async def _bulk_write_table(table: Any, records: list[dict], schema: pa.Schema, batch_size: int = 100):
    """将 records 分块写入 LanceDB table（使用 table.add 批量追加）。"""
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        arrow_tbl = pa.Table.from_pylist(chunk, schema=schema)
        await _maybe_await(table.add(arrow_tbl))


# ---------------------------------------------------------------------------
# 构建单个 DB（批量写入版）
# ---------------------------------------------------------------------------

async def _build_one_db(
    db_path: str,
    namespace: str,
    node_records: list[dict],
    edge_records: list[dict],
    adj_records: list[dict] | None = None,
    is_optimized: bool = False,
    enable_adj_index: bool = False,
    batch_size: int = 10000,
):
    """
    构建一个 LanceDB 数据集并写入 db_path。

    直接使用 table.add() 批量追加预先在内存中生成好的 records，
    而不是逐条 upsert_node/upsert_edge。
    """
    from lightrag.kg.lancedb_impl import ClientManager, GRAPH_NODE_SCHEMA, GRAPH_EDGE_SCHEMA

    # 每次构建时重置 ClientManager 并设置 URI
    ClientManager._instances = {"db": None, "ref_count": 0}
    os.environ["LANCEDB_URI"] = db_path

    if is_optimized:
        from lightrag.kg.lancedb_graph_optimizer import OptimizedLanceDBGraphStorage
        s = OptimizedLanceDBGraphStorage(
            namespace=namespace,
            global_config={"max_graph_nodes": max(len(node_records) * 2, 10000)},
            embedding_func=None,
            workspace="",
            enable_adj_index_table=enable_adj_index,
            enable_physical_clustering=False,
        )
    else:
        from lightrag.kg.lancedb_impl import LanceDBGraphStorage
        s = LanceDBGraphStorage(
            namespace=namespace,
            global_config={"max_graph_nodes": max(len(node_records) * 2, 10000)},
            embedding_func=None,
            workspace="",
        )

    await s.initialize()

    # 批量写入节点（直接操作底层 _node_table）
    print(f"    Writing {len(node_records)} nodes (batch={batch_size}) ...")
    t = time.perf_counter()
    await _bulk_write_table(s._node_table, node_records, GRAPH_NODE_SCHEMA, batch_size)
    print(f"    Nodes done in {time.perf_counter() - t:.1f}s")

    # 批量写入边
    print(f"    Writing {len(edge_records)} edges (batch={batch_size}) ...")
    t = time.perf_counter()
    await _bulk_write_table(s._edge_table, edge_records, GRAPH_EDGE_SCHEMA, batch_size)
    print(f"    Edges done in {time.perf_counter() - t:.1f}s")

    # 邻接索引表：直接从内存 adj_records 写入，不走 _rebuild（避免再全表扫描一次）
    if is_optimized and enable_adj_index and adj_records:
        from lightrag.kg.lancedb_graph_optimizer import KG_ADJ_INDEX_SCHEMA
        adj_name = f"{s._node_table_name}_adj_idx"
        print(f"    Writing {len(adj_records)} adj index rows → {adj_name} ...")
        t = time.perf_counter()
        adj_arrow = pa.Table.from_pylist(adj_records, schema=KG_ADJ_INDEX_SCHEMA)
        s._adj_index_table = await s.db.create_table(adj_name, adj_arrow, mode="overwrite")
        # 创建 BTREE 标量索引
        if hasattr(s._adj_index_table, "create_scalar_index"):
            try:
                await _maybe_await(
                    s._adj_index_table.create_scalar_index("entity_id", index_type="BTREE")
                )
            except Exception:
                pass
        print(f"    Adj index done in {time.perf_counter() - t:.1f}s")

    await s.finalize()
    # 释放 ClientManager
    ClientManager._instances = {"db": None, "ref_count": 0}
    if "LANCEDB_URI" in os.environ:
        del os.environ["LANCEDB_URI"]


# ---------------------------------------------------------------------------
# 写元数据文件（供测试读取参数）
# ---------------------------------------------------------------------------

def _write_meta(db_dir: str, n_nodes: int, n_extra_edges: int):
    """在 db_dir 下写 meta.txt，记录图规模参数。"""
    meta_path = os.path.join(db_dir, "meta.txt")
    with open(meta_path, "w") as f:
        f.write(f"n_nodes={n_nodes}\n")
        f.write(f"n_extra_edges={n_extra_edges}\n")


def read_meta(db_dir: str) -> dict[str, int]:
    """读取 meta.txt，返回 {n_nodes, n_extra_edges}。"""
    meta_path = os.path.join(db_dir, "meta.txt")
    result = {}
    with open(meta_path) as f:
        for line in f:
            k, v = line.strip().split("=")
            result[k] = int(v)
    return result


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

async def main(n_nodes: int, n_extra_edges: int, db_dir: str, batch_size: int, seed: int):
    from lightrag.kg.shared_storage import initialize_share_data
    initialize_share_data(workers=1)

    os.makedirs(db_dir, exist_ok=True)

    namespace = "perf_graph"
    total_edges = n_nodes - 1 + n_extra_edges

    # ---- 0) 在内存中一次性生成全部 records ----
    print(f"\n[0/3] Generating graph data in memory: {n_nodes} nodes, {total_edges} edges ...")
    t0 = time.perf_counter()
    node_records, edge_records = _generate_graph_data(n_nodes, n_extra_edges, seed=seed)
    print(f"  Generated {len(node_records)} node records + {len(edge_records)} edge records "
          f"in {time.perf_counter() - t0:.1f}s")

    # ---- 0.5) 在内存中生成邻接索引表 records（仅 opt 需要）----
    print(f"\n[1/3] Generating adjacency index records in memory ...")
    t0 = time.perf_counter()
    adj_records = _generate_adj_index_records(node_records, edge_records)
    print(f"  Generated {len(adj_records)} adj index records in {time.perf_counter() - t0:.1f}s")

    # ---- 1) Base storage ----
    base_path = os.path.join(db_dir, "base")
    if os.path.exists(base_path):
        shutil.rmtree(base_path)
    os.makedirs(base_path)
    print(f"\n[2/3] Building BASE storage → {base_path}")
    t0 = time.perf_counter()
    await _build_one_db(
        base_path, namespace, node_records, edge_records,
        is_optimized=False,
        batch_size=batch_size,
    )
    print(f"  Total: {time.perf_counter() - t0:.1f}s")

    # ---- 2) Optimized storage（内存缓存 + 邻接索引表）----
    opt_path = os.path.join(db_dir, "opt")
    if os.path.exists(opt_path):
        shutil.rmtree(opt_path)
    os.makedirs(opt_path)
    print(f"\n[3/3] Building OPTIMIZED storage (adj index) → {opt_path}")
    t0 = time.perf_counter()
    await _build_one_db(
        opt_path, namespace, node_records, edge_records,
        adj_records=adj_records,
        is_optimized=True,
        enable_adj_index=True,
        batch_size=batch_size,
    )
    print(f"  Total: {time.perf_counter() - t0:.1f}s")

    # ---- 写元数据 ----
    _write_meta(db_dir, n_nodes, n_extra_edges)

    print(f"\n✅ Prebuilt DB ready at: {db_dir}")
    print(f"   meta: {n_nodes} nodes, {total_edges} edges (chain + {n_extra_edges} random)")
    print(f"\n使用方式：")
    print(f"   PERF_DB_DIR={db_dir} pytest -v -k 'TestPerformancePrebuild' tests/test_lancedb_graph_optimizer.py -s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="预构建大规模图 DB 供性能测试复用")
    parser.add_argument("--n_nodes", type=int, default=1000,
                        help="节点数量（默认 1000）")
    parser.add_argument("--n_extra_edges", type=int, default=3000,
                        help="链式图之外的随机边数量（默认 3000）")
    parser.add_argument("--db_dir", type=str,
                        default=os.path.join(os.path.dirname(__file__), "_prebuilt_db"),
                        help="输出目录（默认 tests/_prebuilt_db）")
    parser.add_argument("--batch_size", type=int, default=10000,
                        help="每批写入行数（默认 10000）")
    parser.add_argument("--seed", type=int, default=42,
                        help="随机种子（默认 42）")
    args = parser.parse_args()
    asyncio.run(main(args.n_nodes, args.n_extra_edges, args.db_dir, args.batch_size, args.seed))
