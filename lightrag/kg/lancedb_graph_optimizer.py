"""
LanceDB 图存储优化层：在 LanceDBGraphStorage 基础上增加标量索引、内存邻接缓存、
可选邻接索引表与物理聚簇，提升度数/邻居查询性能。
"""

from __future__ import annotations

import asyncio
import inspect
import os
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from ..types import KnowledgeGraph
from ..utils import logger
from .lancedb_impl import (
    LanceDBGraphStorage,
    get_or_create_table,
    GRAPH_NODE_SCHEMA,
    GRAPH_EDGE_SCHEMA,
    _make_edge_id,
    _build_where_in,
)

# ---------------------------------------------------------------------------
# Section 1: 标量索引管理
# ---------------------------------------------------------------------------

# 邻接索引表 Schema（Section 3）— 每个节点一行
# entity_id: 节点 ID（主键，有 BTREE 索引）
# out: 出邻居在 **node 表** 中的 Lance _rowid 列表（有向：src→tgt）
# in:  入邻居在 **node 表** 中的 Lance _rowid 列表（有向：tgt←src）
# BFS 无向遍历时合并 out+in；度数 = len(out)+len(in)。
KG_ADJ_INDEX_SCHEMA = pa.schema([
    pa.field("entity_id", pa.utf8(), nullable=False),
    pa.field("out", pa.list_(pa.int64())),
    pa.field("in", pa.list_(pa.int64())),
])


async def _create_scalar_index_safe(table: Any, column: str, index_type: str = "BTREE") -> None:
    """为表的一列创建标量索引，已存在或失败时静默跳过。"""
    try:
        if hasattr(table, "create_scalar_index"):
            # 新版 LanceDB: AsyncTable.create_scalar_index
            result = table.create_scalar_index(column, index_type=index_type)
            if inspect.isawaitable(result):
                await result
        elif hasattr(table, "create_index"):
            try:
                # 旧版 LanceDB: create_index + BTree config
                from lancedb.index import BTree
                result = table.create_index(column=column, config=BTree())
                if inspect.isawaitable(result):
                    await result
            except ImportError:
                pass
    except Exception as e:
        logger.debug(f"Scalar index on {column} skipped: {e}")


# ---------------------------------------------------------------------------
# Section 2 & 3 & 4 & 5: OptimizedLanceDBGraphStorage
# ---------------------------------------------------------------------------


@dataclass
class OptimizedLanceDBGraphStorage(LanceDBGraphStorage):
    """在 LanceDBGraphStorage 上增加邻接缓存、标量索引、可选邻接索引表与物理聚簇。"""

    enable_adj_index_table: bool = False
    enable_physical_clustering: bool = False
    clustering_algorithm: str = "connected_components"
    clustering_min_edges: int = 100

    _adj_cache: dict[str, set[str]] = field(default_factory=dict)
    _node_edges_cache: dict[str, list[tuple[str, str]]] = field(default_factory=dict)
    _cache_loaded: bool = False
    _adj_index_table: Any = field(default=None)

    def __init__(
        self,
        namespace,
        global_config,
        embedding_func,
        workspace=None,
        *,
        enable_adj_index_table: bool = False,
        enable_physical_clustering: bool = False,
        clustering_algorithm: str = "connected_components",
        clustering_min_edges: int = 100,
    ):
        super().__init__(
            namespace=namespace,
            global_config=global_config,
            embedding_func=embedding_func,
            workspace=workspace,
        )
        self.enable_adj_index_table = enable_adj_index_table
        self.enable_physical_clustering = enable_physical_clustering
        self.clustering_algorithm = clustering_algorithm
        self.clustering_min_edges = clustering_min_edges
        self._adj_cache = {}
        self._node_edges_cache = {}
        self._cache_loaded = False
        self._adj_index_table = None

    # ---------- Section 1: 标量索引 ----------
    async def _create_scalar_indices(self) -> None:
        """为节点表、边表创建 BTree 标量索引；空表跳过，已存在则静默跳过。"""
        try:
            node_count = await self._node_table.count_rows()
            edge_count = await self._edge_table.count_rows()
        except Exception:
            return
        if edge_count > 0:
            # 边表三列：source/target/_id
            await _create_scalar_index_safe(self._edge_table, "source_node_id")
            await _create_scalar_index_safe(self._edge_table, "target_node_id")
            await _create_scalar_index_safe(self._edge_table, "_id")
        if node_count > 0:
            # 节点表主键列
            await _create_scalar_index_safe(self._node_table, "_id")
    # ---------- Section 2: 内存邻接缓存 ----------

    def _invalidate_adj_cache(self) -> None:
        # 清空所有缓存，下一次访问会重新全表扫描边表
        self._adj_cache = {}
        self._node_edges_cache = {}
        self._cache_loaded = False

    async def _ensure_adj_cache(self) -> None:
        # 懒加载：仅首次调用时构建邻接缓存
        if self._cache_loaded:
            return
        try:
            rows = await self._edge_table.query().select(["source_node_id", "target_node_id"]).to_list()
        except Exception as e:
            logger.warning(f"[{self.workspace}] Failed to load adj cache: {e}")
            return
        adj: dict[str, set[str]] = defaultdict(set)
        node_edges: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for r in rows:
            src, tgt = r["source_node_id"], r["target_node_id"]
            pair = (src, tgt)
            # 邻接集合（用于度数计算）
            adj[src].add(tgt)
            adj[tgt].add(src)
            # 原始边列表（用于 get_node_edges / get_nodes_edges_batch）
            node_edges[src].append(pair)
            node_edges[tgt].append(pair)
        self._adj_cache = dict(adj)
        self._node_edges_cache = {k: list(v) for k, v in node_edges.items()}
        self._cache_loaded = True
        logger.debug(f"[{self.workspace}] Adj cache loaded: {len(self._adj_cache)} nodes, {len(rows)} edges")

    async def node_degree(self, node_id: str) -> int:
        # 三级回退：内存缓存 → 邻接索引表 → 父类
        await self._ensure_adj_cache()
        if self._cache_loaded:
            return len(self._adj_cache.get(node_id, set()))
        # 邻接索引表：只需一次查询即可获取度数，无需回边表
        if self._adj_index_table is not None:
            deg_map = await self._query_degree_from_adj_table([node_id])
            return deg_map.get(node_id, 0)
        return await super().node_degree(node_id)

    async def get_node_edges(self, source_node_id: str) -> list[tuple[str, str]] | None:
        # 缓存已加载时直接返回对应边列表
        await self._ensure_adj_cache()
        if self._cache_loaded:
            return list(self._node_edges_cache.get(source_node_id, []))
        return await super().get_node_edges(source_node_id)

    async def node_degrees_batch(self, node_ids: list[str]) -> dict[str, int]:
        # 三级回退：内存缓存 → 邻接索引表 → 父类
        if not node_ids:
            return {}
        await self._ensure_adj_cache()
        if self._cache_loaded:
            return {nid: len(self._adj_cache.get(nid, set())) for nid in node_ids}
        # 邻接索引表：一次 WHERE entity_id IN (...) 查询即可获取所有度数
        if self._adj_index_table is not None:
            deg_map = await self._query_degree_from_adj_table(node_ids)
            # 确保所有请求的 node_id 都有返回值（不在索引表中的度数为 0）
            return {nid: deg_map.get(nid, 0) for nid in node_ids}
        return await super().node_degrees_batch(node_ids)

    async def get_nodes_edges_batch(
        self, node_ids: list[str]
    ) -> dict[str, list[tuple[str, str]]]:
        # 批量边列表可直接从缓存映射到每个节点
        if not node_ids:
            return {}
        await self._ensure_adj_cache()
        if self._cache_loaded:
            return {
                nid: list(self._node_edges_cache.get(nid, []))
                for nid in node_ids
            }
        return await super().get_nodes_edges_batch(node_ids)

    async def edge_degrees_batch(
        self, edge_pairs: list[tuple[str, str]]
    ) -> dict[tuple[str, str], int]:
        # 先聚合所有节点度数，再组合边度数，减少重复查询
        if not edge_pairs:
            return {}
        all_nodes = set()
        for src, tgt in edge_pairs:
            all_nodes.add(src)
            all_nodes.add(tgt)
        degrees = await self.node_degrees_batch(list(all_nodes))
        return {
            (src, tgt): degrees.get(src, 0) + degrees.get(tgt, 0)
            for src, tgt in edge_pairs
        }

    # ---------- Section 2.5: 邻接索引表查询辅助（per-node-one-row 方案） ----------

    @staticmethod
    def _merge_row_ids_from_row(
        row: dict, *, use_out: bool = True, use_in: bool = True
    ) -> list[int]:
        """从一行 adj 记录中合并 out / in 列的 _rowid 列表并去重。"""
        ids: set[int] = set()
        if use_out:
            for v in (row.get("out") or []):
                ids.add(int(v))
        if use_in:
            for v in (row.get("in") or []):
                ids.add(int(v))
        return list(ids)

    async def _take_adj_entity_ids(
        self, node_row_ids: list[int]
    ) -> list[str]:
        """
        通过 node_table.take_row_ids 将 node _rowid 列表转换为 entity_id 列表。
        """
        if not node_row_ids:
            return []
        try:
            rows = await self._node_table.take_row_ids(node_row_ids).select(["_id"]).to_list()
            return [r["_id"] for r in rows]
        except Exception as e:
            logger.warning(f"[{self.workspace}] _take_adj_entity_ids: {e}")
            return []

    async def _query_neighbors_from_adj_table(
        self, node_ids: list[str],
        *, use_out: bool = True, use_in: bool = True,
    ) -> dict[str, set[str]]:
        """
        通过持久化邻接索引表批量查询多个节点的邻居。

        两步走：
          1. WHERE entity_id IN (...) 取出 adj 行（每个 node 一行）。
          2. 合并 out+in → node _rowids → take_row_ids 取 entity_id。
        """
        if not self._adj_index_table or not node_ids:
            return {}
        try:
            where = _build_where_in("entity_id", node_ids)
            rows = (
                await self._adj_index_table.query()
                .where(where)
                .select(["entity_id", "out", "in"])
                .to_list()
            )
            result: dict[str, set[str]] = {}
            for r in rows:
                nid = r["entity_id"]
                rids = self._merge_row_ids_from_row(r, use_out=use_out, use_in=use_in)
                if rids:
                    names = await self._take_adj_entity_ids(rids)
                    result[nid] = set(names)
                else:
                    result[nid] = set()
            return result
        except Exception as e:
            logger.warning(f"[{self.workspace}] _query_neighbors_from_adj_table: {e}")
            return {}

    async def _query_neighbors_with_adj_rowids(
        self, node_ids: list[str],
        *, use_out: bool = True, use_in: bool = True,
    ) -> tuple[dict[str, set[str]], list[int]]:
        """
        首跳邻居查询（WHERE 路径），同时返回邻居的 node _rowid 列表。

        返回:
          (neighbors_map, neighbor_node_row_ids)
          - neighbors_map: {entity_id: {neighbor_1, ...}}
          - neighbor_node_row_ids: 所有邻居在 node 表中的 _rowid（去重），
            可传给 _take_adj_neighbors() 执行后续跳。
        """
        if not self._adj_index_table or not node_ids:
            return {}, []
        try:
            where = _build_where_in("entity_id", node_ids)
            rows = (
                await self._adj_index_table.query()
                .where(where)
                .select(["entity_id", "out", "in"])
                .to_list()
            )
            all_rids: set[int] = set()
            per_node_rids: dict[str, list[int]] = {}
            for r in rows:
                rids = self._merge_row_ids_from_row(r, use_out=use_out, use_in=use_in)
                per_node_rids[r["entity_id"]] = rids
                all_rids.update(rids)

            # 一次 take 将 _rowid → entity_id
            if not all_rids:
                return {nid: set() for nid in node_ids}, []
            rid_list = sorted(all_rids)
            names = await self._take_adj_entity_ids(rid_list)
            rid_to_name: dict[int, str] = dict(zip(rid_list, names))

            neighbors: dict[str, set[str]] = {}
            for nid, rids in per_node_rids.items():
                neighbors[nid] = {rid_to_name[rid] for rid in rids if rid in rid_to_name}
            return neighbors, rid_list
        except Exception as e:
            logger.warning(f"[{self.workspace}] _query_neighbors_with_adj_rowids: {e}")
            return {}, []

    async def _take_adj_neighbors(
        self, node_row_ids: list[int],
        *, use_out: bool = True, use_in: bool = True,
    ) -> tuple[dict[str, set[str]], list[int]]:
        """
        通过 take_row_ids 获取后续跳邻居（BFS 第 2 跳及之后）。

        步骤：
          1. node_table.take_row_ids(node_row_ids) → 得到 entity_id 列表。
          2. adj_table WHERE entity_id IN (...) → 取 out/in。
          3. 合并 out+in → 新的 node _rowid 列表 → take → entity_id。

        返回:
          (neighbors_map, next_node_row_ids)
        """
        if not self._adj_index_table or not node_row_ids:
            return {}, []
        try:
            # Step 1: _rowid → entity_id
            entity_ids = await self._take_adj_entity_ids(node_row_ids)
            if not entity_ids:
                return {}, []
            # Step 2+3: 复用 WHERE 路径
            return await self._query_neighbors_with_adj_rowids(
                entity_ids, use_out=use_out, use_in=use_in
            )
        except Exception as e:
            logger.warning(f"[{self.workspace}] _take_adj_neighbors: {e}")
            return {}, []

    async def _query_edges_from_adj_table(
        self, node_ids: list[str]
    ) -> tuple[list[str], list[int]]:
        """
        通过邻接索引表查询两端都在 node_ids 内的边。

        新版 adj 表只存 node _rowid，不存 edge_id / edge_row_id，
        因此本方法仅返回空列表作为占位。
        调用方应回退到父类 WHERE 查询或内存缓存。
        """
        return [], []

    async def _query_degree_from_adj_table(
        self, node_ids: list[str],
        *, use_out: bool = True, use_in: bool = True,
    ) -> dict[str, int]:
        """
        通过邻接索引表计算指定节点的度数。

        度数 = len(out) + len(in)（无向）或仅 len(out) / len(in)。
        """
        if not self._adj_index_table or not node_ids:
            return {}
        try:
            where = _build_where_in("entity_id", node_ids)
            rows = (
                await self._adj_index_table.query()
                .where(where)
                .select(["entity_id", "out", "in"])
                .to_list()
            )
            result: dict[str, int] = {}
            for r in rows:
                deg = 0
                if use_out:
                    deg += len(r.get("out") or [])
                if use_in:
                    deg += len(r.get("in") or [])
                result[r["entity_id"]] = deg
            return result
        except Exception as e:
            logger.warning(f"[{self.workspace}] _query_degree_from_adj_table: {e}")
            return {}

    async def _query_all_degrees_from_adj_table(
        self, *, use_out: bool = True, use_in: bool = True,
    ) -> dict[str, int]:
        """
        通过邻接索引表计算全图所有节点的度数。

        全表扫描 adj 表（每节点一行），度数 = len(out) + len(in)。
        """
        if not self._adj_index_table:
            return {}
        try:
            rows = (
                await self._adj_index_table.query()
                .select(["entity_id", "out", "in"])
                .to_list()
            )
            result: dict[str, int] = {}
            for r in rows:
                deg = 0
                if use_out:
                    deg += len(r.get("out") or [])
                if use_in:
                    deg += len(r.get("in") or [])
                result[r["entity_id"]] = deg
            return result
        except Exception as e:
            logger.warning(f"[{self.workspace}] _query_all_degrees_from_adj_table: {e}")
            return {}

    # ---------- Section 2.6: BFS / 多跳查询加速 ----------

    async def _bidirectional_bfs_nodes(
        self,
        node_labels: list[str],
        seen_nodes: set[str],
        result: KnowledgeGraph,
        depth: int,
        max_depth: int,
        max_nodes: int,
        *,
        _pending_node_row_ids: list[int] | None = None,
    ) -> KnowledgeGraph:
        """
        使用邻接缓存或邻接索引表加速的 BFS 遍历（三级回退）。

        回退策略（从快到慢）：
          1. 内存邻接缓存 (_adj_cache)：纯内存 O(1) 邻居查找。
          2. 邻接索引表路径：
             首跳：WHERE entity_id IN (...) → 取 out+in → take node _rowids → entity_id
             后续跳：先 take node _rowids → entity_id，再 WHERE → 取 out+in → take
          3. 父类原始实现：直接扫描 edge 表做 OR 过滤。

        _pending_node_row_ids: 上一跳返回的邻居在 node 表中的 _rowid 列表，
          非 None 时先 take → entity_id 再查 adj 表（避免纯 WHERE 路径）。
        """
        # ---- 选择邻居发现策略 ----
        await self._ensure_adj_cache()
        use_memory_cache = self._cache_loaded
        use_adj_table = (not use_memory_cache) and (self._adj_index_table is not None)

        if not use_memory_cache and not use_adj_table:
            return await super()._bidirectional_bfs_nodes(
                node_labels, seen_nodes, result, depth, max_depth, max_nodes
            )

        # ---- 快速终止 ----
        if depth > max_depth or len(result.nodes) > max_nodes:
            return result

        # ---- 当前层节点处理 ----
        new_labels = [nid for nid in node_labels if nid not in seen_nodes]
        if new_labels:
            nodes_data = await self.get_nodes_batch(new_labels)
            for node_id in new_labels:
                if node_id in seen_nodes:
                    continue
                seen_nodes.add(node_id)
                node_data = nodes_data.get(node_id)
                if node_data is not None:
                    result.nodes.append(
                        self._construct_graph_node(node_id, node_data)
                    )
                if len(result.nodes) > max_nodes:
                    return result

        # ---- 发现下一层邻居 ----
        neighbor_nodes: list[str] = []
        next_node_row_ids: list[int] | None = None

        if use_memory_cache:
            # 路径 1：纯内存查找（最快，O(1) per neighbor）
            for nid in new_labels:
                for neighbor in self._adj_cache.get(nid, set()):
                    if neighbor not in seen_nodes:
                        neighbor_nodes.append(neighbor)
        elif _pending_node_row_ids is not None and _pending_node_row_ids:
            # 路径 2a：take 路径（BFS 第 2 跳及之后）
            # node_row_ids → entity_ids → WHERE adj table → out+in → new node_row_ids
            adj_result, next_node_row_ids = await self._take_adj_neighbors(
                _pending_node_row_ids
            )
            for nid in new_labels:
                for neighbor in adj_result.get(nid, set()):
                    if neighbor not in seen_nodes:
                        neighbor_nodes.append(neighbor)
        else:
            # 路径 2b：WHERE 路径（BFS 首跳）
            adj_result, next_node_row_ids = (
                await self._query_neighbors_with_adj_rowids(new_labels)
            )
            for nid in new_labels:
                for neighbor in adj_result.get(nid, set()):
                    if neighbor not in seen_nodes:
                        neighbor_nodes.append(neighbor)

        # ---- 去重 + 递归下一层 ----
        if neighbor_nodes:
            neighbor_nodes = list(set(neighbor_nodes))
            result = await self._bidirectional_bfs_nodes(
                neighbor_nodes, seen_nodes, result, depth + 1, max_depth, max_nodes,
                _pending_node_row_ids=next_node_row_ids,
            )

        return result

    async def _fetch_edges_between_nodes(self, node_ids: list[str]) -> list[dict]:
        """
        筛选两端节点都在 node_ids 内的边（二级回退）。

        回退策略：
          1. 内存缓存 (_node_edges_cache)：在内存中筛选候选 edge_id
             → 一次 WHERE _id IN (...) 批量查边属性。
          2. 父类：WHERE source IN (...) AND target IN (...) 直接扫边表。

        注意：邻接索引表路径（adj table → take_row_ids）已移除。
        基准测试显示该路径需要两次查询（先查 adj 表再查 edge 表），
        反而比父类的单次 WHERE 查询更慢。
        """
        if not node_ids:
            return []

        # ---- 尝试从内存缓存获取候选 edge_id ----
        await self._ensure_adj_cache()

        if self._cache_loaded:
            node_set = set(node_ids)
            seen: set[str] = set()
            eids: list[str] = []
            for nid in node_ids:
                for src, tgt in self._node_edges_cache.get(nid, []):
                    if src in node_set and tgt in node_set:
                        eid = _make_edge_id(src, tgt)
                        if eid not in seen:
                            seen.add(eid)
                            eids.append(eid)
            if not eids:
                return []
            try:
                results = (
                    await self._edge_table.query()
                    .where(_build_where_in("_id", eids))
                    .to_list()
                )
                return [dict(r) for r in results]
            except Exception as e:
                logger.warning(f"[{self.workspace}] _fetch_edges_between_nodes: {e}")

        # ---- 内存缓存不可用，回退到父类 ----
        return await super()._fetch_edges_between_nodes(node_ids)

    async def get_knowledge_graph_all_by_degree(
        self, max_depth: int, max_nodes: int
    ) -> KnowledgeGraph:
        """
        全图按度数排序取 Top-K 子图（三级回退）。

        度数计算回退策略：
          1. 内存邻接缓存：O(节点数) 内存遍历。
          2. 邻接索引表：全表扫描 entity_id 列（列少、有 BTREE 索引）。
          3. 父类：全表扫描 edge 表 source/target 列。

        边获取同样通过 _fetch_edges_between_nodes 的三级回退完成。
        """
        # ---- 判断度数数据来源 ----
        await self._ensure_adj_cache()
        use_memory = self._cache_loaded
        use_adj_table = (not use_memory) and (self._adj_index_table is not None)

        if not use_memory and not use_adj_table:
            # 两者都不可用，完全回退到父类
            return await super().get_knowledge_graph_all_by_degree(max_depth, max_nodes)

        result = KnowledgeGraph()
        seen_edges: set[str] = set()

        try:
            total_node_count = await self._node_table.count_rows()
            result.is_truncated = total_node_count > max_nodes

            if result.is_truncated:
                # ---- 计算全图度数 ----
                if use_memory:
                    # 路径 1：从内存缓存计算度数（最快）
                    degree_counter = Counter(
                        {nid: len(neighbors) for nid, neighbors in self._adj_cache.items()}
                    )
                else:
                    # 路径 2：从邻接索引表计算度数
                    # 只扫描 entity_id 列，比扫描整张 edge 表更轻量
                    degree_dict = await self._query_all_degrees_from_adj_table()
                    if not degree_dict:
                        # 邻接索引表为空或查询失败，回退到父类
                        return await super().get_knowledge_graph_all_by_degree(max_depth, max_nodes)
                    degree_counter = Counter(degree_dict)

                # Top-K 节点（按度数从高到低）
                top_nodes = [nid for nid, _ in degree_counter.most_common(max_nodes)]

                # 批量获取节点属性
                nodes_data = await self.get_nodes_batch(top_nodes)
                for nid in top_nodes:
                    node_data = nodes_data.get(nid)
                    if node_data is not None:
                        result.nodes.append(
                            self._construct_graph_node(nid, node_data)
                        )

                # 补全诱导子图的边（_fetch_edges_between_nodes 内部也有三级回退）
                edge_results = await self._fetch_edges_between_nodes(top_nodes)
            else:
                # 节点数不超限，全量获取
                nodes = await self._node_table.query().to_list()
                for node in nodes:
                    result.nodes.append(
                        self._construct_graph_node(node["_id"], dict(node))
                    )
                edge_results = await self._edge_table.query().to_list()

            for edge in edge_results:
                edge_id = f"{edge['source_node_id']}-{edge['target_node_id']}"
                if edge_id not in seen_edges:
                    seen_edges.add(edge_id)
                    result.edges.append(
                        self._construct_graph_edge(edge_id, dict(edge))
                    )

        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error in get_knowledge_graph_all_by_degree: {e}"
            )

        return result
    # ---------- Section 3: 邻接索引表（可选） ----------
    async def _rebuild_adj_index_table(self) -> None:
        # 邻接索引表：每个节点一行，存储 out / in 两列（有向邻居的 node _rowid 列表）。
        #
        # 构建过程：
        #   1. 读取 node 表 → 建 node_id → _rowid 映射。
        #   2. 读取 edge 表 → 对每条边 (src→tgt)：
        #        out_map[src].add(node_rowid[tgt])
        #        in_map[tgt].add(node_rowid[src])
        #   3. 每个 node 生成一行 {entity_id, out: [...], in: [...]}。
        #   4. 一次性 create_table(mode="overwrite") 写入。
        if not self._adj_index_table:
            return
        try:
            # 读取 node 表（含真实 _rowid）
            node_rows = (
                await self._node_table.query()
                .with_row_id()
                .select(["_id"])
                .to_list()
            )
            # 读取 edge 表
            edge_rows = (
                await self._edge_table.query()
                .select(["source_node_id", "target_node_id"])
                .to_list()
            )
        except Exception as e:
            logger.warning(f"[{self.workspace}] Rebuild adj index failed: {e}")
            return

        # node_id → 真实 Lance _rowid
        node_id_to_rowid: dict[str, int] = {
            r["_id"]: int(r["_rowid"]) for r in node_rows
        }

        # 构建 out / in 映射
        out_map: dict[str, set[int]] = defaultdict(set)
        in_map: dict[str, set[int]] = defaultdict(set)
        for edge in edge_rows:
            src = edge["source_node_id"]
            tgt = edge["target_node_id"]
            tgt_rid = node_id_to_rowid.get(tgt)
            src_rid = node_id_to_rowid.get(src)
            if tgt_rid is not None:
                out_map[src].add(tgt_rid)
            if src_rid is not None:
                in_map[tgt].add(src_rid)

        # 每个 node 一行（基础邻接索引表记录，未聚簇）
        records: list[dict] = []
        for nid in node_id_to_rowid:
            records.append(
                {
                    "entity_id": nid,
                    "out": sorted(out_map.get(nid, set())),
                    "in": sorted(in_map.get(nid, set())),
                }
            )

        if not records:
            return

        # 1) 写入基础邻接索引表（未聚簇版本），供默认查询路径使用。
        # 为避免一次性构建巨大 Arrow 表，这里先创建空表，再按固定 batch_size 追加写入。
        base_name = f"{self._node_table_name}_adj_idx"
        try:
            empty_tbl = pa.Table.from_pylist([], schema=KG_ADJ_INDEX_SCHEMA)
            self._adj_index_table = await self.db.create_table(
                base_name, empty_tbl, mode="overwrite"
            )
            batch_size = 10_000
            for i in range(0, len(records), batch_size):
                chunk = records[i : i + batch_size]
                arrow_tbl = pa.Table.from_pylist(chunk, schema=KG_ADJ_INDEX_SCHEMA)
                await self._adj_index_table.add(arrow_tbl)
        except Exception as e:
            logger.warning(f"[{self.workspace}] Rebuild adj index write failed: {e}")
            return

        # 为 entity_id 创建 BTREE 标量索引
        if self._adj_index_table and hasattr(
            self._adj_index_table, "create_scalar_index"
        ):
            try:
                result = self._adj_index_table.create_scalar_index(
                    "entity_id", index_type="BTREE"
                )
                if inspect.isawaitable(result):
                    await result
            except Exception:
                pass

        # 2) 可选：写入按社区聚簇后的邻接索引表，使用不同表名，便于性能对比测试
        if self.enable_physical_clustering and edge_rows:
            try:
                edges_for_clustering = [
                    (e["source_node_id"], e["target_node_id"]) for e in edge_rows
                ]
                community_labels = self._detect_communities(edges_for_clustering)

                def _community_key(rec: dict) -> tuple[int, str]:
                    nid = rec["entity_id"]
                    # 未出现在任何边中的节点社区记为 -1，排在前面或单独成块
                    return community_labels.get(nid, -1), nid

                clustered_records = list(records)
                clustered_records.sort(key=_community_key)

                clustered_name = f"{self._node_table_name}_adj_idx_clustered"
                empty_clustered = pa.Table.from_pylist(
                    [], schema=KG_ADJ_INDEX_SCHEMA
                )
                clustered_table = await self.db.create_table(
                    clustered_name, empty_clustered, mode="overwrite"
                )
                batch_size = 10_000
                for i in range(0, len(clustered_records), batch_size):
                    chunk = clustered_records[i : i + batch_size]
                    arrow_tbl = pa.Table.from_pylist(
                        chunk, schema=KG_ADJ_INDEX_SCHEMA
                    )
                    await clustered_table.add(arrow_tbl)

                if clustered_table and hasattr(
                    clustered_table, "create_scalar_index"
                ):
                    try:
                        result = clustered_table.create_scalar_index(
                            "entity_id", index_type="BTREE"
                        )
                        if inspect.isawaitable(result):
                            await result
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(
                    f"[{self.workspace}] Clustered adj index build skipped: {e}"
                )

    # ---------- Section 4: 物理聚簇 ----------
    def _detect_communities(self, edges: list[tuple[str, str]]) -> dict[str, int]:
        """社区检测：默认连通分量，可选 Louvain。返回 node_id -> community_label。"""
        if self.clustering_algorithm == "louvain":
            try:
                import networkx as nx
                from networkx.algorithms import community
                # Louvain 需要 networkx 依赖
                G = nx.Graph()
                for a, b in edges:
                    G.add_edge(a, b)
                communities = community.louvain_communities(G)
                labels = {}
                for i, comp in enumerate(communities):
                    for n in comp:
                        labels[n] = i
                return labels
            except Exception as e:
                logger.debug(f"Louvain fallback: {e}")
        # 默认：连通分量（纯 Python，使用迭代版并查集，避免递归爆栈）
        parent: dict[str, str] = {}

        def find(x: str) -> str:
            # 迭代版路径压缩：先找到根，再一路压缩
            root = x
            # 查找根节点
            while True:
                root_parent = parent.setdefault(root, root)
                if root_parent == root:
                    break
                root = root_parent
            # 路径压缩
            while x != root:
                px = parent[x]
                parent[x] = root
                x = px
            return root

        def union(a: str, b: str) -> None:
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

        for a, b in edges:
            union(a, b)
        comp_id: dict[str, int] = {}
        for n in parent:
            root = find(n)
            if root not in comp_id:
                comp_id[root] = len(comp_id)
        return {n: comp_id[find(n)] for n in parent}

    async def _run_physical_clustering(self) -> None:
        """按社区标签重排边表、节点表，并重建标量索引与邻接索引表。"""
        try:
            edge_count = await self._edge_table.count_rows()
        except Exception:
            return
        if edge_count < self.clustering_min_edges:
            # 边数太少时跳过聚簇，避免无意义重排
            return
        try:
            edge_rows = await self._edge_table.query().to_list()
            node_rows = await self._node_table.query().to_list()
        except Exception as e:
            logger.warning(f"[{self.workspace}] Physical clustering read: {e}")
            return
        edges = [(r["source_node_id"], r["target_node_id"]) for r in edge_rows]
        labels = self._detect_communities(edges)
        edge_community: list[int] = []
        for r in edge_rows:
            src, tgt = r["source_node_id"], r["target_node_id"]
            ls, lt = labels.get(src, -1), labels.get(tgt, -1)
            if ls == lt:
                edge_community.append(ls)
            else:
                edge_community.append(max(ls, lt))
        node_community = [labels.get(r["_id"], -1) for r in node_rows]
        for i, r in enumerate(edge_rows):
            r["_community"] = edge_community[i]
        for i, r in enumerate(node_rows):
            r["_community"] = node_community[i]
        # 物理排序：按社区分组，局部性更好
        edge_sorted = sorted(edge_rows, key=lambda x: (x["_community"], x["source_node_id"]))
        node_sorted = sorted(node_rows, key=lambda x: x["_community"])
        # 去掉临时列，保持原 schema
        edge_fields = [f.name for f in GRAPH_EDGE_SCHEMA]
        node_fields = [f.name for f in GRAPH_NODE_SCHEMA]
        # 移除临时字段（如 _community）以匹配原 schema
        edge_clean = [{k: row[k] for k in edge_fields if k in row} for row in edge_sorted]
        node_clean = [{k: row[k] for k in node_fields if k in row} for row in node_sorted]
        uri = os.environ.get("LANCEDB_URI", "./lancedb")
        try:
            import lance
            edge_tbl = pa.table(edge_clean, schema=GRAPH_EDGE_SCHEMA)
            node_tbl = pa.table(node_clean, schema=GRAPH_NODE_SCHEMA)
            lance.write_dataset(edge_tbl, f"{uri}/{self._edge_table_name}.lance", mode="overwrite")
            lance.write_dataset(node_tbl, f"{uri}/{self._node_table_name}.lance", mode="overwrite")
        except Exception as e:
            logger.warning(f"[{self.workspace}] Physical clustering write: {e}")
            return
        # 重开表句柄，确保后续读写基于新数据
        self._edge_table = await self.db.open_table(self._edge_table_name)
        self._node_table = await self.db.open_table(self._node_table_name)
        await self._create_scalar_indices()
        if self.enable_adj_index_table:
            await self._rebuild_adj_index_table()

    # ---------- Section 5: 生命周期 ----------
    async def initialize(self) -> None:
        await super().initialize()
        # 初始化后补充标量索引
        await self._create_scalar_indices()
        if self.enable_adj_index_table:
            adj_name = f"{self._node_table_name}_adj_idx"
            self._adj_index_table = await get_or_create_table(
                self.db, adj_name, KG_ADJ_INDEX_SCHEMA
            )

    async def index_done_callback(self) -> None:
        await super().index_done_callback()
        # 写入完成后可选物理聚簇、重建邻接索引，并刷新缓存
        if self.enable_physical_clustering:
            await self._run_physical_clustering()
        if self.enable_adj_index_table:
            await self._rebuild_adj_index_table()
        self._invalidate_adj_cache()

    async def finalize(self) -> None:
        # 清理缓存，释放表引用
        self._invalidate_adj_cache()
        self._adj_index_table = None
        await super().finalize()
