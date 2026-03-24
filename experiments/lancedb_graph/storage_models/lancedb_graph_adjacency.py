from typing import Optional

import lancedb

from experiments.lancedb_graph.config import (
    DEFAULT_DB_PATH,
    DEFAULT_INPUT_TSV,
    DEFAULT_LANCEDB_WRITE_BATCH_SIZE,
    EDGES_TABLE_NAME,
    NODES_TABLE_NAME,
    OVERWRITE_TABLES,
)
from experiments.lancedb_graph.data_prep.build_adjacency_index import build_adjacency_index_dataframe
from experiments.lancedb_graph.data_prep.build_cluster_assignments import (
    assign_clusters_by_community,
    assign_clusters_by_hash,
    assign_clusters_by_node_type,
)
from experiments.lancedb_graph.data_prep.build_graph_tables import build_graph_dataframes_from_tsv
from experiments.lancedb_graph.query_engines.basic_queries import (
    query_in_neighbors,
    query_out_neighbors,
)
from experiments.lancedb_graph.query_engines.adjacency_queries import (
    query_in_neighbors_index,
    query_neighbors_index,
    query_out_neighbors_index,
)
from experiments.lancedb_graph.query_engines.traversal import query_k_hop_index
from experiments.lancedb_graph.utils.adjacency_stats import build_adjacency_stats


ADJ_INDEX_TABLE_NAME = "adj_index"


class LanceDBGraphAdjacency:
    """阶段二的邻接索引图存储实现。

    设计目标：
    1. 保留阶段一 `nodes + edges` baseline，作为正确性与性能对照组。
    2. 新增 `adj_index`，让单跳查询和后续多跳扩展尽量先命中邻接索引。
    3. 当前版本先采用“每节点一行”的简化实现，不引入 chunk 表。

    注意：
    - 本类当前优先完成“可构建、可加载、可作为后续查询接入点”的骨架。
    - 查询细节会在 `query_engines/adjacency_queries.py` 与 `traversal.py` 中逐步补齐。
    """

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        nodes_table_name: str = NODES_TABLE_NAME,
        edges_table_name: str = EDGES_TABLE_NAME,
        adj_index_table_name: str = ADJ_INDEX_TABLE_NAME,
        write_batch_size: int = DEFAULT_LANCEDB_WRITE_BATCH_SIZE,
    ):
        # `db_path` 与阶段一保持一致，便于在同一实验目录下复用存储路径约定。
        self.db_path = db_path
        self.nodes_table_name = nodes_table_name
        self.edges_table_name = edges_table_name
        self.adj_index_table_name = adj_index_table_name
        self.write_batch_size = write_batch_size
        self.db = lancedb.connect(db_path)
        self.nodes_tbl = None
        self.edges_tbl = None
        self.adj_index_tbl = None

    def build_from_tsv(self, tsv_path: str, cluster_strategy: str = "by_node_type"):
        """从 TSV 构建 `nodes`、`edges` 和 `adj_index`。

        这里先复用阶段一的数据转换逻辑，避免阶段二重复实现输入解析。
        这样可以保证：
        - baseline 与 adjacency 使用同一份 `nodes_df / edges_df`
        - 后续 benchmark 对照更可解释
        """
        nodes_df, edges_df = build_graph_dataframes_from_tsv(tsv_path)
        return self.build_from_dataframes(
            nodes_df=nodes_df,
            edges_df=edges_df,
            cluster_strategy=cluster_strategy,
        )

    def build_from_dataframes(self, nodes_df, edges_df, cluster_strategy: str = "by_node_type"):
        """从 DataFrame 构建 `nodes`、`edges` 和 `adj_index`。

        当前版本的处理顺序：
        1. 先根据 cluster 策略为节点生成 `cluster_id`
        2. 再构建 `adj_index_df`
        3. 最后将三张表按批量写入 LanceDB

        之所以先写 `nodes` / `edges` 再写 `adj_index`，是为了保留阶段一的表结构，
        便于 baseline 查询继续可用。
        """
        cluster_assignments = self._build_cluster_assignments(nodes_df, cluster_strategy)
        adj_index_df, _node_to_row_id = build_adjacency_index_dataframe(
            nodes_df=nodes_df,
            edges_df=edges_df,
            cluster_assignments=cluster_assignments,
        )

        # 只按 cluster_id 排序，不再二次按 node_id 排序
        adj_index_df = adj_index_df.sort_values(["cluster_id"]).reset_index(drop=True)
        node_to_physical_row_id = {
            row.node_id: idx for idx, row in enumerate(adj_index_df.itertuples(index=False))
        }
        adj_index_df["physical_row_id"] = adj_index_df["node_id"].map(node_to_physical_row_id).astype(int)
        adj_index_df["out_neighbor_row_ids"] = adj_index_df["out_neighbor_node_ids"].apply(
            lambda node_ids: [node_to_physical_row_id[node_id] for node_id in node_ids]
        )
        adj_index_df["in_neighbor_row_ids"] = adj_index_df["in_neighbor_node_ids"].apply(
            lambda node_ids: [node_to_physical_row_id[node_id] for node_id in node_ids]
        )
        adj_index_df = adj_index_df.drop(columns=["out_neighbor_node_ids", "in_neighbor_node_ids"])

        if OVERWRITE_TABLES:
            # `list_tables()` 在当前 LanceDB 版本中返回的元素可能不是纯字符串，
            # 有些环境下会返回形如 `[name, ...]` 的结构，因此这里显式抽取表名。
            existing_tables = {
                item[0] if isinstance(item, (list, tuple)) else item for item in self.db.list_tables()
            }
            for table_name in [self.nodes_table_name, self.edges_table_name, self.adj_index_table_name]:
                if table_name in existing_tables:
                    self.db.drop_table(table_name)

        self.nodes_tbl = self._write_dataframe_in_batches(self.nodes_table_name, nodes_df)
        self.edges_tbl = self._write_dataframe_in_batches(self.edges_table_name, edges_df)
        self.adj_index_tbl = self._write_dataframe_in_batches(self.adj_index_table_name, adj_index_df)
        return self

    def load(self):
        """加载已有的 `nodes`、`edges` 和 `adj_index`。

        显式检查三张表是否存在，避免在 benchmark 或实验脚本中静默失败。
        """
        table_names = {
            item[0] if isinstance(item, (list, tuple)) else item for item in self.db.list_tables()
        }
        if self.nodes_table_name not in table_names:
            raise ValueError(f"节点表不存在: {self.nodes_table_name}")
        if self.edges_table_name not in table_names:
            raise ValueError(f"边表不存在: {self.edges_table_name}")
        if self.adj_index_table_name not in table_names:
            raise ValueError(f"邻接索引表不存在: {self.adj_index_table_name}")

        self.nodes_tbl = self.db[self.nodes_table_name]
        self.edges_tbl = self.db[self.edges_table_name]
        self.adj_index_tbl = self.db[self.adj_index_table_name]
        return self

    def stats(self):
        """返回邻接索引相关统计信息。

        当前阶段先聚焦 `adj_index` 本身的统计，后续可再叠加：
        - baseline 图统计
        - clustered / unclustered 对比信息
        - 行大小分布
        """
        self._ensure_loaded()
        return build_adjacency_stats(self.adj_index_tbl)

    def get_adj_entry(self, node_id: str):
        """获取某个节点在 `adj_index` 中的索引项。

        这里先用最直接的过滤方式实现，保证语义清晰。
        后续如果需要再补更低开销的 row_id 直取路径。
        """
        self._ensure_loaded()
        df = self.adj_index_tbl.search().where(f"node_id = '{node_id}'").to_pandas()
        return df.to_dict("records")[0] if not df.empty else None

    def query_out_neighbors_index(self, node_id: str, materialize: bool = False):
        """基于邻接索引查询出邻居。

        当前版本先保留占位行为：
        - 如果只做存储层打通，可先通过 `get_adj_entry()` 验证索引项可获取
        - 真正的 row_id 解析与 materialize 逻辑在下一步查询层实现
        """
        self._ensure_loaded()
        return query_out_neighbors_index(
            self.adj_index_tbl,
            node_id,
            materialize=materialize,
        )

    def query_in_neighbors_index(self, node_id: str, materialize: bool = False):
        """基于邻接索引查询入邻居。"""
        self._ensure_loaded()
        return query_in_neighbors_index(
            self.adj_index_tbl,
            node_id,
            materialize=materialize,
        )

    def query_neighbors_index(self, node_id: str, materialize: bool = False):
        """基于邻接索引查询双向邻居。"""
        self._ensure_loaded()
        return query_neighbors_index(
            self.adj_index_tbl,
            node_id,
            materialize=materialize,
        )

    def query_k_hop_index(
        self,
        node_id: str,
        k: int,
        materialize: bool = False,
        direction: str = "out",
    ):
        """基于邻接索引执行 k-hop 查询。

        当前版本先接入 `traversal.py` 中的 BFS 实现，优先验证：
        - 多跳扩展语义是否正确
        - 方向控制（out / in / both）是否符合预期
        - materialized 与 index-only 返回格式是否统一
        """
        self._ensure_loaded()
        return query_k_hop_index(
            self.adj_index_tbl,
            node_id=node_id,
            k=k,
            materialize=materialize,
            direction=direction,
        )

    def query_out_neighbors_baseline(self, node_id: str):
        """调用 baseline 出邻居查询，便于做对照实验。"""
        self._ensure_loaded()
        return query_out_neighbors(self.edges_tbl, node_id)

    def query_in_neighbors_baseline(self, node_id: str):
        """调用 baseline 入邻居查询，便于做对照实验。"""
        self._ensure_loaded()
        return query_in_neighbors(self.edges_tbl, node_id)

    def _build_cluster_assignments(self, nodes_df, cluster_strategy: str):
        """根据策略构建 `cluster_id`。

        当前仅支持最基础、最稳定的两类策略：
        - `by_node_type`: 便于解释，也方便在 synthetic 数据上快速验证
        - `hash`: 作为一个不依赖业务语义的均匀分桶基线
        - `community`: 按节点所属社区聚簇，适合 community graph 实验
        - `none`: 不做语义聚簇，仅保留构建时原始节点顺序
        """
        if cluster_strategy == "none":
            return {node_id: "default" for node_id in nodes_df["node_id"].tolist()}
        if cluster_strategy == "by_node_type":
            return assign_clusters_by_node_type(nodes_df)
        if cluster_strategy == "community":
            return assign_clusters_by_community(nodes_df)
        if cluster_strategy == "hash":
            return assign_clusters_by_hash(nodes_df, num_buckets=16)
        raise ValueError(f"不支持的 cluster_strategy: {cluster_strategy}")

    def _ensure_loaded(self):
        """确保三张核心表已加载。"""
        if self.nodes_tbl is None or self.edges_tbl is None or self.adj_index_tbl is None:
            self.load()

    def _write_dataframe_in_batches(self, table_name: str, df):
        """按批量写入 LanceDB。

        保留这个方法有两个目的：
        1. 与阶段一写入行为保持一致，减少新旧实现差异。
        2. 后续如果要对 `adj_index` 做单独写入策略优化，可集中在这里改。
        """
        if df.empty:
            return self.db.create_table(table_name, data=df, mode="overwrite")

        batch_size = max(1, int(self.write_batch_size))
        first_batch = df.iloc[:batch_size]
        table = self.db.create_table(table_name, data=first_batch, mode="overwrite")

        for start in range(batch_size, len(df), batch_size):
            batch_df = df.iloc[start:start + batch_size]
            table.add(batch_df)

        return table
