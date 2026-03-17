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
from experiments.lancedb_graph.data_prep.build_graph_tables import build_graph_dataframes_from_tsv
from experiments.lancedb_graph.query_engines.basic_queries import (
    query_in_neighbors,
    query_k_hop,
    query_neighbors,
    query_node_by_id,
    query_out_neighbors,
)
from experiments.lancedb_graph.utils.stats import build_basic_graph_stats


class LanceDBGraphBasic:
    """阶段一的基础 LanceDB 图存储实现。"""

    def __init__(
        self,
        db_path: str = DEFAULT_DB_PATH,
        nodes_table_name: str = NODES_TABLE_NAME,
        edges_table_name: str = EDGES_TABLE_NAME,
        write_batch_size: int = DEFAULT_LANCEDB_WRITE_BATCH_SIZE,
    ):
        self.db_path = db_path
        self.nodes_table_name = nodes_table_name
        self.edges_table_name = edges_table_name
        self.write_batch_size = write_batch_size
        self.db = lancedb.connect(db_path)
        self.nodes_tbl = None
        self.edges_tbl = None

    def build_from_tsv(self, tsv_path: str = DEFAULT_INPUT_TSV):
        nodes_df, edges_df = build_graph_dataframes_from_tsv(tsv_path)
        return self.build_from_dataframes(nodes_df, edges_df)

    def build_from_dataframes(self, nodes_df, edges_df):
        if OVERWRITE_TABLES:
            for table_name in [self.nodes_table_name, self.edges_table_name]:
                if table_name in self.db.table_names():
                    self.db.drop_table(table_name)

        self.nodes_tbl = self._write_dataframe_in_batches(self.nodes_table_name, nodes_df)
        self.edges_tbl = self._write_dataframe_in_batches(self.edges_table_name, edges_df)
        return self

    def load(self):
        table_names = set(self.db.table_names())
        if self.nodes_table_name not in table_names:
            raise ValueError(f"节点表不存在: {self.nodes_table_name}")
        if self.edges_table_name not in table_names:
            raise ValueError(f"边表不存在: {self.edges_table_name}")

        self.nodes_tbl = self.db[self.nodes_table_name]
        self.edges_tbl = self.db[self.edges_table_name]
        return self

    def stats(self):
        self._ensure_loaded()
        return build_basic_graph_stats(self.nodes_tbl, self.edges_tbl)

    def get_node(self, node_id: str):
        self._ensure_loaded()
        return query_node_by_id(self.nodes_tbl, node_id)

    def query_out_neighbors(self, node_id: str, edge_type: Optional[str] = None):
        self._ensure_loaded()
        return query_out_neighbors(self.edges_tbl, node_id, edge_type=edge_type)

    def query_in_neighbors(self, node_id: str, edge_type: Optional[str] = None):
        self._ensure_loaded()
        return query_in_neighbors(self.edges_tbl, node_id, edge_type=edge_type)

    def query_neighbors(self, node_id: str, edge_type: Optional[str] = None):
        self._ensure_loaded()
        return query_neighbors(self.edges_tbl, node_id, edge_type=edge_type)

    def query_k_hop(self, node_id: str, k: int):
        self._ensure_loaded()
        return query_k_hop(self.edges_tbl, node_id, k)

    def _ensure_loaded(self):
        if self.nodes_tbl is None or self.edges_tbl is None:
            self.load()

    def _write_dataframe_in_batches(self, table_name: str, df):
        if df.empty:
            return self.db.create_table(table_name, data=df, mode="overwrite")

        batch_size = max(1, int(self.write_batch_size))
        first_batch = df.iloc[:batch_size]
        table = self.db.create_table(table_name, data=first_batch, mode="overwrite")

        for start in range(batch_size, len(df), batch_size):
            batch_df = df.iloc[start:start + batch_size]
            table.add(batch_df)

        return table
