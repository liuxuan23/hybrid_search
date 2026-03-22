import os

from experiments.lancedb_graph.data_prep.build_adjacency_index import build_adjacency_index_dataframe
from experiments.lancedb_graph.data_prep.build_graph_tables import build_graph_dataframes_from_tsv
from experiments.lancedb_graph.query_engines.adjacency_queries import (
    query_in_neighbors_index,
    query_out_neighbors_index,
)
from experiments.lancedb_graph.query_engines.basic_queries import query_k_hop
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPERIMENT_ROOT = os.path.dirname(BASE_DIR)
SAMPLE_TSV = os.path.join(EXPERIMENT_ROOT, "data_prep", "sample_graph.tsv")
TEST_DB_PATH = os.path.join(EXPERIMENT_ROOT, "..", "..", "storage", "lancedb_graph", "phase2_test_validation")


def test_adj_index_builds_expected_rows():
    """验证 `adj_index` 能从小样本图正确构建。"""
    nodes_df, edges_df = build_graph_dataframes_from_tsv(SAMPLE_TSV)

    adj_index_df, node_to_row_id = build_adjacency_index_dataframe(nodes_df, edges_df)

    assert len(adj_index_df) == len(nodes_df)
    assert len(node_to_row_id) == len(nodes_df)
    assert set(adj_index_df["node_id"].tolist()) == set(nodes_df["node_id"].tolist())


def test_neighbor_row_ids_are_reasonable_for_sample_graph():
    """验证小样本中关键节点的出入邻居 row_id 是否符合预期。"""
    nodes_df, edges_df = build_graph_dataframes_from_tsv(SAMPLE_TSV)
    adj_index_df, node_to_row_id = build_adjacency_index_dataframe(nodes_df, edges_df)

    alice_row = adj_index_df[adj_index_df["node_id"] == "user:alice"].iloc[0]
    bob_row_id = node_to_row_id["user:bob"]
    laptop_row_id = node_to_row_id["item:laptop"]
    group_row_id = node_to_row_id["group:ml"]
    dave_row_id = node_to_row_id["user:dave"]

    assert alice_row["out_neighbor_row_ids"] == [bob_row_id, laptop_row_id, group_row_id]
    assert alice_row["in_neighbor_row_ids"] == [dave_row_id]


def test_index_only_single_hop_query_runs():
    """验证 index-only 模式的单跳出边查询可以跑通。"""
    graph = LanceDBGraphAdjacency(db_path=TEST_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)

    result = graph.query_out_neighbors_index("user:alice", materialize=False)

    assert result["count"] == 3
    assert result["mode"] == "index-only"
    assert all("row_id" in row for row in result["rows"])


def test_materialized_single_hop_query_returns_expected_rows():
    """验证 materialized 模式能返回正确的邻接索引行。"""
    graph = LanceDBGraphAdjacency(db_path=TEST_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)

    result = graph.query_out_neighbors_index("user:alice", materialize=True)
    returned_node_ids = {row["node_id"] for row in result["rows"]}

    assert result["count"] == 3
    assert result["mode"] == "materialized"
    assert returned_node_ids == {"user:bob", "item:laptop", "group:ml"}


def test_index_only_in_neighbors_query_runs():
    """验证 index-only 模式的入邻居查询可以跑通。"""
    graph = LanceDBGraphAdjacency(db_path=TEST_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)

    result = query_in_neighbors_index(graph.adj_index_tbl, "item:laptop", materialize=False)

    assert result["count"] == 2
    assert result["mode"] == "index-only"
    assert all("row_id" in row for row in result["rows"])


def test_k_hop_out_query_returns_expected_nodes():
    """验证 2-hop 出向扩展能返回预期节点集合。"""
    graph = LanceDBGraphAdjacency(db_path=TEST_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)

    result = graph.query_k_hop_index("user:alice", k=2, materialize=True, direction="out")
    returned_node_ids = {row["node_id"] for row in result["rows"]}

    assert result["k"] == 2
    assert result["direction"] == "out"
    assert result["mode"] == "materialized"
    assert returned_node_ids == {
        "user:bob",
        "item:laptop",
        "group:ml",
        "user:carol",
        "brand:lenovo",
        "topic:retrieval",
    }


def test_k_hop_in_query_returns_expected_nodes():
    """验证 2-hop 入向扩展能沿反向邻接关系找到预期节点。"""
    graph = LanceDBGraphAdjacency(db_path=TEST_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)

    result = graph.query_k_hop_index("item:laptop", k=2, materialize=False, direction="in")
    returned_row_ids = {row["row_id"] for row in result["rows"]}

    alice_row_id = graph.get_adj_entry("user:alice")["logical_row_id"]
    bob_row_id = graph.get_adj_entry("user:bob")["logical_row_id"]
    dave_row_id = graph.get_adj_entry("user:dave")["logical_row_id"]

    assert result["k"] == 2
    assert result["direction"] == "in"
    assert result["mode"] == "index-only"
    assert returned_row_ids == {alice_row_id, bob_row_id, dave_row_id}


def test_k_hop_both_query_returns_expected_nodes():
    """验证 both 方向的 1-hop 扩展会合并入邻居与出邻居。"""
    graph = LanceDBGraphAdjacency(db_path=TEST_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)

    result = graph.query_k_hop_index("user:alice", k=1, materialize=True, direction="both")
    returned_node_ids = {row["node_id"] for row in result["rows"]}

    assert result["k"] == 1
    assert result["direction"] == "both"
    assert result["mode"] == "materialized"
    assert returned_node_ids == {"user:bob", "item:laptop", "group:ml", "user:dave"}


    def test_baseline_k_hop_query_returns_layered_rows(sample_adjacency_graph):
        """验证 baseline 多跳查询可正常返回分层结果。"""
        result = query_k_hop(sample_adjacency_graph.edges_tbl, "node_a", 2)

        assert result["count"] > 0
        assert len(result["rows"]) == 2

        hop1_targets = {row["dst"] for row in result["rows"][0]}
        hop2_targets = {row["dst"] for row in result["rows"][1]}

        assert hop1_targets == {"node_b", "node_c"}
        assert hop2_targets == {"node_d", "node_e"}
