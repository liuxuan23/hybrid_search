import os

from experiments.lancedb_graph.data_prep.build_adjacency_index import build_adjacency_index_dataframe
from experiments.lancedb_graph.data_prep.generate_synthetic_graph import generate_edges
from experiments.lancedb_graph.data_prep.build_graph_tables import build_graph_dataframes_from_tsv
from experiments.lancedb_graph.query_engines.adjacency_queries import (
    _normalize_row_id_list,
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
    """验证小样本中关键节点的邻接 node_id 列表是否符合预期。"""
    nodes_df, edges_df = build_graph_dataframes_from_tsv(SAMPLE_TSV)
    adj_index_df, node_to_row_id = build_adjacency_index_dataframe(nodes_df, edges_df)

    alice_row = adj_index_df[adj_index_df["node_id"] == "user:alice"].iloc[0]
    assert len(node_to_row_id) == len(nodes_df)

    assert alice_row["out_neighbor_node_ids"] == ["user:bob", "item:laptop", "group:ml"]
    assert alice_row["in_neighbor_node_ids"] == ["user:dave"]


def test_index_only_single_hop_query_runs():
    """验证 index-only 模式的单跳出边查询可以跑通。"""
    graph = LanceDBGraphAdjacency(db_path=TEST_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)

    result = graph.query_out_neighbors_index("user:alice", materialize=False)

    assert result["count"] == 3
    assert result["mode"] == "index-only"
    assert all("row_id" in row for row in result["rows"])


def test_stored_neighbor_row_ids_are_physical_row_ids():
    """验证写入表后的邻居 row_id 已经是最终物理 row_id。"""
    graph = LanceDBGraphAdjacency(db_path=TEST_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)

    alice_entry = graph.get_adj_entry("user:alice")
    expected_out_row_ids = [graph.get_adj_entry(node_id)["physical_row_id"] for node_id in ["user:bob", "item:laptop", "group:ml"]]
    expected_in_row_ids = [graph.get_adj_entry("user:dave")["physical_row_id"]]

    assert _normalize_row_id_list(alice_entry["out_neighbor_row_ids"]) == expected_out_row_ids
    assert _normalize_row_id_list(alice_entry["in_neighbor_row_ids"]) == expected_in_row_ids


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

    alice_row_id = graph.get_adj_entry("user:alice")["physical_row_id"]
    bob_row_id = graph.get_adj_entry("user:bob")["physical_row_id"]
    dave_row_id = graph.get_adj_entry("user:dave")["physical_row_id"]

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


def test_baseline_k_hop_query_returns_layered_rows():
    """验证 baseline 多跳查询可正常返回分层结果。"""
    graph = LanceDBGraphAdjacency(db_path=TEST_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)
    result = query_k_hop(graph.edges_tbl, "user:alice", 2)

    assert result["count"] > 0
    assert len(result["rows"]) == 2

    hop1_targets = {row["dst_id"] for row in result["rows"][0]}
    hop2_targets = {row["dst_id"] for row in result["rows"][1]}

    assert hop1_targets == {"user:bob", "item:laptop", "group:ml"}
    assert hop2_targets == {"user:carol", "brand:lenovo", "topic:retrieval"}


def test_large_synthetic_graph_adjacency_build_is_consistent():
    """验证较大 synthetic 图上邻接表的度数与邻居列表长度保持一致。"""
    edges = generate_edges(
        graph_mode="uniform",
        num_nodes=2000,
        num_edges=10000,
        num_relations=16,
        num_node_types=8,
        seed=7,
        num_communities=8,
        intra_ratio=0.8,
    )
    synthetic_tsv_path = os.path.join(TEST_DB_PATH, "synthetic_large_validation.tsv")
    os.makedirs(os.path.dirname(synthetic_tsv_path), exist_ok=True)

    with open(synthetic_tsv_path, "w", encoding="utf-8") as f:
        f.write("head_type\thead\trelation\ttail_type\ttail\n")
        for src_type, src_id, relation, dst_type, dst_id in edges:
            f.write(f"{src_type}\t{src_id}\t{relation}\t{dst_type}\t{dst_id}\n")

    graph = LanceDBGraphAdjacency(db_path=os.path.join(TEST_DB_PATH, "large_synthetic_case"))
    graph.build_from_tsv(synthetic_tsv_path, cluster_strategy="by_node_type")

    adj_df = graph.adj_index_tbl.to_pandas()
    assert len(adj_df) > 0
    assert adj_df["degree_out"].mean() > 0

    sample_entries = adj_df.sort_values(["degree_out", "degree_in"], ascending=[False, False]).head(20)
    for row in sample_entries.to_dict("records"):
        out_neighbor_row_ids = _normalize_row_id_list(row["out_neighbor_row_ids"])
        in_neighbor_row_ids = _normalize_row_id_list(row["in_neighbor_row_ids"])
        assert row["degree_out"] == len(out_neighbor_row_ids)
        assert row["degree_in"] == len(in_neighbor_row_ids)
        assert row["physical_row_id"] >= 0
