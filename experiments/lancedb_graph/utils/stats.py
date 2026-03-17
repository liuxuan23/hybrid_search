def build_basic_graph_stats(nodes_tbl, edges_tbl):
    nodes_df = nodes_tbl.search().limit(nodes_tbl.count_rows()).to_pandas()
    edges_df = edges_tbl.search().limit(edges_tbl.count_rows()).to_pandas()

    node_count = len(nodes_df)
    edge_count = len(edges_df)

    avg_out = nodes_df["degree_out"].mean() if node_count else 0
    avg_in = nodes_df["degree_in"].mean() if node_count else 0

    return {
        "node_count": node_count,
        "edge_count": edge_count,
        "node_type_count": nodes_df["node_type"].nunique() if node_count else 0,
        "edge_type_count": edges_df["edge_type"].nunique() if edge_count else 0,
        "avg_degree_out": float(avg_out) if node_count else 0.0,
        "avg_degree_in": float(avg_in) if node_count else 0.0,
    }
