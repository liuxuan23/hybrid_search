def build_adjacency_stats(adj_index_tbl):
    """输出邻接索引表基础统计。"""
    df = adj_index_tbl.search().select(["node_id", "degree_out", "degree_in", "cluster_id"]).to_pandas()

    if df.empty:
        return {
            "num_adj_rows": 0,
            "avg_out_degree": 0.0,
            "avg_in_degree": 0.0,
            "max_out_degree": 0,
            "max_in_degree": 0,
            "num_clusters": 0,
        }

    return {
        "num_adj_rows": int(len(df)),
        "avg_out_degree": float(df["degree_out"].mean()),
        "avg_in_degree": float(df["degree_in"].mean()),
        "max_out_degree": int(df["degree_out"].max()),
        "max_in_degree": int(df["degree_in"].max()),
        "num_clusters": int(df["cluster_id"].nunique()),
    }
