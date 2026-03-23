import argparse
from typing import Iterable

from experiments.lancedb_graph.query_engines.basic_queries import query_k_hop
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency


DEFAULT_SAMPLE_COUNT = 5
DEFAULT_MAX_NEIGHBOR_PREVIEW = 8


def _normalize_list(values):
    if values is None:
        return []
    if hasattr(values, "tolist"):
        values = values.tolist()
    return list(values)


def _format_preview(values: Iterable, limit: int) -> str:
    values = list(values)
    preview = values[:limit]
    suffix = " ..." if len(values) > limit else ""
    return f"{preview}{suffix}"


def _print_section(title: str):
    print(f"\n{'=' * 88}")
    print(title)
    print(f"{'=' * 88}")


def _print_graph_stats(graph: LanceDBGraphAdjacency):
    adj_tbl = graph.adj_index_tbl
    adj_count = adj_tbl.count_rows()
    print(f"num_adj_rows: {adj_count}")


def _extract_node_ids(rows, key: str):
    return {row[key] for row in rows}


def _pick_sample_node_ids(adj_df, sample_count: int):
    if adj_df.empty:
        return []

    sorted_df = adj_df.sort_values(["degree_out", "degree_in", "node_id"], ascending=[False, False, True])
    node_ids = sorted_df["node_id"].tolist()

    if len(node_ids) <= sample_count:
        return node_ids

    step = max(1, len(node_ids) // sample_count)
    sampled = []
    for idx in range(0, len(node_ids), step):
        sampled.append(node_ids[idx])
        if len(sampled) >= sample_count:
            break
    return sampled


def _pick_sample_node_ids_from_table(graph: LanceDBGraphAdjacency, sample_count: int):
    if sample_count <= 0:
        return []

    # 这里只读取挑样本所需的最小列，避免把整张大表都拉成 pandas。
    adj_df = graph.adj_index_tbl.search().select(["node_id", "degree_out", "degree_in"]).limit(
        max(sample_count * 200, 1000)
    ).to_pandas()
    return _pick_sample_node_ids(adj_df, sample_count)


def _print_compare_result(node_id: str, hop: int, baseline_node_ids, adjacency_node_ids, max_neighbor_preview: int):
    print(f"node_id: {node_id}")
    print(f"hop: {hop}")
    print(f"baseline_count: {len(baseline_node_ids)}")
    print(f"adjacency_count: {len(adjacency_node_ids)}")
    print(f"baseline_preview: {_format_preview(sorted(baseline_node_ids), max_neighbor_preview)}")
    print(f"adjacency_preview: {_format_preview(sorted(adjacency_node_ids), max_neighbor_preview)}")
    print(f"matched: {baseline_node_ids == adjacency_node_ids}")


def _verify_hop_match(graph: LanceDBGraphAdjacency, node_id: str, hop: int):
    if hop == 1:
        baseline_result = graph.query_out_neighbors_baseline(node_id)
        adjacency_result = graph.query_out_neighbors_index(node_id, materialize=True)
        baseline_node_ids = _extract_node_ids(baseline_result["rows"], "dst_id")
        adjacency_node_ids = _extract_node_ids(adjacency_result["rows"], "node_id")
        return baseline_node_ids, adjacency_node_ids

    baseline_result = query_k_hop(graph.edges_tbl, node_id, hop)
    adjacency_result = graph.query_k_hop_index(node_id, k=hop, materialize=True, direction="out")
    baseline_node_ids = {
        row["dst_id"] for layer in baseline_result["rows"] for row in layer
    }
    adjacency_node_ids = _extract_node_ids(adjacency_result["rows"], "node_id")
    return baseline_node_ids, adjacency_node_ids


def main():
    parser = argparse.ArgumentParser(description="在较大图上验证邻接表构建结果，并打印实际邻接表示例")
    parser.add_argument("--input-path", required=True, help="输入 TSV 路径")
    parser.add_argument("--db-path", required=True, help="LanceDB 输出目录")
    parser.add_argument(
        "--cluster-strategy",
        choices=["by_node_type", "hash", "none"],
        default="by_node_type",
        help="邻接表构建时使用的 cluster 策略",
    )
    parser.add_argument(
        "--sample-count",
        type=int,
        default=DEFAULT_SAMPLE_COUNT,
        help="打印多少个邻接表样例节点",
    )
    parser.add_argument(
        "--max-neighbor-preview",
        type=int,
        default=DEFAULT_MAX_NEIGHBOR_PREVIEW,
        help="每跳结果最多展示多少个 node_id 预览",
    )
    args = parser.parse_args()

    graph = LanceDBGraphAdjacency(db_path=args.db_path)
    graph.build_from_tsv(args.input_path, cluster_strategy=args.cluster_strategy)

    _print_section("大图邻接表构建统计")
    print(f"input_path: {args.input_path}")
    print(f"db_path: {args.db_path}")
    print(f"cluster_strategy: {args.cluster_strategy}")
    _print_graph_stats(graph)

    sample_node_ids = _pick_sample_node_ids_from_table(graph, max(1, args.sample_count))

    _print_section("1-hop / 2-hop node_id 一致性验证")
    for sample_idx, node_id in enumerate(sample_node_ids, start=1):
        print(f"[sample {sample_idx}]")
        for hop in [1, 2]:
            baseline_node_ids, adjacency_node_ids = _verify_hop_match(graph, node_id, hop)
            _print_compare_result(
                node_id=node_id,
                hop=hop,
                baseline_node_ids=baseline_node_ids,
                adjacency_node_ids=adjacency_node_ids,
                max_neighbor_preview=args.max_neighbor_preview,
            )
            assert baseline_node_ids == adjacency_node_ids, f"{node_id} 的 {hop}-hop 查询结果不一致"
        print("-")

    print("\n大图邻接表构建与抽样验证完成")


if __name__ == "__main__":
    main()
