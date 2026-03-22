import argparse
import os
import random
import statistics

from experiments.lancedb_graph.config import (
    DEFAULT_DB_PATH,
    DEFAULT_INPUT_TSV,
    DEFAULT_K_HOP,
    DEFAULT_RANDOM_SEED,
    DEFAULT_SMOKE_SAMPLE_SIZE,
)
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency
from experiments.lancedb_graph.utils.locality_metrics import compute_cluster_locality_metrics


def main():
    """评估 clustered adjacency 在局部性查询上的收益。

    当前版本的 locality benchmark 采用一个简单且可解释的对照：
    - clustered: `cluster_strategy=by_node_type`
    - unclustered: `cluster_strategy=none`

    指标侧重点：
    1. 单跳 materialized 查询延迟
    2. k-hop materialized 查询延迟
    3. 查询结果规模的均值

    之所以先选 materialized 模式，是因为它更容易观察“邻接行访问是否更集中”。
    当前底层仍是全表读入的正确性实现，因此这里更适合作为实验骨架，
    为后续替换成真实局部读取版本保留统一的输入输出结构。
    """
    parser = argparse.ArgumentParser(description="评估 clustered adjacency 在局部性查询上的收益")
    parser.add_argument("--input-path", type=str, default=DEFAULT_INPUT_TSV)
    parser.add_argument("--db-path", type=str, default=DEFAULT_DB_PATH)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SMOKE_SAMPLE_SIZE)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--k-hop", type=int, default=DEFAULT_K_HOP)
    parser.add_argument("--clustered-strategy", type=str, default="by_node_type")
    args = parser.parse_args()

    random.seed(DEFAULT_RANDOM_SEED)

    clustered_db_path = os.path.join(args.db_path, "benchmark_clustered")
    unclustered_db_path = os.path.join(args.db_path, "benchmark_unclustered")

    clustered_graph = LanceDBGraphAdjacency(db_path=clustered_db_path)
    clustered_graph.build_from_tsv(args.input_path, cluster_strategy=args.clustered_strategy)

    unclustered_graph = LanceDBGraphAdjacency(db_path=unclustered_db_path)
    unclustered_graph.build_from_tsv(args.input_path, cluster_strategy="none")

    sample_nodes = _sample_node_ids(clustered_graph, args.sample_size)
    if not sample_nodes:
        print("未采样到节点，locality benchmark 结束。")
        return

    print_section("Locality Benchmark 配置")
    print(f"input_path: {args.input_path}")
    print(f"clustered_db_path: {clustered_db_path}")
    print(f"unclustered_db_path: {unclustered_db_path}")
    print(f"sample_size: {len(sample_nodes)}")
    print(f"repeat: {args.repeat}")
    print(f"k_hop: {args.k_hop}")
    print(f"clustered_strategy: {args.clustered_strategy}")

    print_section("运行 clustered / unclustered 对比")
    clustered_single_hop = _benchmark_query(
        sample_nodes,
        args.repeat,
        lambda node_id: clustered_graph.query_out_neighbors_index(node_id, materialize=True),
    )
    unclustered_single_hop = _benchmark_query(
        sample_nodes,
        args.repeat,
        lambda node_id: unclustered_graph.query_out_neighbors_index(node_id, materialize=True),
    )
    clustered_khop = _benchmark_query(
        sample_nodes,
        args.repeat,
        lambda node_id: clustered_graph.query_k_hop_index(
            node_id,
            k=args.k_hop,
            materialize=True,
            direction="out",
        ),
    )
    unclustered_khop = _benchmark_query(
        sample_nodes,
        args.repeat,
        lambda node_id: unclustered_graph.query_k_hop_index(
            node_id,
            k=args.k_hop,
            materialize=True,
            direction="out",
        ),
    )

    print_benchmark_result("clustered_single_hop_materialized", clustered_single_hop)
    print_benchmark_result("unclustered_single_hop_materialized", unclustered_single_hop)
    print_benchmark_result(f"clustered_{args.k_hop}_hop_materialized", clustered_khop)
    print_benchmark_result(f"unclustered_{args.k_hop}_hop_materialized", unclustered_khop)

    print_section("局部性指标")
    print_locality_result("clustered_single_hop_locality", clustered_single_hop["locality_metrics"])
    print_locality_result("unclustered_single_hop_locality", unclustered_single_hop["locality_metrics"])
    print_locality_result(f"clustered_{args.k_hop}_hop_locality", clustered_khop["locality_metrics"])
    print_locality_result(f"unclustered_{args.k_hop}_hop_locality", unclustered_khop["locality_metrics"])


def _sample_node_ids(graph: LanceDBGraphAdjacency, sample_size: int):
    """从图中采样节点 id。"""
    graph._ensure_loaded()
    df = graph.adj_index_tbl.search().limit(graph.adj_index_tbl.count_rows()).to_pandas()
    if df.empty:
        return []

    node_ids = df["node_id"].tolist()
    sample_size = min(max(1, int(sample_size)), len(node_ids))
    return random.sample(node_ids, sample_size)


def _benchmark_query(node_ids, repeat: int, query_fn):
    """重复执行查询并输出统一统计。"""
    latency_values = []
    count_values = []
    locality_metric_rows = []

    for _ in range(max(1, int(repeat))):
        for node_id in node_ids:
            result = query_fn(node_id)
            latency_values.append(float(result["time_ms"]))
            count_values.append(int(result["count"]))
            locality_metric_rows.append(compute_cluster_locality_metrics(result["rows"]))

    return {
        "queries": len(latency_values),
        "avg_time_ms": statistics.fmean(latency_values) if latency_values else 0.0,
        "p50_time_ms": _percentile(latency_values, 50),
        "p95_time_ms": _percentile(latency_values, 95),
        "avg_count": statistics.fmean(count_values) if count_values else 0.0,
        "locality_metrics": _aggregate_locality_metrics(locality_metric_rows),
    }


def _percentile(values, percentile: int):
    """计算简单分位数。"""
    if not values:
        return 0.0

    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, round((percentile / 100) * (len(ordered) - 1))))
    return float(ordered[index])


def print_section(title: str):
    print(f"\n{'=' * 72}")
    print(title)
    print("=" * 72)


def print_benchmark_result(name: str, stats: dict):
    print(name)
    print(f"  queries: {stats['queries']}")
    print(f"  avg_time_ms: {stats['avg_time_ms']:.3f}")
    print(f"  p50_time_ms: {stats['p50_time_ms']:.3f}")
    print(f"  p95_time_ms: {stats['p95_time_ms']:.3f}")
    print(f"  avg_count: {stats['avg_count']:.3f}")


def _aggregate_locality_metrics(metrics_list):
    """对多次查询的 locality 指标做均值聚合。"""
    if not metrics_list:
        return {
            "row_count": 0.0,
            "physical_row_span": 0.0,
            "physical_row_gap_avg": 0.0,
            "unique_cluster_count": 0.0,
            "top_cluster_ratio": 0.0,
            "cluster_switches": 0.0,
        }

    keys = metrics_list[0].keys()
    return {
        key: statistics.fmean(metric[key] for metric in metrics_list)
        for key in keys
    }


def print_locality_result(name: str, metrics: dict):
    """打印 locality 指标摘要。"""
    print(name)
    print(f"  avg_row_count: {metrics['row_count']:.3f}")
    print(f"  avg_physical_row_span: {metrics['physical_row_span']:.3f}")
    print(f"  avg_physical_row_gap: {metrics['physical_row_gap_avg']:.3f}")
    print(f"  avg_unique_cluster_count: {metrics['unique_cluster_count']:.3f}")
    print(f"  avg_top_cluster_ratio: {metrics['top_cluster_ratio']:.3f}")
    print(f"  avg_cluster_switches: {metrics['cluster_switches']:.3f}")


if __name__ == "__main__":
    main()
