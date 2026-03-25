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
from experiments.lancedb_graph.storage_models.lancedb_graph_basic import LanceDBGraphBasic


def main():
    """对比 baseline 与 adjacency 查询性能。

    当前版本聚焦最基础、最稳定的三类对比：
    1. baseline 单跳出邻居
    2. adjacency 单跳 index-only
    3. adjacency k-hop index-only

    该脚本的目标是先建立一个可重复运行的 benchmark 骨架，便于后续继续加入：
    - materialized 模式
    - in / both 方向多跳
    - clustered / unclustered 对比
    """
    parser = argparse.ArgumentParser(description="对比 baseline 与 adjacency 查询性能")
    parser.add_argument("--input-path", type=str, default=DEFAULT_INPUT_TSV)
    parser.add_argument("--db-path", type=str, default=DEFAULT_DB_PATH)
    # `sample_size` 表示“本轮 benchmark 随机抽取多少个起始节点做查询”。
    # 例如：sample_size=100, repeat=3
    # 则每一种查询类型会执行 100 * 3 = 300 次。
    # 它不是返回结果条数，而是 benchmark 的采样节点数。
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SMOKE_SAMPLE_SIZE)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--k-hop", type=int, default=DEFAULT_K_HOP)
    parser.add_argument("--cluster-strategy", type=str, default="by_node_type")
    args = parser.parse_args()

    random.seed(DEFAULT_RANDOM_SEED)

    baseline_db_path = os.path.join(args.db_path, "benchmark_baseline")
    adjacency_db_path = os.path.join(args.db_path, "benchmark_adjacency")

    baseline_graph = LanceDBGraphBasic(db_path=baseline_db_path)
    baseline_graph.build_from_tsv(args.input_path)

    adjacency_graph = LanceDBGraphAdjacency(db_path=adjacency_db_path)
    adjacency_graph.build_from_tsv(args.input_path, cluster_strategy=args.cluster_strategy)

    sample_nodes = _sample_node_ids(adjacency_graph, args.sample_size)
    if not sample_nodes:
        print("未采样到节点，benchmark 结束。")
        return

    print_section("Benchmark 配置")
    print(f"input_path: {args.input_path}")
    print(f"baseline_db_path: {baseline_db_path}")
    print(f"adjacency_db_path: {adjacency_db_path}")
    # 这里打印的 `sample_size` 是实际采到的节点个数。
    # 正常情况下它等于命令行传入的 `--sample-size`，
    # 但如果图中节点总数不足，则会自动截断到节点总数。
    print(f"sample_size: {len(sample_nodes)}")
    print(f"repeat: {args.repeat}")
    print(f"k_hop: {args.k_hop}")
    print(f"cluster_strategy: {args.cluster_strategy}")

    print_section("运行查询对比")
    baseline_out_stats = _benchmark_query(
        sample_nodes,
        args.repeat,
        lambda node_id: baseline_graph.query_out_neighbors(node_id),
    )
    baseline_khop_stats = _benchmark_query(
        sample_nodes,
        args.repeat,
        lambda node_id: baseline_graph.query_k_hop(node_id, args.k_hop),
    )
    adjacency_out_stats = _benchmark_query(
        sample_nodes,
        args.repeat,
        lambda node_id: adjacency_graph.query_out_neighbors_index(node_id, materialize=False),
    )
    adjacency_khop_stats = _benchmark_query(
        sample_nodes,
        args.repeat,
        lambda node_id: adjacency_graph.query_k_hop_index(
            node_id,
            k=args.k_hop,
            materialize=False,
            direction="out",
        ),
    )

    print_benchmark_result("baseline_out_neighbors", baseline_out_stats)
    print_benchmark_result(f"baseline_{args.k_hop}_hop", baseline_khop_stats)
    print_benchmark_result("adjacency_out_neighbors_index", adjacency_out_stats)
    print_benchmark_result(f"adjacency_{args.k_hop}_hop_index", adjacency_khop_stats)


def _sample_node_ids(graph: LanceDBGraphAdjacency, sample_size: int):
    """从邻接图中采样节点列表。

    `sample_size` 的含义：
    - 本轮 benchmark 选取多少个 node_id 作为查询起点
    - 后续每种查询都会对这些 node_id 逐个执行
    - 因此它决定了 benchmark 的样本规模，而不是查询结果规模
    """
    graph._ensure_loaded()
    df = graph.adj_index_tbl.search().limit(graph.adj_index_tbl.count_rows()).to_pandas()
    if df.empty:
        return []

    node_ids = df["node_id"].tolist()
    sample_size = min(max(1, int(sample_size)), len(node_ids))
    return random.sample(node_ids, sample_size)


def _benchmark_query(node_ids, repeat: int, query_fn):
    """重复执行查询并汇总延迟与结果规模。

    返回字段说明：
    - `queries`:
        实际执行的查询次数。
        计算方式是：`len(node_ids) * repeat`。
        例如采样 100 个节点、每个节点重复 3 次，则 queries=300。

    - `avg_count`:
        单次查询平均返回多少条结果。
        这里的“结果条数”会根据查询类型复用其 `count` 字段：
        - 单跳查询：通常表示返回的邻居数
        - k-hop 查询：通常表示本次多跳扩展返回的总结果数
        它不是执行次数，也不是节点总数，而是“平均每次 query 命中了多少结果”。
    """
    latency_values = []
    count_values = []
    read_bytes_values = []

    for _ in range(max(1, int(repeat))):
        for node_id in node_ids:
            result = query_fn(node_id)
            latency_values.append(float(result["time_ms"]))
            count_values.append(int(_extract_result_count(result)))
            read_bytes_values.append(int(result.get("io_stats", {}).get("read_bytes", 0)))

    total_time_ms = sum(latency_values)
    query_count = len(latency_values)

    return {
        "queries": query_count,
        "total_time_ms": total_time_ms,
        "avg_time_ms": statistics.fmean(latency_values) if latency_values else 0.0,
        "p50_time_ms": _percentile(latency_values, 50),
        "p95_time_ms": _percentile(latency_values, 95),
        "avg_count": statistics.fmean(count_values) if count_values else 0.0,
        "throughput_qps": (query_count / (total_time_ms / 1000.0)) if total_time_ms > 0 else 0.0,
        "avg_read_bytes": statistics.fmean(read_bytes_values) if read_bytes_values else 0.0,
        "total_read_bytes": sum(read_bytes_values),
    }


def _extract_result_count(result):
    """统一提取不同查询返回结构下的结果规模。

    baseline `query_k_hop` 的 `rows` 是按 hop 分层的二维列表；
    其他查询一般直接返回扁平列表和 `count`。
    这里统一优先信任显式 `count`，不存在时再从 `rows` 推导。
    这个值最终会参与 `avg_count` 统计。
    """
    if "count" in result:
        return result["count"]

    rows = result.get("rows", [])
    if not rows:
        return 0

    if isinstance(rows[0], list):
        return sum(len(layer) for layer in rows)
    return len(rows)


def _percentile(values, percentile: int):
    """计算简单分位数，避免引入额外依赖。"""
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
    # `queries`:
    #   该 query 类型本轮一共执行了多少次。
    #   一般等于 `sample_size * repeat`。
    #
    # `avg_count`:
    #   该 query 类型平均每次返回多少条结果。
    #   例如单跳时可理解为平均邻居数，2-hop 时可理解为平均多跳命中结果数。
    print(name)
    print(f"  queries: {stats['queries']}")
    print(f"  avg_time_ms: {stats['avg_time_ms']:.3f}")
    print(f"  p50_time_ms: {stats['p50_time_ms']:.3f}")
    print(f"  p95_time_ms: {stats['p95_time_ms']:.3f}")
    print(f"  avg_count: {stats['avg_count']:.3f}")
    print(f"  throughput_qps: {stats['throughput_qps']:.3f}")
    print(f"  avg_read_bytes: {stats['avg_read_bytes']:.3f}")
    print(f"  total_read_bytes: {stats['total_read_bytes']}")


if __name__ == "__main__":
    main()
