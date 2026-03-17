import argparse
import random

from experiments.lancedb_graph.config import (
    DEFAULT_DB_PATH,
    DEFAULT_INPUT_TSV,
    DEFAULT_RANDOM_SEED,
    DEFAULT_SMOKE_SAMPLE_SIZE,
)
from experiments.lancedb_graph.storage_models.lancedb_graph_basic import LanceDBGraphBasic


def main():
    parser = argparse.ArgumentParser(description="基础冒烟测试")
    parser.add_argument("--input-path", type=str, default=DEFAULT_INPUT_TSV)
    parser.add_argument("--db-path", type=str, default=DEFAULT_DB_PATH)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SMOKE_SAMPLE_SIZE)
    args = parser.parse_args()

    random.seed(DEFAULT_RANDOM_SEED)

    graph = LanceDBGraphBasic(db_path=args.db_path)
    graph.build_from_tsv(args.input_path)

    stats = graph.stats()
    print("基础统计:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    nodes_result = graph.nodes_tbl.search().limit(args.sample_size).to_pandas()
    sample_nodes = nodes_result["node_id"].tolist()

    for node_id in sample_nodes[: min(3, len(sample_nodes))]:
        print(f"\n测试节点: {node_id}")

        out_result = graph.query_out_neighbors(node_id)
        print(f"  出邻居: count={out_result['count']}, time_ms={out_result['time_ms']:.3f}")

        in_result = graph.query_in_neighbors(node_id)
        print(f"  入邻居: count={in_result['count']}, time_ms={in_result['time_ms']:.3f}")

        all_result = graph.query_neighbors(node_id)
        print(f"  双向邻居: count={all_result['count']}, time_ms={all_result['time_ms']:.3f}")

        hop2_result = graph.query_k_hop(node_id, 2)
        print(f"  2-hop: count={hop2_result['count']}, time_ms={hop2_result['time_ms']:.3f}")


if __name__ == "__main__":
    main()
