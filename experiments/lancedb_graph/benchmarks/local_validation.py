import os

from experiments.lancedb_graph.config import DEFAULT_DB_PATH
from experiments.lancedb_graph.storage_models.lancedb_graph_basic import LanceDBGraphBasic


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPERIMENT_ROOT = os.path.dirname(BASE_DIR)
SAMPLE_TSV = os.path.join(EXPERIMENT_ROOT, "data_prep", "sample_graph.tsv")
SAMPLE_DB_PATH = os.path.join(DEFAULT_DB_PATH, "sample_validation")


def print_section(title: str):
    print(f"\n{'=' * 72}")
    print(title)
    print("=" * 72)


def print_result(name: str, result: dict):
    print(f"{name}: count={result['count']}, time_ms={result['time_ms']:.3f}")
    for row in result["rows"][:5]:
        print(f"  {row}")


def main():
    print_section("构建小样本 LanceDB 图")
    graph = LanceDBGraphBasic(db_path=SAMPLE_DB_PATH)
    graph.build_from_tsv(SAMPLE_TSV)

    print_section("基础统计")
    stats = graph.stats()
    for key, value in stats.items():
        print(f"{key}: {value}")

    print_section("节点查询")
    result = graph.get_node("user:alice")
    print_result("get_node(user:alice)", result)

    print_section("单跳查询")
    out_result = graph.query_out_neighbors("user:alice")
    print_result("out_neighbors(user:alice)", out_result)

    in_result = graph.query_in_neighbors("item:laptop")
    print_result("in_neighbors(item:laptop)", in_result)

    all_result = graph.query_neighbors("group:ml")
    print_result("neighbors(group:ml)", all_result)

    print_section("过滤查询")
    filtered_result = graph.query_out_neighbors("user:alice", edge_type="member_of")
    print_result("out_neighbors(user:alice, edge_type=member_of)", filtered_result)

    print_section("多跳查询")
    hop_result = graph.query_k_hop("user:alice", 2)
    print(f"query_k_hop(user:alice, 2): count={hop_result['count']}, time_ms={hop_result['time_ms']:.3f}")
    for hop_idx, layer in enumerate(hop_result["rows"], start=1):
        print(f"  hop {hop_idx}: {len(layer)} edges")
        for row in layer[:5]:
            print(f"    {row}")

    print_section("验证完成")
    print(f"样本数据路径: {SAMPLE_TSV}")
    print(f"LanceDB 路径: {SAMPLE_DB_PATH}")


if __name__ == "__main__":
    main()
