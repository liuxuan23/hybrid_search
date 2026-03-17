import argparse
import csv
import os
import random

from experiments.lancedb_graph.config import (
    DEFAULT_COMMUNITY_INTRA_RATIO,
    DEFAULT_RANDOM_SEED,
    DEFAULT_SYNTHETIC_DATA_DIR,
    DEFAULT_SYNTHETIC_NUM_COMMUNITIES,
    DEFAULT_SYNTHETIC_NUM_EDGES,
    DEFAULT_SYNTHETIC_NUM_NODES,
    DEFAULT_SYNTHETIC_NUM_NODE_TYPES,
    DEFAULT_SYNTHETIC_NUM_RELATIONS,
)
from experiments.lancedb_graph.utils.io import ensure_parent_dir


def build_node_ids(num_nodes: int, num_node_types: int):
    node_types = [f"type{i}" for i in range(num_node_types)]
    node_ids = []
    for idx in range(num_nodes):
        node_type = node_types[idx % num_node_types]
        node_ids.append((node_type, f"{node_type}:node_{idx}"))
    return node_ids


def build_relation_names(num_relations: int):
    return [f"rel_{idx}" for idx in range(num_relations)]


def choose_uniform_edge(node_ids, relation_names, rng):
    src_type, src_id = rng.choice(node_ids)
    dst_type, dst_id = rng.choice(node_ids)
    relation = rng.choice(relation_names)
    return src_type, src_id, relation, dst_type, dst_id


def choose_powerlaw_edge(node_ids, relation_names, rng):
    # random.choices 使用带权采样，头部节点更容易被抽到，形成热点。
    weights = [1.0 / ((idx + 1) ** 0.75) for idx in range(len(node_ids))]
    src_type, src_id = rng.choices(node_ids, weights=weights, k=1)[0]
    dst_type, dst_id = rng.choices(node_ids, weights=weights, k=1)[0]
    relation = rng.choice(relation_names)
    return src_type, src_id, relation, dst_type, dst_id


def build_communities(node_ids, num_communities: int):
    communities = [[] for _ in range(num_communities)]
    for idx, item in enumerate(node_ids):
        communities[idx % num_communities].append(item)
    return communities


def choose_community_edge(node_ids, relation_names, communities, intra_ratio: float, rng):
    # 以较高概率在同一社区内采样边，制造“社区内稠密、社区间稀疏”的结构。
    if rng.random() < intra_ratio:
        community = rng.choice(communities)
        # 起点和终点都从同一个社区中选择，形成社区内部连接。
        src_type, src_id = rng.choice(community)
        dst_type, dst_id = rng.choice(community)
    else:
        # 以较低概率从全图随机连边，保留少量跨社区连接，避免社区完全割裂。
        src_type, src_id = rng.choice(node_ids)
        dst_type, dst_id = rng.choice(node_ids)
    relation = rng.choice(relation_names)
    return src_type, src_id, relation, dst_type, dst_id


def generate_edges(
    graph_mode: str,
    num_nodes: int,
    num_edges: int,
    num_relations: int,
    num_node_types: int,
    seed: int,
    num_communities: int,
    intra_ratio: float,
):
    rng = random.Random(seed)
    node_ids = build_node_ids(num_nodes, num_node_types)
    relation_names = build_relation_names(num_relations)
    communities = build_communities(node_ids, num_communities)

    edges = []
    for _ in range(num_edges):
        if graph_mode == "uniform":
            edge = choose_uniform_edge(node_ids, relation_names, rng)
        elif graph_mode == "powerlaw":
            edge = choose_powerlaw_edge(node_ids, relation_names, rng)
        elif graph_mode == "community":
            edge = choose_community_edge(
                node_ids,
                relation_names,
                communities,
                intra_ratio,
                rng,
            )
        else:
            raise ValueError(f"不支持的 graph_mode: {graph_mode}")
        edges.append(edge)
    return edges


def write_edges_tsv(output_path: str, edges):
    ensure_parent_dir(output_path)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["head_type", "head", "relation", "tail_type", "tail"])
        writer.writerows(edges)


def default_output_path(graph_mode: str, num_edges: int):
    filename = f"synthetic_{graph_mode}_{num_edges}.tsv"
    return os.path.join(DEFAULT_SYNTHETIC_DATA_DIR, filename)


def main():
    parser = argparse.ArgumentParser(description="生成可控分布的图 TSV 数据")
    parser.add_argument(
        "--graph-mode",
        choices=["uniform", "powerlaw", "community"],
        default="powerlaw",
        help="图分布模式",
    )
    parser.add_argument("--num-nodes", type=int, default=DEFAULT_SYNTHETIC_NUM_NODES)
    parser.add_argument("--num-edges", type=int, default=DEFAULT_SYNTHETIC_NUM_EDGES)
    parser.add_argument("--num-relations", type=int, default=DEFAULT_SYNTHETIC_NUM_RELATIONS)
    parser.add_argument("--num-node-types", type=int, default=DEFAULT_SYNTHETIC_NUM_NODE_TYPES)
    parser.add_argument("--num-communities", type=int, default=DEFAULT_SYNTHETIC_NUM_COMMUNITIES)
    parser.add_argument("--intra-ratio", type=float, default=DEFAULT_COMMUNITY_INTRA_RATIO)
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument("--output-path", type=str, default=None)
    args = parser.parse_args()

    output_path = args.output_path or default_output_path(args.graph_mode, args.num_edges)
    edges = generate_edges(
        graph_mode=args.graph_mode,
        num_nodes=args.num_nodes,
        num_edges=args.num_edges,
        num_relations=args.num_relations,
        num_node_types=args.num_node_types,
        seed=args.seed,
        num_communities=args.num_communities,
        intra_ratio=args.intra_ratio,
    )
    write_edges_tsv(output_path, edges)

    print("图数据生成完成")
    print(f"graph_mode: {args.graph_mode}")
    print(f"num_nodes: {args.num_nodes}")
    print(f"num_edges: {args.num_edges}")
    print(f"output_path: {output_path}")


if __name__ == "__main__":
    main()
