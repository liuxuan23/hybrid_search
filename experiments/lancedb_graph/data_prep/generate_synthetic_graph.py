import argparse
import csv
import json
import os
import random
from collections import Counter
from array import array
from typing import List, Tuple

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
    # 打乱 node_id 顺序
    random.shuffle(node_ids)
    return node_ids


def build_shuffled_node_indices(num_nodes: int, seed: int):
    indices = array("I", range(num_nodes))
    rng = random.Random(seed)
    for idx in range(num_nodes - 1, 0, -1):
        swap_idx = rng.randrange(idx + 1)
        indices[idx], indices[swap_idx] = indices[swap_idx], indices[idx]
    return indices


def build_relation_names(num_relations: int):
    return [f"rel_{idx}" for idx in range(num_relations)]


def build_relation_bytes(num_relations: int) -> List[bytes]:
    return [f"rel_{idx}".encode("utf-8") for idx in range(num_relations)]


def build_node_type_bytes(num_node_types: int) -> List[bytes]:
    return [f"type{idx}".encode("utf-8") for idx in range(num_node_types)]


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


def build_node_community_map(node_ids, num_communities: int):
    """为每个节点分配稳定的 community_id。"""
    node_community_map = {}
    for idx, (_node_type, node_id) in enumerate(node_ids):
        node_community_map[node_id] = idx % num_communities
    return node_community_map


def node_type_for_index(node_index: int, num_node_types: int) -> str:
    return f"type{node_index % num_node_types}"


def node_id_for_index(node_index: int, num_node_types: int) -> str:
    node_type = node_type_for_index(node_index, num_node_types)
    return f"{node_type}:node_{node_index}"


def node_fields_for_index(node_index: int, num_node_types: int) -> Tuple[str, str]:
    node_type = f"type{node_index % num_node_types}"
    return node_type, f"{node_type}:node_{node_index}"


def write_edge_line(
    handle,
    src_index: int,
    dst_index: int,
    relation_index: int,
    num_node_types: int,
    node_type_bytes: List[bytes],
    relation_bytes: List[bytes],
):
    src_type = node_type_bytes[src_index % num_node_types]
    dst_type = node_type_bytes[dst_index % num_node_types]
    handle.write(src_type)
    handle.write(b"\t")
    handle.write(src_type)
    handle.write(b":node_")
    handle.write(str(src_index).encode("ascii"))
    handle.write(b"\t")
    handle.write(relation_bytes[relation_index])
    handle.write(b"\t")
    handle.write(dst_type)
    handle.write(b"\t")
    handle.write(dst_type)
    handle.write(b":node_")
    handle.write(str(dst_index).encode("ascii"))
    handle.write(b"\n")


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


def choose_community_edge_from_indices(indices, relation_names, num_node_types, num_communities, intra_ratio: float, rng):
    if rng.random() < intra_ratio:
        community_id = rng.randrange(num_communities)
        src_index = indices[community_id + num_communities * rng.randrange(max(1, len(indices) // num_communities))]
        dst_index = indices[community_id + num_communities * rng.randrange(max(1, len(indices) // num_communities))]
    else:
        src_index = indices[rng.randrange(len(indices))]
        dst_index = indices[rng.randrange(len(indices))]

    src_type = node_type_for_index(src_index, num_node_types)
    dst_type = node_type_for_index(dst_index, num_node_types)
    src_id = f"{src_type}:node_{src_index}"
    dst_id = f"{dst_type}:node_{dst_index}"
    relation = rng.choice(relation_names)
    return src_type, src_id, relation, dst_type, dst_id


def build_position_to_community(num_nodes: int, num_communities: int) -> array:
    community_ids = array("I", [0]) * num_nodes
    for position in range(num_nodes):
        community_ids[position] = position % num_communities
    return community_ids


def build_community_positions(num_nodes: int, num_communities: int):
    community_positions = [[] for _ in range(num_communities)]
    for position in range(num_nodes):
        community_positions[position % num_communities].append(position)
    return community_positions


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
    # 打乱后的 node_ids 用于分 community
    communities = build_communities(node_ids, num_communities)
    node_community_map = build_node_community_map(node_ids, num_communities)

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
    return edges, node_community_map


def stream_generate_graph(
    graph_mode: str,
    num_nodes: int,
    num_edges: int,
    num_relations: int,
    num_node_types: int,
    seed: int,
    num_communities: int,
    intra_ratio: float,
    output_path: str,
):
    """流式生成图 TSV，避免将所有边保存在内存中。"""
    rng = random.Random(seed)
    shuffled_indices = build_shuffled_node_indices(num_nodes, seed)
    relation_names = build_relation_names(num_relations)
    relation_bytes = build_relation_bytes(num_relations)
    node_type_bytes = build_node_type_bytes(num_node_types)
    position_to_community = build_position_to_community(num_nodes, num_communities)
    community_positions = build_community_positions(num_nodes, num_communities) if graph_mode == "community" else None
    degree_out = Counter()
    degree_in = Counter()

    ensure_parent_dir(output_path)
    with open(output_path, "wb") as f:
        f.write(b"head_type\thead\trelation\ttail_type\ttail\n")

        for _ in range(num_edges):
            if graph_mode == "uniform":
                src_index = shuffled_indices[rng.randrange(num_nodes)]
                dst_index = shuffled_indices[rng.randrange(num_nodes)]
                relation_index = rng.randrange(num_relations)
            elif graph_mode == "powerlaw":
                src_index = shuffled_indices[int(rng.random() ** 2 * num_nodes)]
                dst_index = shuffled_indices[int(rng.random() ** 2 * num_nodes)]
                relation_index = rng.randrange(num_relations)
            elif graph_mode == "community":
                if rng.random() < intra_ratio:
                    community_id = rng.randrange(num_communities)
                    positions = community_positions[community_id]
                    src_position = positions[rng.randrange(len(positions))]
                    dst_position = positions[rng.randrange(len(positions))]
                else:
                    src_position = rng.randrange(num_nodes)
                    dst_position = rng.randrange(num_nodes)
                src_index = shuffled_indices[src_position]
                dst_index = shuffled_indices[dst_position]
                relation_index = rng.randrange(num_relations)
            else:
                raise ValueError(f"不支持的 graph_mode: {graph_mode}")

            src_id = f"type{src_index % num_node_types}:node_{src_index}"
            dst_id = f"type{dst_index % num_node_types}:node_{dst_index}"
            degree_out[src_id] += 1
            degree_in[dst_id] += 1
            write_edge_line(
                f,
                src_index,
                dst_index,
                relation_index,
                num_node_types,
                node_type_bytes,
                relation_bytes,
            )

    community_path = None
    node_community_map = None
    if graph_mode == "community":
        node_community_map = {
            node_id_for_index(node_index, num_node_types): position_to_community[position]
            for position, node_index in enumerate(shuffled_indices)
        }
        community_path = write_node_communities_json(output_path, node_community_map)

    return {
        "output_path": output_path,
        "community_path": community_path,
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "num_relations": num_relations,
        "num_node_types": num_node_types,
        "degree_out": degree_out,
        "degree_in": degree_in,
        "node_community_map": node_community_map,
    }


def write_edges_tsv(output_path: str, edges):
    ensure_parent_dir(output_path)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["head_type", "head", "relation", "tail_type", "tail"])
        writer.writerows(edges)


def write_node_communities_json(output_path: str, node_community_map):
    """将节点 community_id 映射写入伴随文件。"""
    community_path = f"{output_path}.communities.json"
    ensure_parent_dir(community_path)
    with open(community_path, "w", encoding="utf-8") as f:
        json.dump(node_community_map, f, ensure_ascii=False, sort_keys=True)
    return community_path


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
    result = stream_generate_graph(
        graph_mode=args.graph_mode,
        num_nodes=args.num_nodes,
        num_edges=args.num_edges,
        num_relations=args.num_relations,
        num_node_types=args.num_node_types,
        seed=args.seed,
        num_communities=args.num_communities,
        intra_ratio=args.intra_ratio,
        output_path=output_path,
    )

    print("图数据生成完成")
    print(f"graph_mode: {args.graph_mode}")
    print(f"num_nodes: {args.num_nodes}")
    print(f"num_edges: {args.num_edges}")
    print(f"output_path: {output_path}")
    if result["community_path"]:
        print(f"community_path: {result['community_path']}")


if __name__ == "__main__":
    main()
