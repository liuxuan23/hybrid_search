import csv
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Iterator, Tuple

import pandas as pd

from experiments.lancedb_graph.config import DEFAULT_ATTRS_JSON, DEFAULT_NODE_TYPE


@dataclass
class EdgeRecord:
    edge_id: str
    src_id: str
    dst_id: str
    edge_type: str
    src_type: str
    dst_type: str
    attrs_json: str


@dataclass
class NodeRecord:
    node_id: str
    node_type: str
    degree_out: int
    degree_in: int
    community_id: int | None
    attrs_json: str


def normalize_node_type(node_type: str) -> str:
    if node_type:
        return node_type
    return DEFAULT_NODE_TYPE


def build_graph_dataframes_from_tsv(tsv_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    从 triples.tsv 构建节点表和边表。

    期望输入列：
    - head_type
    - head
    - relation
    - tail_type
    - tail
    """
    node_info = {}
    degree_out = defaultdict(int)
    degree_in = defaultdict(int)
    edge_rows = []
    node_community_map = _load_node_community_map(tsv_path)

    with open(tsv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for idx, row in enumerate(reader):
            src_id = row["head"]
            dst_id = row["tail"]
            src_type = normalize_node_type(row.get("head_type", ""))
            dst_type = normalize_node_type(row.get("tail_type", ""))
            edge_type = row["relation"]

            degree_out[src_id] += 1
            degree_in[dst_id] += 1

            if src_id not in node_info:
                node_info[src_id] = {
                    "node_id": src_id,
                    "node_type": src_type,
                    "attrs_json": DEFAULT_ATTRS_JSON,
                    "community_id": node_community_map.get(src_id),
                }
            if dst_id not in node_info:
                node_info[dst_id] = {
                    "node_id": dst_id,
                    "node_type": dst_type,
                    "attrs_json": DEFAULT_ATTRS_JSON,
                    "community_id": node_community_map.get(dst_id),
                }

            edge_rows.append(
                {
                    "edge_id": f"edge_{idx}",
                    "src_id": src_id,
                    "dst_id": dst_id,
                    "edge_type": edge_type,
                    "src_type": src_type,
                    "dst_type": dst_type,
                    "attrs_json": DEFAULT_ATTRS_JSON,
                }
            )

    node_rows = []
    for node_id, info in node_info.items():
        node_rows.append(
            {
                "node_id": node_id,
                "node_type": info["node_type"],
                "degree_out": degree_out[node_id],
                "degree_in": degree_in[node_id],
                "community_id": info.get("community_id"),
                "attrs_json": info["attrs_json"],
            }
        )

    nodes_df = pd.DataFrame(node_rows)
    edges_df = pd.DataFrame(edge_rows)
    return nodes_df, edges_df


def iter_edge_records_from_tsv(tsv_path: str) -> Iterator[EdgeRecord]:
    with open(tsv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for idx, row in enumerate(reader):
            yield EdgeRecord(
                edge_id=f"edge_{idx}",
                src_id=row["head"],
                dst_id=row["tail"],
                edge_type=row["relation"],
                src_type=normalize_node_type(row.get("head_type", "")),
                dst_type=normalize_node_type(row.get("tail_type", "")),
                attrs_json=DEFAULT_ATTRS_JSON,
            )


def build_node_records_from_tsv(tsv_path: str) -> list[NodeRecord]:
    node_info = {}
    degree_out = defaultdict(int)
    degree_in = defaultdict(int)
    node_community_map = _load_node_community_map(tsv_path)

    for edge in iter_edge_records_from_tsv(tsv_path):
        degree_out[edge.src_id] += 1
        degree_in[edge.dst_id] += 1

        if edge.src_id not in node_info:
            node_info[edge.src_id] = {
                "node_id": edge.src_id,
                "node_type": edge.src_type,
                "community_id": node_community_map.get(edge.src_id),
            }
        if edge.dst_id not in node_info:
            node_info[edge.dst_id] = {
                "node_id": edge.dst_id,
                "node_type": edge.dst_type,
                "community_id": node_community_map.get(edge.dst_id),
            }

    return [
        NodeRecord(
            node_id=node_id,
            node_type=info["node_type"],
            degree_out=degree_out[node_id],
            degree_in=degree_in[node_id],
            community_id=info.get("community_id"),
            attrs_json=DEFAULT_ATTRS_JSON,
        )
        for node_id, info in node_info.items()
    ]


def batched(iterable: Iterable, batch_size: int):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _load_node_community_map(tsv_path: str):
    """如果存在伴随 community 文件，则加载节点到 community_id 的映射。"""
    community_path = f"{tsv_path}.communities.json"
    if not os.path.exists(community_path):
        return {}

    with open(community_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    return {node_id: int(community_id) for node_id, community_id in loaded.items()}
