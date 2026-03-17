import csv
from collections import defaultdict
from typing import Tuple

import pandas as pd

from experiments.lancedb_graph.config import DEFAULT_ATTRS_JSON, DEFAULT_NODE_TYPE


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
                }
            if dst_id not in node_info:
                node_info[dst_id] = {
                    "node_id": dst_id,
                    "node_type": dst_type,
                    "attrs_json": DEFAULT_ATTRS_JSON,
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
                "attrs_json": info["attrs_json"],
            }
        )

    nodes_df = pd.DataFrame(node_rows)
    edges_df = pd.DataFrame(edge_rows)
    return nodes_df, edges_df
