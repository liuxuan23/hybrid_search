from typing import Dict

import pandas as pd


def assign_clusters_by_node_type(nodes_df: pd.DataFrame) -> Dict[str, str]:
    """按 `node_type` 生成 cluster_id。"""
    if "node_id" not in nodes_df.columns:
        raise ValueError("nodes_df 缺少 node_id 列")
    if "node_type" not in nodes_df.columns:
        raise ValueError("nodes_df 缺少 node_type 列")

    assignments = {}
    for row in nodes_df.itertuples(index=False):
        node_id = row.node_id
        node_type = row.node_type if row.node_type else "unknown"
        assignments[node_id] = f"type::{node_type}"
    return assignments


def assign_clusters_by_hash(nodes_df: pd.DataFrame, num_buckets: int) -> Dict[str, str]:
    """按 `node_id` 哈希分桶生成 cluster_id。"""
    if "node_id" not in nodes_df.columns:
        raise ValueError("nodes_df 缺少 node_id 列")
    if num_buckets <= 0:
        raise ValueError("num_buckets 必须大于 0")

    assignments = {}
    for row in nodes_df.itertuples(index=False):
        node_id = row.node_id
        bucket = hash(node_id) % num_buckets
        assignments[node_id] = f"hash::{bucket}"
    return assignments
