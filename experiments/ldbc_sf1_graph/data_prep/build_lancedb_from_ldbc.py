from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

from experiments.ldbc_sf1_graph import config
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build LanceDB graph storage from normalized LDBC SF1 tables")
    parser.add_argument("--nodes-path", type=Path, default=config.NORMALIZED_NODES_PATH)
    parser.add_argument("--edges-path", type=Path, default=config.NORMALIZED_EDGES_PATH)
    parser.add_argument("--db-path", type=Path, default=config.LDBC_LANCEDB_DIR)
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing LanceDB tables if present")
    parser.add_argument(
        "--cluster-strategy",
        choices=["none", "by_node_type", "hash", "community"],
        default="none",
        help="Cluster strategy passed to the existing LanceDB adjacency graph builder",
    )
    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=config.DEFAULT_BATCH_SIZE,
        help="Batch size used when writing nodes, edges, and adjacency index tables",
    )
    return parser.parse_args()


def load_normalized_frames(nodes_path: Path, edges_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not nodes_path.exists():
        raise FileNotFoundError(f"Normalized nodes file not found: {nodes_path}")
    if not edges_path.exists():
        raise FileNotFoundError(f"Normalized edges file not found: {edges_path}")
    return pd.read_parquet(nodes_path), pd.read_parquet(edges_path)


def enrich_nodes_with_graph_columns(nodes_df: pd.DataFrame, edges_df: pd.DataFrame) -> pd.DataFrame:
    """Prepare normalized LDBC nodes for the existing `lancedb_graph` builder.

    The adjacency graph builder expects `degree_out`, `degree_in`, `community_id`, and `attrs_json`.
    For the first LDBC version we keep `community_id=None` and reuse existing `attrs_json` values.
    """
    prepared = nodes_df.copy()
    degree_out = edges_df.groupby("src_id").size().rename("degree_out")
    degree_in = edges_df.groupby("dst_id").size().rename("degree_in")

    prepared = prepared.merge(degree_out, how="left", left_on="node_id", right_index=True)
    prepared = prepared.merge(degree_in, how="left", left_on="node_id", right_index=True)
    prepared["degree_out"] = prepared["degree_out"].fillna(0).astype(int)
    prepared["degree_in"] = prepared["degree_in"].fillna(0).astype(int)

    if "community_id" not in prepared.columns:
        prepared["community_id"] = None

    if "attrs_json" not in prepared.columns:
        prepared["attrs_json"] = "{}"

    required_node_columns = [
        "node_id",
        "node_type",
        "degree_out",
        "degree_in",
        "community_id",
        "attrs_json",
    ]
    return prepared[required_node_columns]


def build_lancedb_graph(
    nodes_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    db_path: Path,
    overwrite: bool,
    cluster_strategy: str,
    write_batch_size: int,
) -> LanceDBGraphAdjacency:
    """Build LanceDB graph tables from normalized LDBC DataFrames.

    This first implementation intentionally reuses `LanceDBGraphAdjacency.build_from_dataframes`
    so the LDBC path shares the same nodes/edges/adj_index construction logic as the existing experiments.
    """
    db_path.mkdir(parents=True, exist_ok=True)

    if overwrite:
        os.environ["LANCEDB_OVERWRITE_TABLES"] = "1"

    prepared_nodes_df = enrich_nodes_with_graph_columns(nodes_df, edges_df)
    graph = LanceDBGraphAdjacency(
        db_path=str(db_path),
        write_batch_size=write_batch_size,
    )
    graph.build_from_dataframes(
        nodes_df=prepared_nodes_df,
        edges_df=edges_df,
        cluster_strategy=cluster_strategy,
    )
    return graph


def main() -> None:
    args = parse_args()
    nodes_df, edges_df = load_normalized_frames(args.nodes_path, args.edges_path)
    graph = build_lancedb_graph(
        nodes_df,
        edges_df,
        args.db_path,
        overwrite=args.overwrite,
        cluster_strategy=args.cluster_strategy,
        write_batch_size=args.write_batch_size,
    )
    print(
        "Prepared graph tables:",
        graph.nodes_table_name,
        graph.edges_table_name,
        graph.adj_index_table_name,
    )
    print(f"LanceDB graph prepared at {args.db_path}")


if __name__ == "__main__":
    main()
