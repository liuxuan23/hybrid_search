from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa

from experiments.lancedb_graph.data_prep.build_graph_tables import (
    build_graph_dataframes_from_tsv,
)


GRAPH_SCHEMA_YAML = """nodes:
  nodes:
    id_field: node_id

relationships:
  edges:
    source: src_id
    target: dst_id
"""


def _normalize_nodes_table(nodes_df) -> pa.Table:
    records = []
    for row in nodes_df.to_dict(orient="records"):
        attrs = row.get("attrs")
        if attrs is not None and not isinstance(attrs, str):
            attrs = str(attrs)
        records.append(
            {
                "node_id": row["node_id"],
                "node_type": row.get("node_type", "entity"),
                "community_id": row.get("community_id"),
                "degree_out": row.get("degree_out", 0),
                "degree_in": row.get("degree_in", 0),
                "attrs": attrs,
            }
        )
    return pa.Table.from_pylist(records)


def _normalize_edges_table(edges_df) -> pa.Table:
    records = []
    for row in edges_df.to_dict(orient="records"):
        attrs = row.get("attrs")
        if attrs is not None and not isinstance(attrs, str):
            attrs = str(attrs)
        records.append(
            {
                "edge_id": row.get("edge_id"),
                "src_id": row["src_id"],
                "dst_id": row["dst_id"],
                "edge_type": row.get("edge_type", "REL"),
                "attrs": attrs,
            }
        )
    return pa.Table.from_pylist(records)


def build_lance_graph_from_tsv(tsv_path: Path, output_dir: Path) -> None:
    from knowledge_graph import KnowledgeGraphConfig, LanceGraphStore

    output_dir.mkdir(parents=True, exist_ok=True)
    config = KnowledgeGraphConfig.from_root(output_dir)
    config.ensure_directories()

    schema_path = config.resolved_schema_path()
    if isinstance(schema_path, Path):
        schema_path.write_text(GRAPH_SCHEMA_YAML, encoding="utf-8")
    else:
        raise ValueError("Expected local filesystem path for graph schema")

    nodes_df, edges_df = build_graph_dataframes_from_tsv(str(tsv_path))
    tables = {
        "nodes": _normalize_nodes_table(nodes_df),
        "edges": _normalize_edges_table(edges_df),
    }

    store = LanceGraphStore(config)
    store.ensure_layout()
    store.write_tables(tables)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build official lance_graph storage from TSV")
    parser.add_argument("tsv_path", type=Path, help="Input triples TSV path")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/home/lx/workplace/hybrid_search/storage/lance_graph/cross_db_graph_benchmark"),
        help="Output directory for lance_graph storage",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_lance_graph_from_tsv(args.tsv_path, args.output_dir)
    print(f"Build completed: {args.output_dir}")


if __name__ == "__main__":
    main()