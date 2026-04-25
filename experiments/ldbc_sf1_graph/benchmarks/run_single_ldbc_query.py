from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from experiments.ldbc_sf1_graph import config
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency


DEFAULT_SEED = "Person:933"


def json_safe(value):
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    if isinstance(value, np.ndarray):
        return [json_safe(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return value.item()
    return value


def trim_preview_rows(rows: list[dict], max_neighbor_values: int = 24) -> list[dict]:
    trimmed: list[dict] = []
    for row in rows:
        normalized_row = json_safe(row)
        if not isinstance(normalized_row, dict):
            trimmed.append(normalized_row)
            continue

        for key in ["out_neighbor_row_ids", "in_neighbor_row_ids", "neighbor_row_ids"]:
            values = normalized_row.get(key)
            if isinstance(values, list) and len(values) > max_neighbor_values:
                normalized_row[key] = values[:max_neighbor_values]
                normalized_row[f"{key}_truncated_count"] = len(values) - max_neighbor_values
        trimmed.append(normalized_row)
    return trimmed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a single LDBC SF1 graph query")
    parser.add_argument("--db-path", type=Path, default=config.LDBC_LANCEDB_DIR)
    parser.add_argument("--seed", default=DEFAULT_SEED, help="Seed node id, e.g. Person:933")
    parser.add_argument(
        "--query-spec",
        choices=["neighbor", "batch_neighbor", "k_hop_2", "k_hop_3"],
        default="neighbor",
        help="Run one query shape at a time",
    )
    parser.add_argument(
        "--direction",
        choices=["out", "in", "both"],
        default="out",
        help="Traversal direction for the selected query",
    )
    parser.add_argument(
        "--seeds",
        nargs="+",
        default=None,
        help="Optional seed list for batch_neighbor; defaults to --seed when omitted",
    )
    parser.add_argument(
        "--materialize",
        choices=["true", "false"],
        default="true",
        help="Whether to materialize returned rows",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full query result as JSON",
    )
    return parser.parse_args()


def build_query_result(graph: LanceDBGraphAdjacency, args: argparse.Namespace) -> dict:
    materialize = args.materialize == "true"

    if args.query_spec == "neighbor":
        if args.direction == "out":
            return graph.query_out_neighbors_index(args.seed, materialize=materialize)
        if args.direction == "in":
            return graph.query_in_neighbors_index(args.seed, materialize=materialize)
        return graph.query_neighbors_index(args.seed, materialize=materialize)

    if args.query_spec == "batch_neighbor":
        seeds = [str(seed) for seed in (args.seeds or [args.seed])]
        return graph.query_batch_neighbors_index(seeds, direction=args.direction, materialize=materialize)

    if args.query_spec == "k_hop_2":
        return graph.query_k_hop_index(args.seed, k=2, direction=args.direction, materialize=materialize)

    if args.query_spec == "k_hop_3":
        return graph.query_k_hop_index(args.seed, k=3, direction=args.direction, materialize=materialize)

    raise ValueError(f"Unsupported query_spec: {args.query_spec}")


def build_summary(args: argparse.Namespace, result: dict) -> str:
    io_stats = result.get("io_stats", {})
    summary = [
        f"seed={args.seed}",
        f"query_spec={args.query_spec}",
        f"direction={args.direction}",
        f"count={result.get('count', 0)}",
        f"time_ms={float(result.get('time_ms', 0.0)):.3f}",
        f"read_bytes={int(io_stats.get('read_bytes', 0))}",
    ]
    if args.query_spec == "batch_neighbor":
        summary.insert(1, f"batch_size={len(args.seeds or [args.seed])}")
    return "\n".join(summary)


def main() -> None:
    args = parse_args()
    graph = LanceDBGraphAdjacency(db_path=str(args.db_path)).load()
    result = build_query_result(graph, args)

    if args.json:
        print(json.dumps(json_safe(result), ensure_ascii=False, indent=2))
        return

    print(build_summary(args, result))
    if args.materialize == "true":
        rows = result.get("rows", [])
        preview = rows[: min(10, len(rows))]
        if preview:
            print("rows_preview=")
            print(json.dumps(trim_preview_rows(preview), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
