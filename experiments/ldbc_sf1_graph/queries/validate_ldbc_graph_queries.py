from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import pandas as pd

from experiments.ldbc_sf1_graph import config
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency


DEFAULT_SAMPLE_SIZE = 8
DEFAULT_RANDOM_SEED = 20260421


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate LDBC SF1 LanceDB graph query paths")
    parser.add_argument("--db-path", type=Path, default=config.LDBC_LANCEDB_DIR)
    parser.add_argument("--edges-path", type=Path, default=config.NORMALIZED_EDGES_PATH)
    parser.add_argument("--sample-size", type=int, default=DEFAULT_SAMPLE_SIZE)
    parser.add_argument("--seed", type=int, default=DEFAULT_RANDOM_SEED)
    parser.add_argument(
        "--start-node",
        type=str,
        default="Person:933",
        help="Seed node used for deterministic single-hop and k-hop validation",
    )
    parser.add_argument("--k", type=int, default=2, help="k used for k-hop validation")
    return parser.parse_args()


def load_edges(edges_path: Path) -> pd.DataFrame:
    if not edges_path.exists():
        raise FileNotFoundError(f"Normalized edges file not found: {edges_path}")
    return pd.read_parquet(edges_path, columns=["src_id", "dst_id"])


def build_expected_neighbors(edges_df: pd.DataFrame, node_id: str) -> tuple[list[str], list[str]]:
    expected_out = sorted(edges_df.loc[edges_df["src_id"] == node_id, "dst_id"].tolist())
    expected_in = sorted(edges_df.loc[edges_df["dst_id"] == node_id, "src_id"].tolist())
    return expected_out, expected_in


def build_expected_batch(edges_df: pd.DataFrame, node_ids: list[str], direction: str) -> dict[str, list[str]]:
    results: dict[str, list[str]] = {}
    for node_id in node_ids:
        expected_out, expected_in = build_expected_neighbors(edges_df, node_id)
        if direction == "out":
            results[node_id] = expected_out
        elif direction == "in":
            results[node_id] = expected_in
        elif direction == "both":
            results[node_id] = sorted(set(expected_out) | set(expected_in))
        else:
            raise ValueError(f"Unsupported direction: {direction}")
    return results


def expected_k_hop(edges_df: pd.DataFrame, start_node: str, k: int, direction: str) -> list[str]:
    if direction not in {"out", "in", "both"}:
        raise ValueError(f"Unsupported direction: {direction}")

    visited = {start_node}
    frontier = [start_node]
    discovered: list[str] = []
    discovered_set = set()

    for _ in range(k):
        if not frontier:
            break

        next_frontier: list[str] = []
        for node_id in frontier:
            out_neighbors, in_neighbors = build_expected_neighbors(edges_df, node_id)
            if direction == "out":
                neighbors = out_neighbors
            elif direction == "in":
                neighbors = in_neighbors
            else:
                neighbors = []
                seen = set()
                for neighbor in out_neighbors + in_neighbors:
                    if neighbor in seen:
                        continue
                    seen.add(neighbor)
                    neighbors.append(neighbor)

            for neighbor in neighbors:
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                next_frontier.append(neighbor)
                if neighbor not in discovered_set:
                    discovered_set.add(neighbor)
                    discovered.append(neighbor)
        frontier = next_frontier

    return sorted(discovered)


def validate_single_hop(graph: LanceDBGraphAdjacency, edges_df: pd.DataFrame, node_id: str) -> dict:
    expected_out, expected_in = build_expected_neighbors(edges_df, node_id)

    out_rows = graph.query_out_neighbors_index(node_id, materialize=True)["rows"]
    in_rows = graph.query_in_neighbors_index(node_id, materialize=True)["rows"]
    both_rows = graph.query_neighbors_index(node_id, materialize=True)["rows"]

    actual_out = sorted(row["node_id"] for row in out_rows)
    actual_in = sorted(row["node_id"] for row in in_rows)
    actual_both = sorted(row["node_id"] for row in both_rows)
    expected_both = sorted(expected_out + expected_in)

    return {
        "node_id": node_id,
        "out_match": actual_out == expected_out,
        "in_match": actual_in == expected_in,
        "both_match": actual_both == expected_both,
        "out_count": len(actual_out),
        "in_count": len(actual_in),
        "both_count": len(actual_both),
        "out_preview": actual_out[:10],
        "in_preview": actual_in[:10],
    }


def validate_batch(graph: LanceDBGraphAdjacency, edges_df: pd.DataFrame, node_ids: list[str], direction: str) -> dict:
    result = graph.query_batch_neighbors_index(node_ids, direction=direction, materialize=True)
    grouped_actual: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for row in result["rows"]:
        grouped_actual.setdefault(row["seed"], []).append(row["node_id"])
    grouped_actual = {seed: sorted(values) for seed, values in grouped_actual.items()}

    expected = build_expected_batch(edges_df, node_ids, direction)
    per_seed = []
    all_match = True
    for seed in node_ids:
        actual_neighbors = grouped_actual.get(seed, [])
        expected_neighbors = expected.get(seed, [])
        match = actual_neighbors == expected_neighbors
        all_match = all_match and match
        per_seed.append(
            {
                "seed": seed,
                "match": match,
                "actual_count": len(actual_neighbors),
                "expected_count": len(expected_neighbors),
                "preview": actual_neighbors[:8],
            }
        )

    return {
        "direction": direction,
        "all_match": all_match,
        "count": result["count"],
        "seeds": per_seed,
    }


def validate_k_hop(graph: LanceDBGraphAdjacency, edges_df: pd.DataFrame, start_node: str, k: int, direction: str) -> dict:
    result = graph.query_k_hop_index(start_node, k=k, materialize=True, direction=direction)
    actual = sorted(row["node_id"] for row in result["rows"])
    expected = expected_k_hop(edges_df, start_node, k=k, direction=direction)
    return {
        "start_node": start_node,
        "k": k,
        "direction": direction,
        "match": actual == expected,
        "actual_count": len(actual),
        "expected_count": len(expected),
        "preview": actual[:12],
    }


def pick_random_nodes(graph: LanceDBGraphAdjacency, sample_size: int, seed: int) -> list[str]:
    adj_df = graph.adj_index_tbl.to_lance().to_table(columns=["node_id"]).to_pandas()
    node_ids = adj_df["node_id"].tolist()
    rng = random.Random(seed)
    sample_size = max(1, min(sample_size, len(node_ids)))
    return rng.sample(node_ids, sample_size)


def main() -> None:
    args = parse_args()
    edges_df = load_edges(args.edges_path)
    graph = LanceDBGraphAdjacency(db_path=str(args.db_path)).load()

    random_nodes = pick_random_nodes(graph, args.sample_size, args.seed)
    single_hop = validate_single_hop(graph, edges_df, args.start_node)
    batch_out = validate_batch(graph, edges_df, random_nodes[: min(4, len(random_nodes))], direction="out")
    batch_both = validate_batch(graph, edges_df, random_nodes[: min(4, len(random_nodes))], direction="both")
    k_hop_out = validate_k_hop(graph, edges_df, args.start_node, args.k, direction="out")
    k_hop_both = validate_k_hop(graph, edges_df, args.start_node, args.k, direction="both")

    report = {
        "db_path": str(args.db_path),
        "edges_path": str(args.edges_path),
        "random_nodes": random_nodes,
        "single_hop": single_hop,
        "batch_out": batch_out,
        "batch_both": batch_both,
        "k_hop_out": k_hop_out,
        "k_hop_both": k_hop_both,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
