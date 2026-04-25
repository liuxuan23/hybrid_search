from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import pandas as pd

from experiments.ldbc_sf1_graph import config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export benchmark seeds from normalized or LanceDB-backed LDBC graph data")
    parser.add_argument("--nodes-path", type=Path, default=config.NORMALIZED_NODES_PATH)
    parser.add_argument("--edges-path", type=Path, default=config.NORMALIZED_EDGES_PATH)
    parser.add_argument("--output-path", type=Path, default=config.LDBC_SEEDS_PATH)
    parser.add_argument("--seed", type=int, default=config.DEFAULT_RANDOM_SEED)
    parser.add_argument("--sample-size", type=int, default=config.DEFAULT_SEED_SAMPLE_SIZE)
    parser.add_argument("--person-type", default=config.DEFAULT_PERSON_NODE_TYPE)
    parser.add_argument("--batch-size", type=int, default=config.DEFAULT_BATCH_QUERY_SIZE)
    parser.add_argument("--low-percentile", type=float, default=config.DEFAULT_LOW_PERCENTILE)
    parser.add_argument("--mid-percentile", type=float, default=config.DEFAULT_MID_PERCENTILE)
    return parser.parse_args()


def load_person_nodes(nodes_path: Path, person_type: str) -> pd.DataFrame:
    if not nodes_path.exists():
        raise FileNotFoundError(f"Normalized nodes file not found: {nodes_path}")

    nodes_df = pd.read_parquet(nodes_path)
    filtered = nodes_df[nodes_df["node_type"] == person_type].copy()
    if filtered.empty:
        raise ValueError(f"No nodes found for person type: {person_type}")
    return filtered


def load_degree_series(edges_path: Path) -> pd.Series:
    if not edges_path.exists():
        raise FileNotFoundError(f"Normalized edges file not found: {edges_path}")

    edges_df = pd.read_parquet(edges_path, columns=["src_id", "dst_id"])
    degree_out = edges_df.groupby("src_id").size()
    degree_in = edges_df.groupby("dst_id").size()
    total_degree = degree_out.add(degree_in, fill_value=0).astype(int)
    total_degree.name = "total_degree"
    return total_degree


def attach_degree(nodes_df: pd.DataFrame, degree_series: pd.Series) -> pd.DataFrame:
    enriched = nodes_df.copy()
    enriched = enriched.merge(degree_series, how="left", left_on="node_id", right_index=True)
    enriched["total_degree"] = enriched["total_degree"].fillna(0).astype(int)
    return enriched.sort_values(["total_degree", "node_id"]).reset_index(drop=True)


def stratify_degree_buckets(
    nodes_df: pd.DataFrame,
    low_percentile: float,
    mid_percentile: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """Split nodes into low / mid / high degree buckets.

    Percentiles are computed over the selected node population, typically `Person` nodes.
    """
    if not 0.0 < low_percentile < mid_percentile < 1.0:
        raise ValueError(
            "Percentiles must satisfy 0.0 < low_percentile < mid_percentile < 1.0"
        )

    low_cutoff = float(nodes_df["total_degree"].quantile(low_percentile))
    mid_cutoff = float(nodes_df["total_degree"].quantile(mid_percentile))

    low_bucket = nodes_df[nodes_df["total_degree"] <= low_cutoff].copy()
    mid_bucket = nodes_df[
        (nodes_df["total_degree"] > low_cutoff) & (nodes_df["total_degree"] <= mid_cutoff)
    ].copy()
    high_bucket = nodes_df[nodes_df["total_degree"] > mid_cutoff].copy()

    stats = {
        "low_cutoff": low_cutoff,
        "mid_cutoff": mid_cutoff,
        "low_count": int(len(low_bucket)),
        "mid_count": int(len(mid_bucket)),
        "high_count": int(len(high_bucket)),
    }
    return low_bucket, mid_bucket, high_bucket, stats


def sample_bucket_nodes(bucket_df: pd.DataFrame, sample_size: int, rng: random.Random) -> list[str]:
    node_ids = bucket_df["node_id"].tolist()
    if not node_ids:
        return []
    chosen_size = min(sample_size, len(node_ids))
    return rng.sample(node_ids, k=chosen_size)


def sample_batch_groups(node_ids: list[str], batch_size: int, rng: random.Random, num_groups: int = 3) -> list[list[str]]:
    if not node_ids:
        return []

    groups = []
    unique_nodes = list(dict.fromkeys(node_ids))
    for _ in range(num_groups):
        chosen_size = min(batch_size, len(unique_nodes))
        groups.append(rng.sample(unique_nodes, k=chosen_size))
    return groups


def export_seed_payload(
    nodes_df: pd.DataFrame,
    sample_size: int,
    batch_size: int,
    rng: random.Random,
    low_percentile: float,
    mid_percentile: float,
) -> dict:
    low_bucket, mid_bucket, high_bucket, bucket_stats = stratify_degree_buckets(
        nodes_df,
        low_percentile=low_percentile,
        mid_percentile=mid_percentile,
    )

    low_seeds = sample_bucket_nodes(low_bucket, sample_size, rng)
    mid_seeds = sample_bucket_nodes(mid_bucket, sample_size, rng)
    high_seeds = sample_bucket_nodes(high_bucket, sample_size, rng)
    combined_single = low_seeds + mid_seeds + high_seeds
    all_person_nodes = nodes_df["node_id"].tolist()
    random_single = sample_bucket_nodes(nodes_df, sample_size, rng)

    return {
        "dataset": "ldbc_sf1",
        "seed": getattr(rng, "seed", None),
        "node_type": config.DEFAULT_PERSON_NODE_TYPE,
        "degree_metric": "total_degree",
        "bucket_stats": bucket_stats,
        "single_seeds": {
            "low_degree": low_seeds,
            "mid_degree": mid_seeds,
            "high_degree": high_seeds,
            "random": random_single,
            "combined": combined_single,
        },
        "batch_seeds": {
            "low_degree": sample_batch_groups(low_seeds or all_person_nodes, batch_size, rng),
            "mid_degree": sample_batch_groups(mid_seeds or all_person_nodes, batch_size, rng),
            "high_degree": sample_batch_groups(high_seeds or all_person_nodes, batch_size, rng),
            "mixed": sample_batch_groups(combined_single or all_person_nodes, batch_size, rng),
        },
    }


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    nodes_df = load_person_nodes(args.nodes_path, args.person_type)
    degree_series = load_degree_series(args.edges_path)
    nodes_with_degree = attach_degree(nodes_df, degree_series)
    payload = export_seed_payload(
        nodes_with_degree,
        sample_size=args.sample_size,
        batch_size=args.batch_size,
        rng=rng,
        low_percentile=args.low_percentile,
        mid_percentile=args.mid_percentile,
    )
    payload["seed"] = args.seed
    payload["node_type"] = args.person_type

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Exported seeds to {args.output_path}")
    print(f"Low single seeds: {len(payload['single_seeds']['low_degree'])}")
    print(f"Mid single seeds: {len(payload['single_seeds']['mid_degree'])}")
    print(f"High single seeds: {len(payload['single_seeds']['high_degree'])}")
    print(f"Random single seeds: {len(payload['single_seeds']['random'])}")
    print(f"Mixed batch groups: {len(payload['batch_seeds']['mixed'])}")


if __name__ == "__main__":
    main()
