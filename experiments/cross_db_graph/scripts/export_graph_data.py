import json

from experiments.cross_db_graph import config
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import (
    LanceDBGraphAdjacency,
)


def _pick_evenly_spaced(values, sample_size):
    values = list(values)
    if sample_size <= 0 or not values:
        return []
    if len(values) <= sample_size:
        return values

    step = max(1, len(values) // sample_size)
    picked = []
    for idx in range(0, len(values), step):
        picked.append(values[idx])
        if len(picked) >= sample_size:
            break
    return picked


def generate_seeds(sample_size_per_bucket=10, batch_size=None):
    graph = LanceDBGraphAdjacency(db_path=str(config.LANCEDB_DB_PATH)).load()
    df = graph.adj_index_tbl.search().select(["node_id", "degree_out", "degree_in"]).to_pandas()
    if df.empty:
        raise ValueError("adj_index table is empty; cannot generate seeds")

    df = df.copy()
    df["degree_total"] = df["degree_out"].fillna(0) + df["degree_in"].fillna(0)
    df = df.sort_values(["degree_total", "node_id"], ascending=[True, True]).reset_index(drop=True)

    n = len(df)
    low_df = df.iloc[: max(1, n // 3)]
    medium_df = df.iloc[max(1, n // 3) : max(2, (2 * n) // 3)]
    high_df = df.iloc[max(2, (2 * n) // 3) :]

    low_degree = _pick_evenly_spaced(low_df["node_id"].tolist(), sample_size_per_bucket)
    medium_degree = _pick_evenly_spaced(medium_df["node_id"].tolist(), sample_size_per_bucket)
    high_degree = _pick_evenly_spaced(high_df["node_id"].tolist(), sample_size_per_bucket)

    combined = low_degree + medium_degree + high_degree
    batch_size = batch_size or min(config.DEFAULT_BATCH_SIZE, len(combined))
    batch_seed_set = combined[:batch_size]

    payload = {
        "low_degree": low_degree,
        "medium_degree": medium_degree,
        "high_degree": high_degree,
        "batch_seed_set": batch_seed_set,
    }

    with open(config.SEEDS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return payload


def main():
    payload = generate_seeds()
    print(f"Generated seeds file: {config.SEEDS_FILE}")
    print(
        "counts:",
        {key: len(value) for key, value in payload.items()},
    )


if __name__ == "__main__":
    main()
