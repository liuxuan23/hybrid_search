#!/usr/bin/env python
"""Analyze Lance load/open/search/take efficiency on the benchmark graph table."""

from __future__ import annotations

import statistics
import time

from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency

DB_PATH = "/home/liuxuan/workplace/hybrid_search/storage/lancedb_graph/cross_db_graph_benchmark"
SEED = "type386:node_33386"
REPEATS = 12
WARMUP = 3
COLD_REPEATS = 6


def _measure_ms(func, repeats: int = REPEATS, warmup: int = WARMUP):
    for _ in range(warmup):
        func()
    values = []
    for _ in range(repeats):
        start = time.perf_counter()
        func()
        values.append((time.perf_counter() - start) * 1000.0)
    return values


def _stats(values):
    return {
        "avg": statistics.mean(values),
        "p50": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }


def _paired_stats(values):
    return {
        "first_avg": statistics.mean(first for first, _ in values),
        "second_avg": statistics.mean(second for _, second in values),
        "delta_avg": statistics.mean(first - second for first, second in values),
        "speedup_avg": statistics.mean((first / second) for first, second in values if second > 0),
    }


def _open_graph():
    return LanceDBGraphAdjacency(db_path=DB_PATH).load()


def _timed_ms(func):
    start = time.perf_counter()
    result = func()
    return result, (time.perf_counter() - start) * 1000.0


def _seed_context():
    graph = _open_graph()
    adj_tbl = graph.adj_index_tbl
    mapping_df = adj_tbl.search().select(["node_id", "physical_row_id", "out_neighbor_row_ids"]).to_pandas()
    node_to_row = {
        row["node_id"]: {
            "physical_row_id": int(row["physical_row_id"]),
            "out_neighbor_row_ids": [int(v) for v in row["out_neighbor_row_ids"]],
        }
        for row in mapping_df.to_dict("records")
    }
    entry_df = adj_tbl.search().where(f"node_id = '{SEED}'").with_row_id(True).to_pandas()
    assert not entry_df.empty, f"missing seed: {SEED}"
    entry = entry_df.to_dict("records")[0]
    out_row_ids = [int(v) for v in entry["out_neighbor_row_ids"]]
    assert out_row_ids, f"seed {SEED} has no out neighbors"
    return {
        "graph": graph,
        "adj_tbl": adj_tbl,
        "lance_ds": adj_tbl.to_lance(),
        "node_to_row": node_to_row,
        "seed_physical_row_id": int(entry["physical_row_id"]),
        "neighbor_row_ids": sorted(set(out_row_ids)),
    }


def run_load_efficiency_breakdown():
    ctx = _seed_context()
    graph = ctx["graph"]
    adj_tbl = ctx["adj_tbl"]
    lance_ds = ctx["lance_ds"]
    node_to_row = ctx["node_to_row"]
    seed_physical_row_id = ctx["seed_physical_row_id"]
    neighbor_row_ids = ctx["neighbor_row_ids"]

    def open_and_load():
        loaded = _open_graph()
        assert loaded.adj_index_tbl is not None
        return loaded

    def table_to_lance():
        ds = adj_tbl.to_lance()
        assert ds is not None
        return ds

    def search_seed_only():
        df = adj_tbl.search().where(f"node_id = '{SEED}'").to_pandas()
        assert len(df) == 1
        return df

    def search_seed_with_rowid():
        df = adj_tbl.search().where(f"node_id = '{SEED}'").with_row_id(True).to_pandas()
        assert len(df) == 1
        return df

    def take_seed_only():
        tbl = lance_ds.take([seed_physical_row_id])
        assert tbl.num_rows == 1
        return tbl

    def take_neighbors_arrow():
        tbl = lance_ds.take(neighbor_row_ids)
        assert tbl.num_rows == len(neighbor_row_ids)
        return tbl

    def take_neighbors_to_pandas():
        df = lance_ds.take(neighbor_row_ids).to_pandas()
        assert len(df) == len(neighbor_row_ids)
        return df

    def map_lookup_only():
        entry = node_to_row.get(SEED)
        assert entry is not None
        return entry

    def map_then_take_neighbors_arrow():
        entry = node_to_row.get(SEED)
        assert entry is not None
        tbl = lance_ds.take(sorted(set(entry["out_neighbor_row_ids"])))
        assert tbl.num_rows == len(neighbor_row_ids)
        return tbl

    def map_then_take_neighbors_to_pandas():
        entry = node_to_row.get(SEED)
        assert entry is not None
        df = lance_ds.take(sorted(set(entry["out_neighbor_row_ids"]))).to_pandas()
        assert len(df) == len(neighbor_row_ids)
        return df

    def full_neighbor_query():
        result = graph.query_out_neighbors_index(SEED, materialize=True)
        assert result["count"] == len(neighbor_row_ids)
        return result

    def cold_open_then_search_once():
        local_graph = _open_graph()
        local_adj_tbl = local_graph.adj_index_tbl
        df = local_adj_tbl.search().where(f"node_id = '{SEED}'").to_pandas()
        assert len(df) == 1
        return df

    def cold_open_then_to_lance_then_take_once():
        local_graph = _open_graph()
        local_ds = local_graph.adj_index_tbl.to_lance()
        tbl = local_ds.take([seed_physical_row_id])
        assert tbl.num_rows == 1
        return tbl

    def search_twice_same_table():
        _, first_ms = _timed_ms(lambda: adj_tbl.search().where(f"node_id = '{SEED}'").to_pandas())
        _, second_ms = _timed_ms(lambda: adj_tbl.search().where(f"node_id = '{SEED}'").to_pandas())
        return first_ms, second_ms

    def take_twice_same_dataset():
        _, first_ms = _timed_ms(lambda: lance_ds.take([seed_physical_row_id]))
        _, second_ms = _timed_ms(lambda: lance_ds.take([seed_physical_row_id]))
        return first_ms, second_ms

    search_pairs = [search_twice_same_table() for _ in range(COLD_REPEATS)]
    take_pairs = [take_twice_same_dataset() for _ in range(COLD_REPEATS)]

    return {
        "seed": SEED,
        "seed_physical_row_id": seed_physical_row_id,
        "neighbor_count": len(neighbor_row_ids),
        "mapping_size": len(node_to_row),
        "open_and_load_ms": _stats(_measure_ms(open_and_load, repeats=8, warmup=1)),
        "table_to_lance_ms": _stats(_measure_ms(table_to_lance)),
        "search_seed_only_ms": _stats(_measure_ms(search_seed_only)),
        "search_seed_with_rowid_ms": _stats(_measure_ms(search_seed_with_rowid)),
        "cold_open_then_search_ms": _stats(_measure_ms(cold_open_then_search_once, repeats=COLD_REPEATS, warmup=0)),
        "search_twice_same_table_ms": _paired_stats(search_pairs),
        "map_lookup_only_ms": _stats(_measure_ms(map_lookup_only)),
        "take_seed_only_ms": _stats(_measure_ms(take_seed_only)),
        "cold_open_then_take_ms": _stats(_measure_ms(cold_open_then_to_lance_then_take_once, repeats=COLD_REPEATS, warmup=0)),
        "take_twice_same_dataset_ms": _paired_stats(take_pairs),
        "take_neighbors_arrow_ms": _stats(_measure_ms(take_neighbors_arrow)),
        "take_neighbors_to_pandas_ms": _stats(_measure_ms(take_neighbors_to_pandas)),
        "map_then_take_neighbors_arrow_ms": _stats(_measure_ms(map_then_take_neighbors_arrow)),
        "map_then_take_neighbors_to_pandas_ms": _stats(_measure_ms(map_then_take_neighbors_to_pandas)),
        "full_neighbor_query_ms": _stats(_measure_ms(full_neighbor_query)),
    }


def test_lance_load_efficiency():
    summary = run_load_efficiency_breakdown()
    print("\n[lance load efficiency]")
    print(f"seed={summary['seed']}")
    print(f"seed_physical_row_id={summary['seed_physical_row_id']}")
    print(f"neighbor_count={summary['neighbor_count']}")
    print(f"mapping_size={summary['mapping_size']}")
    for key, value in summary.items():
        if key in {"seed", "seed_physical_row_id", "neighbor_count", "mapping_size"}:
            continue
        print(f"{key}={value}")

    assert summary["neighbor_count"] > 0


if __name__ == "__main__":
    test_lance_load_efficiency()