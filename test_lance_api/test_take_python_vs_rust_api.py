#!/usr/bin/env python
"""Compare LanceDB table take path with lower-level Lance dataset take path."""

from __future__ import annotations

import statistics
import time

from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency

DB_PATH = "/home/liuxuan/workplace/hybrid_search/storage/lancedb_graph/cross_db_graph_benchmark"
SEED = "type386:node_33386"
REPEATS = 20
WARMUP = 5


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


def _load_row_ids():
    graph = LanceDBGraphAdjacency(db_path=DB_PATH).load()
    entry = graph.get_adj_entry(SEED)
    assert entry is not None, f"missing seed: {SEED}"
    row_ids = [int(v) for v in entry["out_neighbor_row_ids"]]
    assert row_ids, f"seed {SEED} has no out neighbors"
    return graph, row_ids


def run_take_comparison():
    graph, row_ids = _load_row_ids()
    adj_tbl = graph.adj_index_tbl
    lance_ds = adj_tbl.to_lance()
    sorted_row_ids = sorted(set(int(v) for v in row_ids))

    def lancedb_take_arrow():
        tbl = adj_tbl.take_row_ids(row_ids)
        assert tbl is not None
        return tbl

    def lance_take_arrow():
        tbl = lance_ds.take(sorted_row_ids)
        assert tbl is not None
        return tbl

    def lancedb_take_to_pandas():
        df = adj_tbl.take_row_ids(row_ids).to_pandas()
        assert len(df) > 0
        return df

    def lance_take_to_pandas():
        df = lance_ds.take(sorted_row_ids).to_pandas()
        assert len(df) == len(sorted_row_ids)
        return df

    return {
        "seed": SEED,
        "row_id_count": len(sorted_row_ids),
        "lancedb_take_row_count": len(adj_tbl.take_row_ids(row_ids).to_list()),
        "lance_take_row_count": lance_ds.take(sorted_row_ids).num_rows,
        "lancedb_take_arrow_ms": _stats(_measure_ms(lancedb_take_arrow)),
        "lance_take_arrow_ms": _stats(_measure_ms(lance_take_arrow)),
        "lancedb_take_to_pandas_ms": _stats(_measure_ms(lancedb_take_to_pandas)),
        "lance_take_to_pandas_ms": _stats(_measure_ms(lance_take_to_pandas)),
    }


def test_take_python_vs_rust_api():
    summary = run_take_comparison()
    print("\n[take python vs rust api]")
    print(f"seed={summary['seed']}")
    print(f"row_id_count={summary['row_id_count']}")
    for key, value in summary.items():
        if key in {"seed", "row_id_count"}:
            continue
        print(f"{key}={value}")

    assert summary["row_id_count"] > 0


if __name__ == "__main__":
    test_take_python_vs_rust_api()
