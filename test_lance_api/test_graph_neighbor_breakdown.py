#!/usr/bin/env python
"""Break down LanceDB graph neighbor query latency into storage vs Python overhead."""

from __future__ import annotations

import statistics
import time

from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency
from experiments.lancedb_graph.query_engines import adjacency_queries as aq

DB_PATH = "/home/liuxuan/workplace/hybrid_search/storage/lancedb_graph/cross_db_graph_benchmark"
REPEATS = 20
WARMUP = 5
SEED = "type386:node_33386"


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


def run_breakdown(seed: str = SEED):
    graph = LanceDBGraphAdjacency(db_path=DB_PATH).load()
    adj_tbl = graph.adj_index_tbl

    def entry_only():
        result = aq.get_adj_entry(adj_tbl, seed)
        assert result["count"] == 1
        return result

    entry = entry_only()["rows"][0]
    neighbor_row_ids = aq._normalize_row_id_list(entry.get("out_neighbor_row_ids"))
    assert neighbor_row_ids, f"seed {seed} has no outgoing neighbors"

    def take_only():
        rows = aq._materialize_adj_rows(adj_tbl, neighbor_row_ids)
        assert len(rows) == len(neighbor_row_ids)
        return rows

    def full_query():
        result = graph.query_out_neighbors_index(seed, materialize=True)
        assert result["count"] == len(neighbor_row_ids)
        return result

    entry_times = _measure_ms(entry_only)
    take_times = _measure_ms(take_only)
    full_times = _measure_ms(full_query)
    python_overhead = [max(0.0, full - entry - take) for full, entry, take in zip(full_times, entry_times, take_times)]

    return {
        "seed": seed,
        "neighbor_count": len(neighbor_row_ids),
        "entry_only_ms": _stats(entry_times),
        "take_only_ms": _stats(take_times),
        "full_query_ms": _stats(full_times),
        "python_overhead_ms": _stats(python_overhead),
    }


def test_graph_neighbor_breakdown():
    summary = run_breakdown()
    print("\n[graph neighbor breakdown]")
    print(f"seed={summary['seed']}")
    print(f"neighbor_count={summary['neighbor_count']}")
    print(f"entry_only_ms={summary['entry_only_ms']}")
    print(f"take_only_ms={summary['take_only_ms']}")
    print(f"full_query_ms={summary['full_query_ms']}")
    print(f"python_overhead_ms={summary['python_overhead_ms']}")

    assert summary["neighbor_count"] > 0
    assert summary["full_query_ms"]["avg"] >= 0.0


if __name__ == "__main__":
    test_graph_neighbor_breakdown()
