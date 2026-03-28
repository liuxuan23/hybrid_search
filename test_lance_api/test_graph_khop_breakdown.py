#!/usr/bin/env python
"""Break down LanceDB graph k-hop latency into storage and Python traversal overhead."""

from __future__ import annotations

import statistics
import time

from experiments.cross_db_graph import config
from experiments.lancedb_graph.query_engines import traversal as tr
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency

REPEATS = 10
WARMUP = 3
SEED = "type386:node_33386"
K = 3
DIRECTION = "out"


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


def _run_khop_once(adj_tbl, seed: str, k: int, direction: str):
    phase = {
        "start_lookup_ms": 0.0,
        "frontier_lookup_ms": 0.0,
        "reorder_ms": 0.0,
        "batch_fetch_ms": 0.0,
        "batch_fetch_take_ms": 0.0,
        "batch_fetch_to_pandas_ms": 0.0,
        "batch_fetch_to_records_ms": 0.0,
        "python_frontier_ms": 0.0,
        "per_hop": [],
    }

    total_start = time.perf_counter()
    start_lookup_begin = time.perf_counter()
    start_row = tr._get_row_by_node_id(adj_tbl, seed)
    phase["start_lookup_ms"] += (time.perf_counter() - start_lookup_begin) * 1000.0
    if start_row is None:
        return {"count": 0, "time_ms": (time.perf_counter() - total_start) * 1000.0, "phase": phase}

    visited = {seed}
    frontier_node_ids = [seed]
    discovered_rows = []
    discovered_node_ids = set()
    row_cache_by_node_id = {seed: start_row}
    row_cache_by_physical_row_id = {}

    start_physical_row_id = start_row.get("physical_row_id", start_row.get("_rowid"))
    if start_physical_row_id is not None:
        row_cache_by_physical_row_id[int(start_physical_row_id)] = start_row

    for depth in range(1, k + 1):
        if not frontier_node_ids:
            break

        hop = {
            "depth": depth,
            "frontier_size": len(frontier_node_ids),
            "neighbor_candidate_count": 0,
            "next_frontier_size": 0,
            "reorder_ms": 0.0,
            "frontier_lookup_ms": 0.0,
            "batch_fetch_ms": 0.0,
            "batch_fetch_take_ms": 0.0,
            "batch_fetch_to_pandas_ms": 0.0,
            "batch_fetch_to_records_ms": 0.0,
            "python_frontier_ms": 0.0,
        }

        reorder_begin = time.perf_counter()
        frontier_node_ids = tr._reorder_frontier_node_ids(
            frontier_node_ids,
            row_cache_by_node_id=row_cache_by_node_id,
            adj_index_tbl=adj_tbl,
            row_cache_by_physical_row_id=row_cache_by_physical_row_id,
        )
        hop["reorder_ms"] = (time.perf_counter() - reorder_begin) * 1000.0
        phase["reorder_ms"] += hop["reorder_ms"]

        frontier_rows = []
        frontier_lookup_begin = time.perf_counter()
        for current_node_id in frontier_node_ids:
            current_row = row_cache_by_node_id.get(current_node_id)
            if current_row is None:
                current_row = tr._get_row_by_node_id(adj_tbl, current_node_id)
                if current_row is None:
                    continue
                row_cache_by_node_id[current_node_id] = current_row
                current_physical_row_id = current_row.get("physical_row_id", current_row.get("_rowid"))
                if current_physical_row_id is not None:
                    row_cache_by_physical_row_id[int(current_physical_row_id)] = current_row
            frontier_rows.append(current_row)
        hop["frontier_lookup_ms"] = (time.perf_counter() - frontier_lookup_begin) * 1000.0
        phase["frontier_lookup_ms"] += hop["frontier_lookup_ms"]

        python_begin = time.perf_counter()
        aggregated_neighbor_row_ids = []
        seen_neighbor_row_ids = set()
        for current_row in frontier_rows:
            for neighbor_row_id in tr._get_neighbor_row_ids(current_row, direction):
                neighbor_row_id = int(neighbor_row_id)
                if neighbor_row_id in seen_neighbor_row_ids:
                    continue
                seen_neighbor_row_ids.add(neighbor_row_id)
                aggregated_neighbor_row_ids.append(neighbor_row_id)
        hop["neighbor_candidate_count"] = len(aggregated_neighbor_row_ids)

        missing_row_ids = [
            neighbor_row_id
            for neighbor_row_id in aggregated_neighbor_row_ids
            if neighbor_row_id not in row_cache_by_physical_row_id
        ]
        python_mid = time.perf_counter()

        if missing_row_ids:
            fetch_begin = time.perf_counter()
            lance_ds = adj_tbl.to_lance()
            take_begin = time.perf_counter()
            arrow_tbl = lance_ds.take(sorted(set(int(row_id) for row_id in missing_row_ids)))
            hop["batch_fetch_take_ms"] = (time.perf_counter() - take_begin) * 1000.0

            to_pandas_begin = time.perf_counter()
            df = arrow_tbl.to_pandas()
            hop["batch_fetch_to_pandas_ms"] = (time.perf_counter() - to_pandas_begin) * 1000.0

            if "_rowid" not in df.columns:
                df = df.copy()
                df["_rowid"] = sorted(set(int(row_id) for row_id in missing_row_ids))

            to_records_begin = time.perf_counter()
            fetched_rows = df.to_dict("records")
            hop["batch_fetch_to_records_ms"] = (time.perf_counter() - to_records_begin) * 1000.0

            hop["batch_fetch_ms"] = (time.perf_counter() - fetch_begin) * 1000.0
            phase["batch_fetch_ms"] += hop["batch_fetch_ms"]
            phase["batch_fetch_take_ms"] += hop["batch_fetch_take_ms"]
            phase["batch_fetch_to_pandas_ms"] += hop["batch_fetch_to_pandas_ms"]
            phase["batch_fetch_to_records_ms"] += hop["batch_fetch_to_records_ms"]
            for row in fetched_rows:
                physical_row_id = row.get("physical_row_id", row.get("_rowid"))
                if physical_row_id is not None:
                    row_cache_by_physical_row_id[int(physical_row_id)] = row
                row_cache_by_node_id[row["node_id"]] = row

        next_frontier_node_ids = []
        for neighbor_row_id in aggregated_neighbor_row_ids:
            neighbor_row = row_cache_by_physical_row_id.get(int(neighbor_row_id))
            if neighbor_row is None:
                continue
            neighbor_node_id = neighbor_row["node_id"]
            if neighbor_node_id in visited:
                continue
            visited.add(neighbor_node_id)
            next_frontier_node_ids.append(neighbor_node_id)
            if neighbor_node_id not in discovered_node_ids:
                discovered_node_ids.add(neighbor_node_id)
                discovered_rows.append({"row_id": int(neighbor_row_id)})

        hop["next_frontier_size"] = len(next_frontier_node_ids)
        hop["python_frontier_ms"] = (
            (python_mid - python_begin) + (time.perf_counter() - python_mid)
        ) * 1000.0 - hop["batch_fetch_ms"]
        phase["python_frontier_ms"] += hop["python_frontier_ms"]
        phase["per_hop"].append(hop)
        frontier_node_ids = next_frontier_node_ids

    total_ms = (time.perf_counter() - total_start) * 1000.0
    return {
        "count": len(discovered_rows),
        "time_ms": total_ms,
        "phase": phase,
    }


def run_khop_breakdown(seed: str = SEED, k: int = K, direction: str = DIRECTION):
    graph = LanceDBGraphAdjacency(db_path=str(config.LANCEDB_DB_PATH)).load()
    adj_tbl = graph.adj_index_tbl

    single_run = _run_khop_once(adj_tbl, seed, k, direction)

    total_times = _measure_ms(lambda: _run_khop_once(adj_tbl, seed, k, direction))
    start_lookup_times = _measure_ms(lambda: tr._get_row_by_node_id(adj_tbl, seed))

    return {
        "seed": seed,
        "k": k,
        "direction": direction,
        "result_count": single_run["count"],
        "full_query_ms": _stats(total_times),
        "single_run_phase": single_run["phase"],
        "start_lookup_ms": _stats(start_lookup_times),
    }


def test_graph_khop_breakdown():
    summary = run_khop_breakdown()
    print("\n[graph k-hop breakdown]")
    print(f"seed={summary['seed']}")
    print(f"k={summary['k']}")
    print(f"direction={summary['direction']}")
    print(f"result_count={summary['result_count']}")
    print(f"full_query_ms={summary['full_query_ms']}")
    print(f"start_lookup_ms={summary['start_lookup_ms']}")
    print("per_hop=")
    for hop in summary['single_run_phase']['per_hop']:
        print(hop)
    print(
        "phase_totals=",
        {
            key: value
            for key, value in summary['single_run_phase'].items()
            if key != 'per_hop'
        },
    )

    assert summary["result_count"] > 0
    assert summary["full_query_ms"]["avg"] >= 0.0


if __name__ == "__main__":
    test_graph_khop_breakdown()
