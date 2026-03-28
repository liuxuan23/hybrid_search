#!/usr/bin/env python
"""Compare basic non-graph search latency between LanceDB and PostgreSQL."""

from __future__ import annotations

import statistics
import time

import psycopg

from experiments.cross_db_graph import config
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency

LANCEDB_PATH = "/home/liuxuan/workplace/hybrid_search/storage/lancedb_graph/cross_db_graph_benchmark"
POSTGRES_DSN = config.POSTGRES_DSN
REPEATS = 20
WARMUP = 5
NODE_ID = "type386:node_33386"
COMMUNITY_ID = "community_33"
LIMIT = 128


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


def _quote_lance_literal(value):
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


def _resolve_community_value(nodes_tbl):
    sample = nodes_tbl.search().limit(1).to_arrow()
    community_col = sample.column("community_id")
    sample_value = community_col[0].as_py()
    if isinstance(sample_value, int):
        digits = "".join(ch for ch in COMMUNITY_ID if ch.isdigit())
        return int(digits)
    return COMMUNITY_ID


def run_search_comparison():
    graph = LanceDBGraphAdjacency(db_path=LANCEDB_PATH).load()
    nodes_tbl = graph.nodes_tbl
    community_value = _resolve_community_value(nodes_tbl)
    community_literal = _quote_lance_literal(community_value)

    with psycopg.connect(POSTGRES_DSN) as conn:
        def lancedb_node_pk():
            tbl = nodes_tbl.search().where(f"node_id = '{NODE_ID}'").limit(1).to_arrow()
            assert tbl.num_rows == 1
            return tbl

        def postgres_node_pk():
            with conn.cursor() as cur:
                cur.execute("SELECT node_id FROM graph_nodes WHERE node_id = %s LIMIT 1", (NODE_ID,))
                row = cur.fetchone()
            assert row is not None
            return row

        def lancedb_node_type_prefix():
            tbl = nodes_tbl.search().where("node_type = 'type386'").limit(LIMIT).to_arrow()
            assert tbl.num_rows >= 1
            return tbl

        def postgres_node_type_prefix():
            with conn.cursor() as cur:
                cur.execute("SELECT node_id FROM graph_nodes WHERE node_type = %s LIMIT %s", ("type386", LIMIT))
                rows = cur.fetchall()
            assert len(rows) >= 1
            return rows

        def lancedb_community_filter():
            tbl = nodes_tbl.search().where(f"community_id = {community_literal}").limit(LIMIT).to_arrow()
            assert tbl.num_rows >= 1
            return tbl

        def postgres_community_filter():
            with conn.cursor() as cur:
                cur.execute("SELECT node_id FROM graph_nodes WHERE community_id = %s LIMIT %s", (str(community_value), LIMIT))
                rows = cur.fetchall()
            assert len(rows) >= 1
            return rows

        def lancedb_degree_range():
            tbl = nodes_tbl.search().where("degree_out >= 10 AND degree_out < 20").limit(LIMIT).to_arrow()
            assert tbl.num_rows >= 1
            return tbl

        def postgres_degree_range():
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT node_id FROM graph_nodes WHERE degree_out >= %s AND degree_out < %s LIMIT %s",
                    (10, 20, LIMIT),
                )
                rows = cur.fetchall()
            assert len(rows) >= 1
            return rows

        return {
            "lancedb_node_pk_ms": _stats(_measure_ms(lancedb_node_pk)),
            "postgres_node_pk_ms": _stats(_measure_ms(postgres_node_pk)),
            "lancedb_node_type_filter_ms": _stats(_measure_ms(lancedb_node_type_prefix)),
            "postgres_node_type_filter_ms": _stats(_measure_ms(postgres_node_type_prefix)),
            "lancedb_community_filter_ms": _stats(_measure_ms(lancedb_community_filter)),
            "postgres_community_filter_ms": _stats(_measure_ms(postgres_community_filter)),
            "lancedb_degree_range_ms": _stats(_measure_ms(lancedb_degree_range)),
            "postgres_degree_range_ms": _stats(_measure_ms(postgres_degree_range)),
        }


def test_lancedb_vs_postgres_search():
    summary = run_search_comparison()
    print("\n[lancedb vs postgres basic search]")
    for key, value in summary.items():
        print(f"{key}={value}")

    assert summary["lancedb_node_pk_ms"]["avg"] > 0.0
    assert summary["postgres_node_pk_ms"]["avg"] > 0.0


if __name__ == "__main__":
    test_lancedb_vs_postgres_search()
