#!/usr/bin/env python
"""
LanceDB 批处理均摊与 IN 扩展性测试。

运行:
    uv run pytest test_lance_api/test_query_latency_batch_patterns.py -v -s
"""

from test_lance_api.query_latency_utils import (
    BATCH_SIZE,
    IN_SIZES,
    PATTERN_REPEATS,
    benchmark_ids,
    benchmark_total_ms,
    create_table,
    print_amortized_table,
    print_in_scaling_table,
    print_rowid_samples,
    search_id_in_batch,
    search_id_single,
    take_id_batch,
    take_id_single,
    temp_db,
)


def _benchmark_take_patterns(table, row_id_map: dict[int, int], target_ids: list[int]) -> list[dict]:
    target_row_ids = [row_id_map[target_id] for target_id in target_ids]

    def run_single_loop():
        for row_id, target_id in zip(target_row_ids, target_ids):
            take_id_single(table, row_id, target_id)

    def run_batch_once():
        take_id_batch(table, target_row_ids, target_ids)

    single_stats = benchmark_total_ms(run_single_loop, repeats=PATTERN_REPEATS)
    batch_stats = benchmark_total_ms(run_batch_once, repeats=PATTERN_REPEATS)

    single_row = {
        "name": "take_row_ids single x N",
        "n": len(target_ids),
        "per_row_ms": single_stats["total_avg_ms"] / len(target_ids),
        **single_stats,
    }
    batch_row = {
        "name": "take_row_ids batch",
        "n": len(target_ids),
        "per_row_ms": batch_stats["total_avg_ms"] / len(target_ids),
        **batch_stats,
    }
    single_row["speedup_vs_single"] = 1.0
    batch_row["speedup_vs_single"] = single_row["per_row_ms"] / batch_row["per_row_ms"]
    return [single_row, batch_row]


def _benchmark_search_patterns(table, target_ids: list[int]) -> list[dict]:
    def run_single_loop():
        for target_id in target_ids:
            search_id_single(table, target_id)

    def run_batch():
        search_id_in_batch(table, target_ids)

    single_stats = benchmark_total_ms(run_single_loop, repeats=PATTERN_REPEATS)
    batch_stats = benchmark_total_ms(run_batch, repeats=PATTERN_REPEATS)

    single_row = {
        "name": "search id = x, single x N",
        "n": len(target_ids),
        "per_row_ms": single_stats["total_avg_ms"] / len(target_ids),
        **single_stats,
    }
    batch_row = {
        "name": "search id IN (...), batch",
        "n": len(target_ids),
        "per_row_ms": batch_stats["total_avg_ms"] / len(target_ids),
        **batch_stats,
    }
    single_row["speedup_vs_single"] = 1.0
    batch_row["speedup_vs_single"] = single_row["per_row_ms"] / batch_row["per_row_ms"]
    return [single_row, batch_row]


def _benchmark_in_scaling(table, target_ids: list[int]) -> list[dict]:
    rows = []
    for size in IN_SIZES:
        batch_ids = target_ids[:size]

        def run_batch():
            search_id_in_batch(table, batch_ids)

        total_stats = benchmark_total_ms(run_batch, repeats=PATTERN_REPEATS)
        rows.append(
            {
                "name": "search id IN (...)",
                "n": size,
                "per_row_ms": total_stats["total_avg_ms"] / size,
                **total_stats,
            }
        )
    return rows


class TestQueryLatencyBatchPatterns:
    def test_take_row_ids_single_vs_batch_amortized(self):
        with temp_db() as db:
            table, rows, row_id_map = create_table(
                db,
                "table_query_latency_take_batch_patterns",
                create_indexes=True,
            )
            print_rowid_samples("sample rows with indexes", rows)

            sample_ids = sorted(row_id_map)
            take_rows = _benchmark_take_patterns(table, row_id_map, sample_ids[:BATCH_SIZE])
            print_amortized_table("take_row_ids single vs batch amortized", take_rows)
            assert take_rows

    def test_search_id_single_vs_in_amortized(self):
        with temp_db() as db:
            table, rows, row_id_map = create_table(
                db,
                "table_query_latency_search_batch_patterns",
                create_indexes=True,
            )
            print_rowid_samples("sample rows with indexes", rows)

            sample_ids = sorted(row_id_map)
            search_rows = _benchmark_search_patterns(table, sample_ids[:BATCH_SIZE])
            print_amortized_table("search id single vs IN amortized", search_rows)
            assert search_rows

    def test_search_id_in_scaling(self):
        with temp_db() as db:
            table, rows, _ = create_table(
                db,
                "table_query_latency_in_scaling",
                create_indexes=True,
            )
            print_rowid_samples("sample rows with indexes", rows)

            scaling_ids = benchmark_ids(max(IN_SIZES))
            scaling_rows = _benchmark_in_scaling(table, scaling_ids)
            print_in_scaling_table("search id IN scaling", scaling_rows)
            assert scaling_rows
