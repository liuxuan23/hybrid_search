#!/usr/bin/env python
"""
LanceDB locality 测试：分散样本 vs 连续样本。

运行:
    uv run pytest test_lance_api/test_query_latency_locality.py -v -s
"""

from test_lance_api.query_latency_utils import (
    BATCH_SIZE,
    benchmark_ids,
    benchmark_total_ms,
    contiguous_ids,
    create_table,
    fetch_rows_with_row_id,
    print_locality_table,
    print_rowid_samples,
    search_id_in_batch,
    take_id_batch,
    temp_db,
)


def _benchmark_take_locality(table, row_id_map: dict[int, int], target_ids: list[int], label: str) -> dict[str, float | str | int]:
    target_row_ids = [row_id_map[target_id] for target_id in target_ids]

    def run_take_batch():
        take_id_batch(table, target_row_ids, target_ids)

    take_stats = benchmark_total_ms(run_take_batch)
    return {
        "label": label,
        "n": len(target_ids),
        "take_total_avg_ms": take_stats["total_avg_ms"],
        "take_per_row_ms": take_stats["total_avg_ms"] / len(target_ids),
    }


def _benchmark_search_locality(table, target_ids: list[int], label: str) -> dict[str, float | str | int]:
    def run_search_batch():
        search_id_in_batch(table, target_ids)

    search_stats = benchmark_total_ms(run_search_batch)
    return {
        "label": label,
        "n": len(target_ids),
        "search_total_avg_ms": search_stats["total_avg_ms"],
        "search_per_row_ms": search_stats["total_avg_ms"] / len(target_ids),
    }


class TestQueryLatencyLocality:
    def test_take_scattered_vs_contiguous_locality(self):
        with temp_db() as db:
            table, rows, _ = create_table(
                db,
                "table_query_latency_take_locality",
                create_indexes=True,
            )
            print_rowid_samples("sample rows with indexes", rows)

            scattered_ids = benchmark_ids(BATCH_SIZE)
            contiguous_batch_ids = contiguous_ids(BATCH_SIZE, start=500_000)
            lookup_rows = fetch_rows_with_row_id(
                table,
                sorted(set(scattered_ids + contiguous_batch_ids)),
            )
            row_id_map = {row["id"]: int(row["_rowid"]) for row in lookup_rows}

            scattered = _benchmark_take_locality(table, row_id_map, scattered_ids, "scattered")
            contiguous = _benchmark_take_locality(table, row_id_map, contiguous_batch_ids, "contiguous")
            locality_rows = [
                {
                    "label": "scattered",
                    "n": scattered["n"],
                    "take_total_avg_ms": scattered["take_total_avg_ms"],
                    "take_per_row_ms": scattered["take_per_row_ms"],
                    "search_total_avg_ms": 0.0,
                    "search_per_row_ms": 0.0,
                },
                {
                    "label": "contiguous",
                    "n": contiguous["n"],
                    "take_total_avg_ms": contiguous["take_total_avg_ms"],
                    "take_per_row_ms": contiguous["take_per_row_ms"],
                    "search_total_avg_ms": 0.0,
                    "search_per_row_ms": 0.0,
                },
            ]
            print_locality_table("take scattered vs contiguous locality", locality_rows)
            assert scattered["take_total_avg_ms"] > 0
            assert contiguous["take_total_avg_ms"] > 0

    def test_search_scattered_vs_contiguous_locality(self):
        with temp_db() as db:
            table, rows, _ = create_table(
                db,
                "table_query_latency_search_locality",
                create_indexes=True,
            )
            print_rowid_samples("sample rows with indexes", rows)

            scattered_ids = benchmark_ids(BATCH_SIZE)
            contiguous_batch_ids = contiguous_ids(BATCH_SIZE, start=500_000)
            lookup_rows = fetch_rows_with_row_id(
                table,
                sorted(set(scattered_ids + contiguous_batch_ids)),
            )
            row_id_map = {row["id"]: int(row["_rowid"]) for row in lookup_rows}

            scattered = _benchmark_search_locality(table, scattered_ids, "scattered")
            contiguous = _benchmark_search_locality(table, contiguous_batch_ids, "contiguous")
            locality_rows = [
                {
                    "label": "scattered",
                    "n": scattered["n"],
                    "take_total_avg_ms": 0.0,
                    "take_per_row_ms": 0.0,
                    "search_total_avg_ms": scattered["search_total_avg_ms"],
                    "search_per_row_ms": scattered["search_per_row_ms"],
                },
                {
                    "label": "contiguous",
                    "n": contiguous["n"],
                    "take_total_avg_ms": 0.0,
                    "take_per_row_ms": 0.0,
                    "search_total_avg_ms": contiguous["search_total_avg_ms"],
                    "search_per_row_ms": contiguous["search_per_row_ms"],
                },
            ]
            print_locality_table("search scattered vs contiguous locality", locality_rows)
            assert scattered["search_total_avg_ms"] > 0
            assert contiguous["search_total_avg_ms"] > 0
