#!/usr/bin/env python
"""
更直观的 LanceDB 查询延迟实验脚本。

特点：
1. 不使用 pytest，直接顺序执行实验并打印结果
2. 每个实验自己建表，尽量减少相互影响
3. 输出聚焦在“结论型”对比上，更适合人工阅读

运行示例：
    uv run python test_lance_api/run_query_latency_demo.py
"""

from __future__ import annotations

from test_lance_api.query_latency_utils import (
    BATCH_SIZE,
    IN_SIZES,
    benchmark_ids,
    benchmark_total_ms,
    contiguous_ids,
    create_table,
    fetch_rows_with_row_id,
    print_amortized_table,
    print_in_scaling_table,
    print_locality_table,
    print_rowid_samples,
    search_id_in_batch,
    search_id_single,
    take_id_batch,
    take_id_single,
    temp_db,
)


def experiment_take_and_search_batch_patterns():
    print("\n" + "=" * 80)
    print("Experiment 1: Single vs Batch")
    print("=" * 80)

    with temp_db() as db:
        table, rows, row_id_map = create_table(
            db,
            "demo_query_latency_batch_patterns",
            create_indexes=True,
        )
        print_rowid_samples("sample rows with indexes", rows)

        sample_ids = sorted(row_id_map)[:BATCH_SIZE]
        sample_row_ids = [row_id_map[row_id] for row_id in sample_ids]

        def run_take_single_loop():
            for row_id, target_id in zip(sample_row_ids, sample_ids):
                take_id_single(table, row_id, target_id)

        def run_take_batch():
            take_id_batch(table, sample_row_ids, sample_ids)

        def run_search_single_loop():
            for target_id in sample_ids:
                search_id_single(table, target_id)

        def run_search_in_batch():
            search_id_in_batch(table, sample_ids)

        take_single = benchmark_total_ms(run_take_single_loop)
        take_batch = benchmark_total_ms(run_take_batch)
        search_single = benchmark_total_ms(run_search_single_loop)
        search_batch = benchmark_total_ms(run_search_in_batch)

        print_amortized_table(
            "take_row_ids single vs batch amortized",
            [
                {
                    "name": "take_row_ids single x N",
                    "n": len(sample_ids),
                    "per_row_ms": take_single["total_avg_ms"] / len(sample_ids),
                    "speedup_vs_single": 1.0,
                    **take_single,
                },
                {
                    "name": "take_row_ids batch",
                    "n": len(sample_ids),
                    "per_row_ms": take_batch["total_avg_ms"] / len(sample_ids),
                    "speedup_vs_single": (take_single["total_avg_ms"] / len(sample_ids))
                    / (take_batch["total_avg_ms"] / len(sample_ids)),
                    **take_batch,
                },
            ],
        )

        print_amortized_table(
            "search id single vs IN amortized",
            [
                {
                    "name": "search id = x, single x N",
                    "n": len(sample_ids),
                    "per_row_ms": search_single["total_avg_ms"] / len(sample_ids),
                    "speedup_vs_single": 1.0,
                    **search_single,
                },
                {
                    "name": "search id IN (...), batch",
                    "n": len(sample_ids),
                    "per_row_ms": search_batch["total_avg_ms"] / len(sample_ids),
                    "speedup_vs_single": (search_single["total_avg_ms"] / len(sample_ids))
                    / (search_batch["total_avg_ms"] / len(sample_ids)),
                    **search_batch,
                },
            ],
        )


def experiment_in_scaling():
    print("\n" + "=" * 80)
    print("Experiment 2: IN Scaling")
    print("=" * 80)

    with temp_db() as db:
        table, rows, _ = create_table(
            db,
            "demo_query_latency_in_scaling",
            create_indexes=True,
        )
        print_rowid_samples("sample rows with indexes", rows)

        target_ids = benchmark_ids(max(IN_SIZES))
        scaling_rows = []
        for size in IN_SIZES:
            batch_ids = target_ids[:size]

            def run_batch():
                search_id_in_batch(table, batch_ids)

            result = benchmark_total_ms(run_batch)
            scaling_rows.append(
                {
                    "name": "search id IN (...)",
                    "n": size,
                    "per_row_ms": result["total_avg_ms"] / size,
                    **result,
                }
            )

        print_in_scaling_table("search id IN scaling", scaling_rows)


def experiment_locality():
    print("\n" + "=" * 80)
    print("Experiment 3: Scattered vs Contiguous")
    print("=" * 80)

    with temp_db() as db:
        table, rows, _ = create_table(
            db,
            "demo_query_latency_locality",
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

        scattered_row_ids = [row_id_map[row_id] for row_id in scattered_ids]
        contiguous_row_ids = [row_id_map[row_id] for row_id in contiguous_batch_ids]

        def run_scattered_take():
            take_id_batch(table, scattered_row_ids, scattered_ids)

        def run_contiguous_take():
            take_id_batch(table, contiguous_row_ids, contiguous_batch_ids)

        def run_scattered_search():
            search_id_in_batch(table, scattered_ids)

        def run_contiguous_search():
            search_id_in_batch(table, contiguous_batch_ids)

        scattered_take = benchmark_total_ms(run_scattered_take)
        contiguous_take = benchmark_total_ms(run_contiguous_take)
        scattered_search = benchmark_total_ms(run_scattered_search)
        contiguous_search = benchmark_total_ms(run_contiguous_search)

        print_locality_table(
            "scattered vs contiguous locality",
            [
                {
                    "label": "scattered",
                    "n": len(scattered_ids),
                    "take_total_avg_ms": scattered_take["total_avg_ms"],
                    "take_per_row_ms": scattered_take["total_avg_ms"] / len(scattered_ids),
                    "search_total_avg_ms": scattered_search["total_avg_ms"],
                    "search_per_row_ms": scattered_search["total_avg_ms"] / len(scattered_ids),
                },
                {
                    "label": "contiguous",
                    "n": len(contiguous_batch_ids),
                    "take_total_avg_ms": contiguous_take["total_avg_ms"],
                    "take_per_row_ms": contiguous_take["total_avg_ms"] / len(contiguous_batch_ids),
                    "search_total_avg_ms": contiguous_search["total_avg_ms"],
                    "search_per_row_ms": contiguous_search["total_avg_ms"] / len(contiguous_batch_ids),
                },
            ],
        )


def main():
    experiment_take_and_search_batch_patterns()
    experiment_in_scaling()
    experiment_locality()


if __name__ == "__main__":
    main()
