#!/usr/bin/env python
"""Shared helpers for independent LanceDB query latency tests."""

from __future__ import annotations

import random
import shutil
import statistics
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

import pyarrow as pa

import lancedb


ROW_COUNT = 1_000_000
RANDOM_SEED = 20260318
SAMPLE_COUNT = 128
PRINT_SAMPLE_LIMIT = 24
MEASURE_REPEATS = 20
WARMUP_COUNT = 5
BATCH_SIZE = 128
FILTER_LIMIT = 512
PATTERN_REPEATS = 8
IN_SIZES = (8, 64, 512, 2048)
WRITE_BATCH_SIZE = 1000


def schema() -> pa.Schema:
    return pa.schema([
        ("id", pa.int64()),
        ("key", pa.utf8()),
        ("group_id", pa.int64()),
        ("score", pa.int64()),
        ("flag", pa.bool_()),
        ("payload", pa.utf8()),
    ])


def make_data() -> list[dict]:
    return [
        {
            "id": i,
            "key": f"key_{i:05d}",
            "group_id": i % 64,
            "score": (i * 17) % 1000,
            "flag": i % 2 == 0,
            "payload": f"payload_{i:05d}",
        }
        for i in range(ROW_COUNT)
    ]


def batch_records(records: list[dict], batch_size: int):
    if batch_size <= 0:
        raise ValueError(f"batch_size must be positive, got {batch_size}")
    for start in range(0, len(records), batch_size):
        yield records[start : start + batch_size]


def sample_ids() -> list[int]:
    rng = random.Random(RANDOM_SEED)
    return sorted(rng.sample(range(ROW_COUNT), SAMPLE_COUNT))


def benchmark_ids(count: int) -> list[int]:
    rng = random.Random(RANDOM_SEED + count)
    return sorted(rng.sample(range(ROW_COUNT), count))


def contiguous_ids(count: int, start: int | None = None) -> list[int]:
    if start is None:
        start = ROW_COUNT // 2
    return list(range(start, start + count))


@contextmanager
def temp_db() -> Iterator[lancedb.db.DBConnection]:
    path = tempfile.mkdtemp(prefix="test_lance_latency_")
    try:
        yield lancedb.connect(path)
    finally:
        shutil.rmtree(path, ignore_errors=True)


def create_table(db, table_name: str, create_indexes: bool):
    table = db.create_table(
        table_name,
        pa.Table.from_pylist([], schema=schema()),
        mode="overwrite",
    )
    data = make_data()
    if WRITE_BATCH_SIZE is None:
        table.add(data)
    else:
        for batch in batch_records(data, WRITE_BATCH_SIZE):
            table.add(batch)

    if create_indexes:
        table.create_scalar_index("id", index_type="BTREE")
        table.create_scalar_index("key", index_type="BTREE")
        table.create_scalar_index("group_id", index_type="BITMAP")
        table.create_scalar_index("flag", index_type="BITMAP")
        table.create_scalar_index("score", index_type="BTREE")

    rows = fetch_rows_with_row_id(table, sample_ids())
    row_id_map = {row["id"]: int(row["_rowid"]) for row in rows}
    return table, rows, row_id_map


def fetch_rows_with_row_id(table, ids: list[int]) -> list[dict]:
    return sorted(
        table.search()
        .where(f"id IN ({', '.join(str(row_id) for row_id in ids)})")
        .with_row_id(True)
        .limit(len(ids))
        .to_list(),
        key=lambda row: row["id"],
    )


def measure_ms(func: Callable[[], None], repeats: int) -> list[float]:
    times_ms = []
    for _ in range(repeats):
        start = time.perf_counter()
        func()
        times_ms.append((time.perf_counter() - start) * 1000)
    return times_ms


def stats(times_ms: list[float]) -> dict[str, float]:
    return {
        "avg": statistics.mean(times_ms),
        "p50": statistics.median(times_ms),
        "min": min(times_ms),
        "max": max(times_ms),
    }


def benchmark_total_ms(func: Callable[[], None], repeats: int = PATTERN_REPEATS) -> dict[str, float]:
    for _ in range(WARMUP_COUNT):
        func()
    times_ms = measure_ms(func, repeats)
    return {
        "total_avg_ms": statistics.mean(times_ms),
        "total_p50_ms": statistics.median(times_ms),
        "total_min_ms": min(times_ms),
        "total_max_ms": max(times_ms),
    }


def arrow_ids(table: pa.Table) -> list[int]:
    return table.column("id").to_pylist()


def print_rowid_samples(title: str, rows: list[dict]):
    print(f"\n[{title}]")
    print("id   | row_id | key       | group_id | score | flag")
    print("---- | ------ | --------- | -------- | ----- | -----")
    display_rows = rows[:PRINT_SAMPLE_LIMIT]
    for row in display_rows:
        print(
            f"{row['id']:>4} | "
            f"{int(row['_rowid']):>6} | "
            f"{row['key']:<9} | "
            f"{row['group_id']:>8} | "
            f"{row['score']:>5} | "
            f"{str(row['flag']):<5}"
        )
    if len(rows) > PRINT_SAMPLE_LIMIT:
        print(f"... ({len(rows) - PRINT_SAMPLE_LIMIT} more rows omitted)")


def print_latency_table(title: str, rows: list[dict]):
    print(f"\n[{title}]")
    print("query                      | avg_ms | p50_ms | min_ms | max_ms")
    print("------------------------- | ------ | ------ | ------ | ------")
    for row in rows:
        print(
            f"{row['name']:<25} | "
            f"{row['avg']:>6.3f} | "
            f"{row['p50']:>6.3f} | "
            f"{row['min']:>6.3f} | "
            f"{row['max']:>6.3f}"
        )


def print_comparison_table(title: str, left_rows: list[dict], right_rows: list[dict], left_label: str, right_label: str):
    left_by_name = {row["name"]: row for row in left_rows}
    right_by_name = {row["name"]: row for row in right_rows}

    print(f"\n[{title}]")
    print(f"query                      | {left_label}_avg_ms | {right_label}_avg_ms | speedup")
    print("------------------------- | --------------- | ----------------- | -------")
    for name in left_by_name:
        left_avg = left_by_name[name]["avg"]
        right_avg = right_by_name[name]["avg"]
        speedup = left_avg / right_avg if right_avg > 0 else float("inf")
        print(
            f"{name:<25} | "
            f"{left_avg:>15.3f} | "
            f"{right_avg:>17.3f} | "
            f"{speedup:>7.2f}x"
        )


def print_amortized_table(title: str, rows: list[dict]):
    print(f"\n[{title}]")
    print("pattern                         | n    | total_avg_ms | per_row_ms | speedup_vs_single")
    print("------------------------------ | ---- | ------------ | ---------- | -----------------")
    for row in rows:
        speedup = row.get("speedup_vs_single")
        speedup_text = f"{speedup:>17.2f}x" if speedup is not None else f"{'-':>17}"
        print(
            f"{row['name']:<30} | "
            f"{row['n']:>4} | "
            f"{row['total_avg_ms']:>12.3f} | "
            f"{row['per_row_ms']:>10.3f} | "
            f"{speedup_text}"
        )


def print_in_scaling_table(title: str, rows: list[dict]):
    print(f"\n[{title}]")
    print("mode             | n    | total_avg_ms | per_row_ms | growth_vs_prev")
    print("---------------- | ---- | ------------ | ---------- | --------------")
    previous_total = None
    for row in rows:
        growth_text = "-"
        if previous_total is not None:
            growth_text = f"{row['total_avg_ms'] / previous_total:.2f}x"
        print(
            f"{row['name']:<16} | "
            f"{row['n']:>4} | "
            f"{row['total_avg_ms']:>12.3f} | "
            f"{row['per_row_ms']:>10.3f} | "
            f"{growth_text:>14}"
        )
        previous_total = row["total_avg_ms"]


def print_locality_table(title: str, rows: list[dict]):
    print(f"\n[{title}]")
    print("pattern    | n    | take_total_ms | take_per_row_ms | search_total_ms | search_per_row_ms")
    print("---------- | ---- | ------------- | --------------- | --------------- | -----------------")
    for row in rows:
        print(
            f"{row['label']:<10} | "
            f"{row['n']:>4} | "
            f"{row['take_total_avg_ms']:>13.3f} | "
            f"{row['take_per_row_ms']:>15.3f} | "
            f"{row['search_total_avg_ms']:>15.3f} | "
            f"{row['search_per_row_ms']:>17.3f}"
        )


def take_id_single(table, row_id: int, expected_id: int):
    result = table.take_row_ids([row_id]).select(["id"]).to_arrow()
    assert result.num_rows == 1
    assert result.column("id")[0].as_py() == expected_id


def take_id_batch(table, row_ids: list[int], expected_ids: list[int]):
    result = table.take_row_ids(row_ids).select(["id"]).to_arrow()
    assert result.num_rows == len(expected_ids)
    assert set(arrow_ids(result)) == set(expected_ids)


def search_id_single(table, target_id: int):
    result = table.search().where(f"id = {target_id}").select(["id"]).limit(1).to_arrow()
    assert result.num_rows == 1
    assert result.column("id")[0].as_py() == target_id


def search_id_in_batch(table, target_ids: list[int]):
    result = (
        table.search()
        .where(f"id IN ({', '.join(str(value) for value in target_ids)})")
        .select(["id"])
        .limit(len(target_ids))
        .to_arrow()
    )
    assert result.num_rows == len(target_ids)
    assert set(arrow_ids(result)) == set(target_ids)
