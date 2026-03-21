#!/usr/bin/env python
"""
LanceDB 冷启动 IO locality 测试：每次测量前清理 Linux 页缓存。

说明：
1. 该测试依赖 Linux 的 `/proc/sys/vm/drop_caches`
2. 为了真正清理 OS page cache，通常需要 root 权限
3. 如果当前进程没有足够权限，测试会直接 skip

运行:
    sudo uv run pytest test_lance_api/test_query_io_locality_cold.py -v -s
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from test_lance_api.query_latency_utils import (
    BATCH_SIZE,
    benchmark_ids,
    contiguous_ids,
    create_table,
    fetch_rows_with_row_id,
    print_rowid_samples,
    search_id_in_batch,
    take_id_batch,
    temp_db,
)


PROC_IO_PATH = Path("/proc/self/io")
DROP_CACHES_PATH = Path("/proc/sys/vm/drop_caches")
MEASURE_REPEATS = 3


def _ensure_drop_caches_available():
    if not PROC_IO_PATH.exists():
        pytest.skip("/proc/self/io is not available on this system")
    if not DROP_CACHES_PATH.exists():
        pytest.skip("/proc/sys/vm/drop_caches is not available on this system")
    if os.geteuid() != 0:
        pytest.skip("cold-cache IO test requires root to write /proc/sys/vm/drop_caches")


def _drop_linux_page_cache():
    os.sync()
    DROP_CACHES_PATH.write_text("3\n", encoding="utf-8")


def _read_proc_io() -> dict[str, int]:
    values: dict[str, int] = {}
    for line in PROC_IO_PATH.read_text(encoding="utf-8").splitlines():
        key, value = line.split(":")
        values[key.strip()] = int(value.strip())
    return values


def _io_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    keys = set(before) | set(after)
    return {key: after.get(key, 0) - before.get(key, 0) for key in keys}


def _benchmark_cold_io(func):
    elapsed_ms = []
    deltas = []
    for _ in range(MEASURE_REPEATS):
        _drop_linux_page_cache()
        before = _read_proc_io()
        start = time.perf_counter()
        func()
        elapsed_ms.append((time.perf_counter() - start) * 1000)
        after = _read_proc_io()
        deltas.append(_io_delta(before, after))

    return {
        "avg_ms": sum(elapsed_ms) / len(elapsed_ms),
        "avg_read_bytes": sum(delta.get("read_bytes", 0) for delta in deltas) / len(deltas),
        "avg_rchar": sum(delta.get("rchar", 0) for delta in deltas) / len(deltas),
        "avg_syscr": sum(delta.get("syscr", 0) for delta in deltas) / len(deltas),
    }


def _print_io_table(title: str, rows: list[dict]):
    print(f"\n[{title}]")
    print("pattern    | mode   | n    | avg_ms | avg_read_bytes | avg_rchar | avg_syscr")
    print("---------- | ------ | ---- | ------ | -------------- | --------- | ---------")
    for row in rows:
        print(
            f"{row['pattern']:<10} | "
            f"{row['mode']:<6} | "
            f"{row['n']:>4} | "
            f"{row['avg_ms']:>6.3f} | "
            f"{int(row['avg_read_bytes']):>14} | "
            f"{int(row['avg_rchar']):>9} | "
            f"{row['avg_syscr']:>9.1f}"
        )


class TestQueryIoLocalityCold:
    def test_contiguous_vs_scattered_io_cold_start(self):
        _ensure_drop_caches_available()

        with temp_db() as db:
            table, rows, _ = create_table(
                db,
                "table_query_io_locality_cold",
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

            rows_out = [
                {
                    "pattern": "scattered",
                    "mode": "take",
                    "n": len(scattered_ids),
                    **_benchmark_cold_io(lambda: take_id_batch(table, scattered_row_ids, scattered_ids)),
                },
                {
                    "pattern": "contiguous",
                    "mode": "take",
                    "n": len(contiguous_batch_ids),
                    **_benchmark_cold_io(lambda: take_id_batch(table, contiguous_row_ids, contiguous_batch_ids)),
                },
                {
                    "pattern": "scattered",
                    "mode": "search",
                    "n": len(scattered_ids),
                    **_benchmark_cold_io(lambda: search_id_in_batch(table, scattered_ids)),
                },
                {
                    "pattern": "contiguous",
                    "mode": "search",
                    "n": len(contiguous_batch_ids),
                    **_benchmark_cold_io(lambda: search_id_in_batch(table, contiguous_batch_ids)),
                },
            ]

            _print_io_table("cold-start contiguous vs scattered IO delta", rows_out)

            assert all(row["avg_ms"] > 0 for row in rows_out)
