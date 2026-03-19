#!/usr/bin/env python
"""
LanceDB IO locality 测试：连续样本 vs 非连续样本的磁盘 IO 差异。

说明：
1. 这里读取 Linux 的 `/proc/self/io`，统计当前 pytest 进程在查询前后的 IO 计数差值
2. 这些计数会受到 OS 页缓存影响，因此更适合做“相对对比”，不宜解读成绝对磁盘读量
3. 该脚本独立建表、独立运行，尽量减少与其他 benchmark 的缓存串扰

运行:
    uv run pytest test_lance_api/test_query_io_locality.py -v -s
"""

from __future__ import annotations

import time
from pathlib import Path

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
WARMUP_COUNT = 2
MEASURE_REPEATS = 5


def _read_proc_io() -> dict[str, int]:
    values: dict[str, int] = {}
    for line in PROC_IO_PATH.read_text(encoding="utf-8").splitlines():
        key, value = line.split(":")
        values[key.strip()] = int(value.strip())
    return values


def _io_delta(before: dict[str, int], after: dict[str, int]) -> dict[str, int]:
    keys = set(before) | set(after)
    return {key: after.get(key, 0) - before.get(key, 0) for key in keys}


def _benchmark_io(func):
    for _ in range(WARMUP_COUNT):
        func()

    elapsed_ms = []
    deltas = []
    for _ in range(MEASURE_REPEATS):
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


class TestQueryIoLocality:
    def test_contiguous_vs_scattered_io(self):
        if not PROC_IO_PATH.exists():
            raise RuntimeError("/proc/self/io is not available on this system")

        with temp_db() as db:
            table, rows, _ = create_table(
                db,
                "table_query_io_locality",
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
                    **_benchmark_io(lambda: take_id_batch(table, scattered_row_ids, scattered_ids)),
                },
                {
                    "pattern": "contiguous",
                    "mode": "take",
                    "n": len(contiguous_batch_ids),
                    **_benchmark_io(lambda: take_id_batch(table, contiguous_row_ids, contiguous_batch_ids)),
                },
                {
                    "pattern": "scattered",
                    "mode": "search",
                    "n": len(scattered_ids),
                    **_benchmark_io(lambda: search_id_in_batch(table, scattered_ids)),
                },
                {
                    "pattern": "contiguous",
                    "mode": "search",
                    "n": len(contiguous_batch_ids),
                    **_benchmark_io(lambda: search_id_in_batch(table, contiguous_batch_ids)),
                },
            ]

            _print_io_table("contiguous vs scattered IO delta", rows_out)

            assert all(row["avg_ms"] > 0 for row in rows_out)
