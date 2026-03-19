#!/usr/bin/env python
"""
LanceDB 查询延迟：无索引 vs 有索引。

运行:
    uv run pytest test_lance_api/test_query_latency_indexes.py -v -s
"""

from test_lance_api.query_latency_utils import (
    FILTER_LIMIT,
    MEASURE_REPEATS,
    WARMUP_COUNT,
    create_table,
    measure_ms,
    print_comparison_table,
    print_latency_table,
    print_rowid_samples,
    stats,
    temp_db,
)


def _benchmark_case(func) -> dict[str, float]:
    for _ in range(WARMUP_COUNT):
        func()
    return stats(measure_ms(func, MEASURE_REPEATS))


def _run_no_index_vs_with_index(case_name: str, no_index_func, with_index_func):
    no_index_stats = _benchmark_case(no_index_func)
    with_index_stats = _benchmark_case(with_index_func)
    rows_left = [{"name": case_name, **no_index_stats}]
    rows_right = [{"name": case_name, **with_index_stats}]
    print_latency_table(f"{case_name} without indexes", rows_left)
    print_latency_table(f"{case_name} with indexes", rows_right)
    print_comparison_table(
        f"{case_name} comparison",
        rows_left,
        rows_right,
        "no_index",
        "with_index",
    )
    return no_index_stats, with_index_stats


def _run_take_single(table, row_id: int, expected_id: int):
    result = table.take_row_ids([row_id]).select(["id"]).to_arrow()
    assert result.num_rows == 1
    assert result.column("id")[0].as_py() == expected_id


def _run_search_id(table, target_id: int):
    result = table.search().where(f"id = {target_id}").select(["id"]).limit(1).to_arrow()
    assert result.num_rows == 1
    assert result.column("id")[0].as_py() == target_id


def _run_search_key(table, target_key: str, expected_id: int):
    result = table.search().where(f"key = '{target_key}'").select(["id"]).limit(1).to_arrow()
    assert result.num_rows == 1
    assert result.column("id")[0].as_py() == expected_id


def _run_search_group(table, group_id: int):
    result = table.search().where(f"group_id = {group_id}").select(["id"]).limit(FILTER_LIMIT).to_arrow()
    assert result.num_rows >= 1


def _run_search_flag(table):
    result = table.search().where("flag = true").select(["id"]).limit(FILTER_LIMIT).to_arrow()
    assert result.num_rows >= 1


def _run_search_score(table, low: int, high: int):
    result = table.search().where(f"score >= {low} AND score < {high}").select(["id"]).limit(FILTER_LIMIT).to_arrow()
    assert result.num_rows >= 1


def _run_take_batch(table, row_ids: list[int], expected_count: int):
    result = table.take_row_ids(row_ids).select(["id"]).to_arrow()
    assert result.num_rows == expected_count


def _setup_tables():
    db_cm = temp_db()
    db = db_cm.__enter__()
    no_index_table, no_index_rows, no_index_row_ids = create_table(
        db,
        "table_query_latency_no_index",
        create_indexes=False,
    )
    with_index_table, with_index_rows, with_index_row_ids = create_table(
        db,
        "table_query_latency_with_index",
        create_indexes=True,
    )
    print_rowid_samples("sample rows without indexes", no_index_rows)
    print_rowid_samples("sample rows with indexes", with_index_rows)
    return db_cm, no_index_table, no_index_row_ids, with_index_table, with_index_row_ids


class TestQueryLatencyIndexes:
    def test_take_row_ids_single_no_index_vs_with_index(self):
        db_cm, no_index_table, no_index_row_ids, with_index_table, with_index_row_ids = _setup_tables()
        try:
            sample_ids = sorted(no_index_row_ids)
            single_id = sample_ids[0]
            no_index_stats, with_index_stats = _run_no_index_vs_with_index(
                "take_row_ids(single)",
                lambda: _run_take_single(no_index_table, no_index_row_ids[single_id], single_id),
                lambda: _run_take_single(with_index_table, with_index_row_ids[single_id], single_id),
            )
            assert no_index_stats["avg"] > 0
            assert with_index_stats["avg"] > 0
        finally:
            db_cm.__exit__(None, None, None)

    def test_search_where_id_no_index_vs_with_index(self):
        db_cm, no_index_table, no_index_row_ids, with_index_table, _ = _setup_tables()
        try:
            target_id = sorted(no_index_row_ids)[0]
            no_index_stats, with_index_stats = _run_no_index_vs_with_index(
                "search where id",
                lambda: _run_search_id(no_index_table, target_id),
                lambda: _run_search_id(with_index_table, target_id),
            )
            assert no_index_stats["avg"] > 0
            assert with_index_stats["avg"] > 0
        finally:
            db_cm.__exit__(None, None, None)

    def test_search_where_key_no_index_vs_with_index(self):
        db_cm, no_index_table, no_index_row_ids, with_index_table, _ = _setup_tables()
        try:
            target_id = sorted(no_index_row_ids)[0]
            target_key = f"key_{target_id:05d}"
            no_index_stats, with_index_stats = _run_no_index_vs_with_index(
                "search where key",
                lambda: _run_search_key(no_index_table, target_key, target_id),
                lambda: _run_search_key(with_index_table, target_key, target_id),
            )
            assert no_index_stats["avg"] > 0
            assert with_index_stats["avg"] > 0
        finally:
            db_cm.__exit__(None, None, None)

    def test_search_group_eq_no_index_vs_with_index(self):
        db_cm, no_index_table, no_index_row_ids, with_index_table, _ = _setup_tables()
        try:
            target_id = sorted(no_index_row_ids)[0]
            group_id = target_id % 64
            no_index_stats, with_index_stats = _run_no_index_vs_with_index(
                "search group eq",
                lambda: _run_search_group(no_index_table, group_id),
                lambda: _run_search_group(with_index_table, group_id),
            )
            assert no_index_stats["avg"] > 0
            assert with_index_stats["avg"] > 0
        finally:
            db_cm.__exit__(None, None, None)

    def test_search_flag_eq_no_index_vs_with_index(self):
        db_cm, no_index_table, _no_index_row_ids, with_index_table, _ = _setup_tables()
        try:
            no_index_stats, with_index_stats = _run_no_index_vs_with_index(
                "search flag eq",
                lambda: _run_search_flag(no_index_table),
                lambda: _run_search_flag(with_index_table),
            )
            assert no_index_stats["avg"] > 0
            assert with_index_stats["avg"] > 0
        finally:
            db_cm.__exit__(None, None, None)

    def test_search_score_range_no_index_vs_with_index(self):
        db_cm, no_index_table, _no_index_row_ids, with_index_table, _ = _setup_tables()
        try:
            low, high = 200, 260
            no_index_stats, with_index_stats = _run_no_index_vs_with_index(
                "search score range",
                lambda: _run_search_score(no_index_table, low, high),
                lambda: _run_search_score(with_index_table, low, high),
            )
            assert no_index_stats["avg"] > 0
            assert with_index_stats["avg"] > 0
        finally:
            db_cm.__exit__(None, None, None)

    def test_take_row_ids_batch_no_index_vs_with_index(self):
        db_cm, no_index_table, no_index_row_ids, with_index_table, with_index_row_ids = _setup_tables()
        try:
            sample_ids = sorted(no_index_row_ids)
            no_index_batch = [no_index_row_ids[row_id] for row_id in sample_ids]
            with_index_batch = [with_index_row_ids[row_id] for row_id in sample_ids]
            no_index_stats, with_index_stats = _run_no_index_vs_with_index(
                "take_row_ids(batch)",
                lambda: _run_take_batch(no_index_table, no_index_batch, len(no_index_batch)),
                lambda: _run_take_batch(with_index_table, with_index_batch, len(with_index_batch)),
            )
            assert no_index_stats["avg"] > 0
            assert with_index_stats["avg"] > 0
        finally:
            db_cm.__exit__(None, None, None)
