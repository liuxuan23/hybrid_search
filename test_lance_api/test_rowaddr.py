#!/usr/bin/env python
"""
Lance _rowaddr 用法测试。

目标：
1. 通过 LanceDataset.to_table(..., with_row_address=True) 获取 `_rowaddr`
2. 同时打印 `_rowid` 与 `_rowaddr`，观察当前版本里的实际值

运行:
    pytest test_lance_api/test_rowaddr.py -v -s
    uv run pytest test_lance_api/test_rowaddr.py -v -s
"""

import shutil
import tempfile

import pyarrow as pa
import pytest
import lancedb

try:
    import lance  # noqa: F401
except ImportError:
    lance = None


@pytest.fixture(scope="module")
def db_dir():
    path = tempfile.mkdtemp(prefix="test_lance_rowaddr_")
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def db(db_dir):
    return lancedb.connect(db_dir)


def _schema():
    return pa.schema([
        ("id", pa.int64()),
        ("key", pa.utf8()),
        ("col_a", pa.int64()),
        ("col_b", pa.utf8()),
    ])


def _empty_table(db, name: str):
    empty = pa.Table.from_pylist([], schema=_schema())
    return db.create_table(name, empty, mode="overwrite")


def _decode_row_meta(value: int | None) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    value = int(value)
    return value >> 32, value & 0xFFFFFFFF


def _print_rows(title: str, rows: list[dict]):
    print(f"\n[{title}]")
    print("id | row_id | rowaddr | frag_id | offset | key      | col_a | col_b")
    print("-- | ------ | ------- | ------- | ------ | -------- | ----- | ----------")
    for row in rows:
        rowaddr = row.get("_rowaddr")
        fragment_id, offset = _decode_row_meta(rowaddr)
        print(
            f"{str(row.get('id', '-')):>2} | "
            f"{str(row.get('_rowid', '-')):>6} | "
            f"{str(row.get('_rowaddr', '-')):>7} | "
            f"{str(fragment_id if fragment_id is not None else '-'):>7} | "
            f"{str(offset if offset is not None else '-'):>6} | "
            f"{str(row.get('key', '-')):<8} | "
            f"{str(row.get('col_a', '-')):>5} | "
            f"{str(row.get('col_b', '-'))}"
        )


def _rows_with_row_metadata(tbl, columns: list[str] | None = None) -> list[dict]:
    lance_ds = tbl.to_lance()
    arrow_tbl = lance_ds.to_table(
        columns=columns,
        with_row_id=True,
        with_row_address=True,
    )
    return sorted(arrow_tbl.to_pylist(), key=lambda row: row["id"])


def _row_meta_summary(rows: list[dict]):
    row_ids = [int(row["_rowid"]) for row in rows]
    row_addrs = [int(row["_rowaddr"]) for row in rows]
    return row_ids, row_addrs


def _print_row_meta_summary(rows: list[dict], title: str = "rowaddr summary"):
    row_ids, row_addrs = _row_meta_summary(rows)
    decoded = [_decode_row_meta(value) for value in row_addrs]
    print(f"\n[{title}]")
    print(f"_rowid values:   {row_ids}")
    print(f"_rowaddr values: {row_addrs}")
    print(f"fragment/offset: {decoded}")


def _fragment_ids_for_table(tbl) -> list[int]:
    return [int(fragment.fragment_id) for fragment in tbl.to_lance().get_fragments()]


class TestRowAddr:
    def test_lance_to_table_with_row_address(self, db):
        if lance is None:
            pytest.skip("The current environment does not have the `lance` package installed")

        table_name = "table_rowaddr_lance"
        tbl = _empty_table(db, table_name)

        n = 5
        data = [
            {"id": i, "key": f"addr{i:02d}", "col_a": 200 + i, "col_b": f"addr_row_{i}"}
            for i in range(n)
        ]
        tbl.add(data)

        rows = _rows_with_row_metadata(tbl, columns=["id", "key", "col_a", "col_b"])
        _print_rows("lance to_table with rowaddr", rows)
        _print_row_meta_summary(rows)

        assert len(rows) == n
        assert all("_rowid" in row for row in rows)
        assert all("_rowaddr" in row for row in rows)
        assert all(row["_rowaddr"] is not None for row in rows)

        for expected_id, row in enumerate(rows):
            assert row["id"] == expected_id
            assert row["key"] == f"addr{expected_id:02d}"
            assert row["col_a"] == 200 + expected_id
            assert row["col_b"] == f"addr_row_{expected_id}"

        row_ids, row_addrs = _row_meta_summary(rows)
        assert row_ids == row_addrs

    def test_rowaddr_across_multiple_appends(self, db):
        if lance is None:
            pytest.skip("The current environment does not have the `lance` package installed")

        table_name = "table_rowaddr_append"
        tbl = _empty_table(db, table_name)

        batch1 = [
            {"id": i, "key": f"batch1_{i}", "col_a": 100 + i, "col_b": f"append_a_{i}"}
            for i in range(3)
        ]
        batch2 = [
            {"id": i, "key": f"batch2_{i}", "col_a": 200 + i, "col_b": f"append_b_{i}"}
            for i in range(3, 6)
        ]
        tbl.add(batch1)
        tbl.add(batch2)

        rows = _rows_with_row_metadata(tbl, columns=["id", "key", "col_a", "col_b"])
        _print_rows("rowaddr after two appends", rows)
        _print_row_meta_summary(rows, title="append summary")

        fragment_ids = _fragment_ids_for_table(tbl)
        print(f"\n[append fragments] fragment ids: {fragment_ids}")

        row_ids, row_addrs = _row_meta_summary(rows)
        assert len(rows) == 6
        assert len(fragment_ids) >= 2
        assert row_ids == row_addrs
        assert row_ids[:3] == [0, 1, 2]
        assert row_ids[3:] == [2**32, 2**32 + 1, 2**32 + 2]

    def test_rowaddr_after_delete_and_optimize(self, db):
        if lance is None:
            pytest.skip("The current environment does not have the `lance` package installed")

        table_name = "table_rowaddr_delete_optimize"
        tbl = _empty_table(db, table_name)

        tbl.add([
            {"id": i, "key": f"batch1_{i}", "col_a": 100 + i, "col_b": f"append_a_{i}"}
            for i in range(3)
        ])
        tbl.add([
            {"id": i, "key": f"batch2_{i}", "col_a": 200 + i, "col_b": f"append_b_{i}"}
            for i in range(3, 6)
        ])

        rows_before_delete = _rows_with_row_metadata(tbl, columns=["id", "key", "col_a", "col_b"])
        _print_rows("before delete", rows_before_delete)
        _print_row_meta_summary(rows_before_delete, title="before delete summary")

        tbl.delete("id in (1, 4)")

        rows_after_delete = _rows_with_row_metadata(tbl, columns=["id", "key", "col_a", "col_b"])
        _print_rows("after delete", rows_after_delete)
        _print_row_meta_summary(rows_after_delete, title="after delete summary")

        deleted_ids = [row["id"] for row in rows_after_delete]
        assert deleted_ids == [0, 2, 3, 5]

        row_ids_after_delete, row_addrs_after_delete = _row_meta_summary(rows_after_delete)
        assert row_ids_after_delete == row_addrs_after_delete
        assert row_ids_after_delete == [0, 2, 2**32, 2**32 + 2]

        tbl.optimize()

        rows_after_optimize = _rows_with_row_metadata(tbl, columns=["id", "key", "col_a", "col_b"])
        _print_rows("after optimize", rows_after_optimize)
        _print_row_meta_summary(rows_after_optimize, title="after optimize summary")

        fragment_ids_after_optimize = _fragment_ids_for_table(tbl)
        print(f"\n[after optimize fragments] fragment ids: {fragment_ids_after_optimize}")

        row_ids_after_optimize, row_addrs_after_optimize = _row_meta_summary(rows_after_optimize)
        assert row_ids_after_optimize == row_addrs_after_optimize
        assert row_ids_after_optimize == [2**33, 2**33 + 1, 2**33 + 2, 2**33 + 3]
        assert fragment_ids_after_optimize == [2]
