#!/usr/bin/env python
"""
LanceDB row_id 用法测试：with_row_id 与 take_row_ids。

- 测试一：空表 3 列，一次性写入若干行，通过 search().with_row_id 得到 row_id，
  用 take_row_ids 访问该行的 3 列数据。
- 测试二：先写入普通列 key，再分两次按 id 逐列补写 col_a / col_b，
  最后通过 search().with_row_id 得到 row_id，验证同一 row_id 能访问到补齐后的整行数据。

运行: pytest test_lance_api/test_row_id.py -v   (在 hybrid_search 目录下)
要看测试里的 print 输出，请加 -s:  pytest test_lance_api/test_row_id.py -v -s
"""

import shutil
import tempfile

import pyarrow as pa
import pytest

import lancedb


# 使用临时目录作为 DB 路径
@pytest.fixture(scope="module")
def db_dir():
    d = tempfile.mkdtemp(prefix="test_lance_row_id_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def db(db_dir):
    return lancedb.connect(db_dir)


@pytest.fixture
def stable_db(db_dir):
    return lancedb.connect(
        db_dir,
        storage_options={"new_table_enable_stable_row_ids": "true"},
    )


# 3 列 schema：key、col_a、col_b
def _schema():
    return pa.schema([
        ("key", pa.utf8()),
        ("col_a", pa.int64()),
        ("col_b", pa.utf8()),
    ])


def _empty_table(db, name: str):
    empty = pa.Table.from_pylist([], schema=_schema())
    return db.create_table(name, empty, mode="overwrite")


def _schema_with_id():
    return pa.schema([
        ("id", pa.int64()),
        ("key", pa.utf8()),
        ("col_a", pa.int64()),
        ("col_b", pa.utf8()),
    ])


def _empty_table_with_id(db, name: str):
    empty = pa.Table.from_pylist([], schema=_schema_with_id())
    return db.create_table(name, empty, mode="overwrite")


def _print_rows(title: str, rows: list[dict], include_rowid: bool = False):
    print(f"\n[{title}]")
    if include_rowid:
        print("row_id | id | key     | col_a | col_b")
        print("------ | -- | ------- | ----- | ----------")
        for row in rows:
            print(
                f"{str(row.get('_rowid', '-')):>6} | "
                f"{str(row.get('id', '-')):>2} | "
                f"{str(row.get('key', '-')):<7} | "
                f"{str(row.get('col_a', '-')):>5} | "
                f"{str(row.get('col_b', '-'))}"
            )
        return

    print("id | key     | col_a | col_b")
    print("-- | ------- | ----- | ----------")
    for row in rows:
        print(
            f"{str(row.get('id', '-')):>2} | "
            f"{str(row.get('key', '-')):<7} | "
            f"{str(row.get('col_a', '-')):>5} | "
            f"{str(row.get('col_b', '-'))}"
        )


# ---------------------------------------------------------------------------
# 测试一：空表 3 列，一次性写入若干行 → search().with_row_id → take_row_ids 取 3 列
# ---------------------------------------------------------------------------
class TestRowIdOneShotWrite:
    """一次性写入后，通过 search 的 with_row_id 得到 row_id，take_row_ids 访问 3 列。"""

    def test_one_shot_write_then_take_row_ids(self, db):
        table_name = "table_one_shot"
        tbl = _empty_table(db, table_name)

        # 生成若干行，一次性写入（3 列：key, col_a, col_b）
        n = 7
        data = [
            {"key": f"k{i:02d}", "col_a": 100 + i, "col_b": f"row_{i}"}
            for i in range(n)
        ]
        tbl.add(data)

        # 通过 search 的 with_row_id 得到 row_id
        search_result = (
            tbl.search()
            .with_row_id(True)
            .limit(n)
            .to_list()
        )
        assert len(search_result) == n
        row_ids = [int(r["_rowid"]) for r in search_result]
        _print_rows("one-shot search result", search_result, include_rowid=True)

        # 用 take_row_ids 取这些行，并验证能访问 3 列数据
        rows = tbl.take_row_ids(row_ids).to_list()
        assert len(rows) == n
        _print_rows("one-shot take_row_ids result", sorted(rows, key=lambda r: r["col_a"]))

        # 每行应包含 key, col_a, col_b
        for row in rows:
            assert "key" in row
            assert "col_a" in row
            assert "col_b" in row
            assert row["col_a"] in range(100, 100 + n)
            assert row["col_b"].startswith("row_")
            assert row["key"].startswith("k")

        # 可选：按 col_a 与 search 结果对应（顺序可能不同，用 col_a 对齐）
        by_col_a = {r["col_a"]: r for r in rows}
        for r in search_result:
            key = r.get("key")
            a = r.get("col_a")
            b = r.get("col_b")
            if key is not None and a is not None and b is not None:
                assert by_col_a[a]["key"] == key
                assert by_col_a[a]["col_a"] == a
                assert by_col_a[a]["col_b"] == b


# ---------------------------------------------------------------------------
# 测试二：先写 key，再按 id 分两次逐列补写 col_a / col_b → search().with_row_id → 同一 row_id 取整行
# ---------------------------------------------------------------------------
class TestRowIdThreeWrites:
    """先写入普通列 key，再按稳定 id 分两次补写其它列，最后通过 row_id 访问补齐后的整行。

    这里刻意区分两类标识：
    - id: 写入阶段使用的稳定业务键，用于 merge_insert 把不同批次写入合并到同一行
    - _rowid: 读取阶段由 search().with_row_id(True) 返回，用于 take_row_ids 回读当前版本中的那一行
    """

    def test_three_separate_writes_then_same_row_id_access(self, db):
        table_name = "table_three_writes"
        tbl = _empty_table_with_id(db, table_name)

        n = 5
        ids = list(range(n))
        keys = [f"k{i:02d}" for i in range(n)]
        col_a_list = [200 + i for i in range(n)]
        col_b_list = [f"batch2_{i}" for i in range(n)]

        # 第一次只写 key，先建立 n 行；其余列保持为空
        batch1 = [
            {"id": row_id, "key": key, "col_a": None, "col_b": None}
            for row_id, key in zip(ids, keys)
        ]
        tbl.add(batch1)

        # 第二次只补写 col_a，按稳定 id 合并到已有行
        batch2 = [{"id": row_id, "col_a": a} for row_id, a in zip(ids, col_a_list)]
        tbl.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute(batch2)

        # 第三次只补写 col_b，仍按稳定 id 合并到同一批行
        batch3 = [{"id": row_id, "col_b": b} for row_id, b in zip(ids, col_b_list)]
        tbl.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute(batch3)

        # 通过 search 取得当前版本中的 row_id
        search_result = (
            tbl.search()
            .with_row_id(True)
            .limit(n)
            .to_list()
        )
        assert len(search_result) == n
        row_ids = [int(r["_rowid"]) for r in search_result]
        _print_rows("three-step search result", sorted(search_result, key=lambda r: r["id"]), include_rowid=True)

        # 用 take_row_ids 验证：同一个 row_id 能取到逐列补齐后的完整行
        rows = tbl.take_row_ids(row_ids).to_list()
        assert len(rows) == len(row_ids)
        _print_rows("three-step take_row_ids result", sorted(rows, key=lambda r: r["id"]))

        by_id = {row["id"]: row for row in rows}
        assert set(by_id) == set(ids)

        for row_id, key, col_a, col_b in zip(ids, keys, col_a_list, col_b_list):
            row = by_id[row_id]
            assert row["id"] == row_id
            assert "key" in row
            assert "col_a" in row
            assert "col_b" in row
            assert row["key"] == key
            assert row["col_a"] == col_a
            assert row["col_b"] == col_b


# ---------------------------------------------------------------------------
# 测试三：启用 stable row ids 后，merge_insert 更新前后的 row_id 保持稳定
# ---------------------------------------------------------------------------
class TestStableRowIds:
    """验证启用 stable row ids 后，同一逻辑行在 merge_insert 前后保持相同 row_id。"""

    def test_stable_row_ids_across_merge_insert(self, stable_db):
        table_name = "table_stable_row_ids"
        tbl = _empty_table_with_id(stable_db, table_name)

        n = 5
        ids = list(range(n))
        keys = [f"s{i:02d}" for i in range(n)]
        col_a_list = [300 + i for i in range(n)]
        col_b_list = [f"stable_{i}" for i in range(n)]

        # 第一次写入基础行，只包含 key
        tbl.add(
            [
                {"id": row_id, "key": key, "col_a": None, "col_b": None}
                for row_id, key in zip(ids, keys)
            ]
        )

        before_rows = sorted(
            tbl.search().with_row_id(True).limit(n).to_list(),
            key=lambda row: row["id"],
        )
        before_row_ids = {row["id"]: int(row["_rowid"]) for row in before_rows}
        _print_rows("stable-rowids before merge", before_rows, include_rowid=True)

        # 第二次只补写 col_a
        tbl.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute(
            [{"id": row_id, "col_a": value} for row_id, value in zip(ids, col_a_list)]
        )

        after_col_a_rows = sorted(
            tbl.search().with_row_id(True).limit(n).to_list(),
            key=lambda row: row["id"],
        )
        after_col_a_row_ids = {row["id"]: int(row["_rowid"]) for row in after_col_a_rows}
        _print_rows("stable-rowids after merge col_a", after_col_a_rows, include_rowid=True)

        # 第三次只补写 col_b
        tbl.merge_insert("id").when_matched_update_all().when_not_matched_insert_all().execute(
            [{"id": row_id, "col_b": value} for row_id, value in zip(ids, col_b_list)]
        )

        final_rows = sorted(
            tbl.search().with_row_id(True).limit(n).to_list(),
            key=lambda row: row["id"],
        )
        final_row_ids = {row["id"]: int(row["_rowid"]) for row in final_rows}
        _print_rows("stable-rowids after merge col_b", final_rows, include_rowid=True)

        print("\n[stable-rowid comparison]")
        print("id | before | after_col_a | after_col_b")
        print("-- | ------ | ----------- | -----------")
        for row_id in ids:
            print(
                f"{row_id:>2} | "
                f"{before_row_ids[row_id]:>6} | "
                f"{after_col_a_row_ids[row_id]:>11} | "
                f"{final_row_ids[row_id]:>11}"
            )

        assert before_row_ids == after_col_a_row_ids
        assert before_row_ids == final_row_ids

        taken_with_original_row_ids = tbl.take_row_ids(
            [before_row_ids[row_id] for row_id in ids]
        ).to_list()
        taken_with_original_row_ids = sorted(
            taken_with_original_row_ids,
            key=lambda row: row["id"],
        )
        _print_rows("stable-rowids take_row_ids with original row_ids", taken_with_original_row_ids)

        for row_id, key, col_a, col_b in zip(ids, keys, col_a_list, col_b_list):
            row = taken_with_original_row_ids[row_id]
            assert row["id"] == row_id
            assert row["key"] == key
            assert row["col_a"] == col_a
            assert row["col_b"] == col_b
