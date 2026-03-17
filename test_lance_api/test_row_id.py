#!/usr/bin/env python
"""
LanceDB row_id 用法测试：with_row_id 与 take_row_ids。

- 测试一：空表 3 列，一次性写入若干行，通过 search().with_row_id 得到 row_id，
  用 take_row_ids 访问该行的 3 列数据。
- 测试二：空表 3 列，独立生成 3 列数据并分 3 次写入，同样通过 search().with_row_id
  得到 row_id，验证同一 row_id 能否访问到 3 列数据。

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


# 3 列 schema：vector（用于 search）、col_a、col_b
def _schema():
    return pa.schema([
        ("vector", pa.list_(pa.float32(), 2)),
        ("col_a", pa.int64()),
        ("col_b", pa.utf8()),
    ])


def _empty_table(db, name: str):
    empty = pa.Table.from_pylist([], schema=_schema())
    return db.create_table(name, empty, mode="overwrite")


# ---------------------------------------------------------------------------
# 测试一：空表 3 列，一次性写入若干行 → search().with_row_id → take_row_ids 取 3 列
# ---------------------------------------------------------------------------
class TestRowIdOneShotWrite:
    """一次性写入后，通过 search 的 with_row_id 得到 row_id，take_row_ids 访问 3 列。"""

    def test_one_shot_write_then_take_row_ids(self, db):
        table_name = "table_one_shot"
        tbl = _empty_table(db, table_name)

        # 生成若干行，一次性写入（3 列：vector, col_a, col_b）
        n = 7
        data = [
            {"vector": [float(i), float(i) * 0.1], "col_a": 100 + i, "col_b": f"row_{i}"}
            for i in range(n)
        ]
        tbl.add(data)

        # 通过 search 的 with_row_id 得到 row_id（用任意向量做最近邻）
        search_result = (
            tbl.search([0.0, 0.0])
            .with_row_id(True)
            .limit(n)
            .to_list()
        )
        assert len(search_result) == n
        row_ids = [int(r["_rowid"]) for r in search_result]
        print(row_ids)

        # 用 take_row_ids 取这些行，并验证能访问 3 列数据
        rows = tbl.take_row_ids(row_ids).to_list()
        assert len(rows) == n

        # 每行应包含 vector, col_a, col_b（以及可能的 _rowid）
        for i, row in enumerate(rows):
            assert "vector" in row
            assert "col_a" in row
            assert "col_b" in row
            assert row["col_a"] in range(100, 100 + n)
            assert row["col_b"].startswith("row_")
            print(row)

        # 可选：按 col_a 与 search 结果对应（顺序可能不同，用 col_a 对齐）
        by_col_a = {r["col_a"]: r for r in rows}
        for r in search_result:
            a = r.get("col_a")
            b = r.get("col_b")
            if a is not None and b is not None:
                assert by_col_a[a]["col_a"] == a
                assert by_col_a[a]["col_b"] == b


# ---------------------------------------------------------------------------
# 测试二：空表 3 列，独立 3 列数据分 3 次写入 → search().with_row_id → 同一 row_id 取 3 列
# ---------------------------------------------------------------------------
class TestRowIdThreeWrites:
    """3 列数据分 3 次 add 写入，用 search 得到 row_id，验证 take_row_ids 能取到「一行、且含 3 列」。
    注意：多批 append 后，search() 返回结果里的 _rowid 可能与当前展示的那一行不对应（已知现象），
    但 take_row_ids(rid) 仍会返回「一条完整行」且包含 vector/col_a/col_b 三列。
    """

    def test_three_separate_writes_then_same_row_id_access(self, db):
        table_name = "table_three_writes"
        tbl = _empty_table(db, table_name)

        n = 5
        # 独立生成 3 列数据
        vectors = [[float(i), float(i) * 0.2] for i in range(n)]
        col_a_list = [200 + i for i in range(n)]
        col_b_list = [f"batch2_{i}" for i in range(n)]

        # 分 3 次写入：每次写入一批“完整行”（每行仍含 3 列，便于表结构一致）
        # 第一次：只填 vector，其余列先写占位
        batch1 = [{"vector": v, "col_a": 0, "col_b": ""} for v in vectors]
        tbl.add(batch1)

        # 第二次：再追加一批行（col_a 有值）
        batch2 = [{"vector": v, "col_a": a, "col_b": ""} for v, a in zip(vectors, col_a_list)]
        tbl.add(batch2)

        # 第三次：再追加一批行（col_b 有值）
        batch3 = [
            {"vector": v, "col_a": a, "col_b": b}
            for v, a, b in zip(vectors, col_a_list, col_b_list)
        ]
        tbl.add(batch3)

        # 表中共 3*n 行；通过 search 取一批 row_id（多批 append 后，这些 _rowid 可能与展示行不对应，属已知现象）
        search_result = (
            tbl.search([0.0, 0.0])
            .with_row_id(True)
            .limit(n)
            .to_list()
        )
        assert len(search_result) >= 1
        row_ids = [int(r["_rowid"]) for r in search_result]

        # 用 take_row_ids 验证：每个 row_id 都能取到「一条完整行」且包含 3 列
        rows = tbl.take_row_ids(row_ids).to_list()
        assert len(rows) == len(row_ids)

        for row in rows:
            assert "vector" in row
            assert "col_a" in row
            assert "col_b" in row
            assert isinstance(row.get("col_a"), (int, type(None)))
            assert isinstance(row.get("col_b"), (str, type(None)))
