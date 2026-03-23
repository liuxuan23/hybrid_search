import time


def get_adj_entry(adj_index_tbl, node_id: str):
    """获取某个节点在 `adj_index` 中的索引项。"""
    start = time.time()
    df = adj_index_tbl.search().where(f"node_id = '{node_id}'").to_pandas()
    rows = df.to_dict("records") if not df.empty else []
    return {
        "rows": rows,
        "count": len(rows),
        "time_ms": (time.time() - start) * 1000,
    }


def query_out_neighbors_index(
    adj_index_tbl,
    node_id: str,
    materialize: bool = False,
):
    """基于邻接索引查询出邻居。"""
    start = time.time()
    entry_result = get_adj_entry(adj_index_tbl, node_id)
    if entry_result["count"] == 0:
        return _build_result([], start, materialize)

    entry = entry_result["rows"][0]
    neighbor_row_ids = _normalize_row_id_list(entry.get("out_neighbor_row_ids"))
    if not materialize:
        rows = [{"row_id": row_id} for row_id in neighbor_row_ids]
        return _build_result(rows, start, materialize)

    rows = _materialize_adj_rows(adj_index_tbl, neighbor_row_ids)
    return _build_result(rows, start, materialize)


def query_in_neighbors_index(
    adj_index_tbl,
    node_id: str,
    materialize: bool = False,
):
    """基于邻接索引查询入邻居。"""
    start = time.time()
    entry_result = get_adj_entry(adj_index_tbl, node_id)
    if entry_result["count"] == 0:
        return _build_result([], start, materialize)

    entry = entry_result["rows"][0]
    neighbor_row_ids = _normalize_row_id_list(entry.get("in_neighbor_row_ids"))
    if not materialize:
        rows = [{"row_id": row_id} for row_id in neighbor_row_ids]
        return _build_result(rows, start, materialize)

    rows = _materialize_adj_rows(adj_index_tbl, neighbor_row_ids)
    return _build_result(rows, start, materialize)


def query_neighbors_index(
    adj_index_tbl,
    node_id: str,
    materialize: bool = False,
):
    """基于邻接索引查询双向邻居。"""
    start = time.time()
    out_result = query_out_neighbors_index(
        adj_index_tbl,
        node_id,
        materialize=materialize,
    )
    in_result = query_in_neighbors_index(
        adj_index_tbl,
        node_id,
        materialize=materialize,
    )

    rows = []
    for row in out_result["rows"]:
        row = dict(row)
        row["direction"] = "out"
        rows.append(row)
    for row in in_result["rows"]:
        row = dict(row)
        row["direction"] = "in"
        rows.append(row)

    return {
        "rows": rows,
        "count": len(rows),
        "time_ms": (time.time() - start) * 1000,
        "mode": "materialized" if materialize else "index-only",
    }


def _materialize_adj_rows(adj_index_tbl, neighbor_row_ids):
    """根据 row_id 列表回表获取邻接索引行。

    当前版本已从“整表读取”切换为“按需回表”：
    1. 邻接表中已直接存储物理 row_id
    2. 因此可基于 Lance 的 `_rowid` 元数据直接读取目标记录

    这样做虽然还没有进一步做到批量 page 级优化，但已经避免了：
    - 每次查询都把整张 `adj_index` 拉到 pandas
    - materialized 单跳查询随着表规模线性放大
    """
    if not neighbor_row_ids:
        return []

    physical_row_ids = [int(row_id) for row_id in neighbor_row_ids]

    if not physical_row_ids:
        return []

    try:
        df = _take_rows_with_row_id(adj_index_tbl, physical_row_ids)
        if "_rowid" not in df.columns:
            df = df.copy()
            df["_rowid"] = sorted(set(int(row_id) for row_id in physical_row_ids))
    except Exception:
        row_id_expr = _build_row_id_filter(physical_row_ids)
        df = _fetch_rows_with_row_id(adj_index_tbl, row_id_expr)
    if df.empty:
        return []

    row_by_physical_row_id = {
        int(row["_rowid"]): row for row in df.to_dict("records") if row.get("_rowid") is not None
    }

    rows = []
    for row_id in neighbor_row_ids:
        row = row_by_physical_row_id.get(int(row_id))
        if row is None:
            continue

        materialized_row = dict(row)
        materialized_row["row_id"] = int(row_id)
        materialized_row["physical_row_id"] = int(row_id)
        rows.append(materialized_row)
    return rows


def _fetch_rows_with_row_id(adj_index_tbl, row_id_expr: str):
    """基于 `_rowid` 过滤条件按需读取目标邻接行。"""
    lance_ds = adj_index_tbl.to_lance()
    arrow_tbl = lance_ds.to_table(with_row_id=True, filter=row_id_expr)
    return arrow_tbl.to_pandas()


def _take_rows_with_row_id(adj_index_tbl, row_ids):
    """优先使用 Lance `take` 直接按物理 row_id 抽取目标行。"""
    lance_ds = adj_index_tbl.to_lance()
    # 将待读取的row_id列表做规范化、去重、排序，确保 `take` 的输入符合预期。
    arrow_tbl = lance_ds.take(sorted(set(int(row_id) for row_id in row_ids)))
    return arrow_tbl.to_pandas()


def _build_row_id_filter(row_ids):
    """把物理 row_id 列表转换成 Lance 可执行的过滤表达式。"""
    unique_row_ids = sorted(set(int(row_id) for row_id in row_ids))
    if len(unique_row_ids) == 1:
        return f"_rowid = {unique_row_ids[0]}"
    joined = ", ".join(str(row_id) for row_id in unique_row_ids)
    return f"_rowid IN ({joined})"


def _normalize_row_id_list(value):
    """将 Lance / pandas 返回的数组类型统一转成 Python list。

    这里专门处理一个阶段二常见细节：
    `out_neighbor_row_ids` / `in_neighbor_row_ids` 从表里读出来后，
    可能是 `list`，也可能是 numpy array / arrow array 风格对象。

    如果直接写成 `value or []`，在 numpy array 场景下会触发：
    "The truth value of an array with more than one element is ambiguous"。
    因此这里显式做归一化，避免布尔判断踩坑。
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if hasattr(value, "tolist"):
        return value.tolist()
    return list(value)


def _build_result(rows, start_time: float, materialize: bool):
    """统一构造索引查询结果。"""
    return {
        "rows": rows,
        "count": len(rows),
        "time_ms": (time.time() - start_time) * 1000,
        "mode": "materialized" if materialize else "index-only",
    }
