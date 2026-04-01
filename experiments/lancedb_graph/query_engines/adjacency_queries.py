import time


_GRAPH_OWNER_BY_TABLE_ID = {}


def register_graph_owner(adj_index_tbl, graph_owner):
    _GRAPH_OWNER_BY_TABLE_ID[id(adj_index_tbl)] = graph_owner


def _get_cached_row_by_node_id(adj_index_tbl, node_id: str):
    graph = _GRAPH_OWNER_BY_TABLE_ID.get(id(adj_index_tbl)) or getattr(adj_index_tbl, "_graph_owner", None)
    if graph is None:
        return None
    cache = getattr(graph, "node_id_to_physical_row_id", None)
    if cache is None:
        return None
    physical_row_id = cache.get(node_id)
    if physical_row_id is None:
        return None
    return _get_row_by_physical_row_id(adj_index_tbl, int(physical_row_id))


def get_adj_entry(adj_index_tbl, node_id: str):
    """获取某个节点在 `adj_index` 中的索引项。"""
    start = time.time()
    cached_row = _get_cached_row_by_node_id(adj_index_tbl, node_id)
    if cached_row is not None:
        return {
            "rows": [cached_row],
            "count": 1,
            "time_ms": (time.time() - start) * 1000,
        }

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
    io_before = _read_process_io_bytes()
    entry_result = get_adj_entry(adj_index_tbl, node_id)
    if entry_result["count"] == 0:
        return _build_result([], start, materialize, io_before)

    entry = entry_result["rows"][0]
    neighbor_row_ids = _normalize_row_id_list(entry.get("out_neighbor_row_ids"))
    if not materialize:
        rows = [{"row_id": row_id} for row_id in neighbor_row_ids]
        return _build_result(rows, start, materialize, io_before)

    rows = _materialize_adj_rows(adj_index_tbl, neighbor_row_ids)
    return _build_result(rows, start, materialize, io_before)


def query_in_neighbors_index(
    adj_index_tbl,
    node_id: str,
    materialize: bool = False,
):
    """基于邻接索引查询入邻居。"""
    start = time.time()
    io_before = _read_process_io_bytes()
    entry_result = get_adj_entry(adj_index_tbl, node_id)
    if entry_result["count"] == 0:
        return _build_result([], start, materialize, io_before)

    entry = entry_result["rows"][0]
    neighbor_row_ids = _normalize_row_id_list(entry.get("in_neighbor_row_ids"))
    if not materialize:
        rows = [{"row_id": row_id} for row_id in neighbor_row_ids]
        return _build_result(rows, start, materialize, io_before)

    rows = _materialize_adj_rows(adj_index_tbl, neighbor_row_ids)
    return _build_result(rows, start, materialize, io_before)


def query_neighbors_index(
    adj_index_tbl,
    node_id: str,
    materialize: bool = False,
):
    """基于邻接索引查询双向邻居。"""
    start = time.time()
    io_before = _read_process_io_bytes()
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
        "io_stats": _build_io_stats(io_before),
    }


def query_batch_neighbors_index(
    adj_index_tbl,
    node_ids,
    direction: str = "out",
    materialize: bool = False,
):
    """基于邻接索引批量查询多个节点的邻居。"""
    if direction not in {"out", "in", "both"}:
        raise ValueError(f"Unsupported direction: {direction}")

    start = time.perf_counter()
    io_before = _read_process_io_bytes()
    normalized_node_ids = [str(node_id) for node_id in node_ids or []]
    if not normalized_node_ids:
        return {
            "rows": [],
            "count": 0,
            "time_ms": (time.perf_counter() - start) * 1000,
            "mode": "materialized" if materialize else "index-only",
            "io_stats": _build_io_stats(io_before),
        }

    entries = _get_adj_entries_by_node_ids(adj_index_tbl, normalized_node_ids)
    if not entries:
        return {
            "rows": [],
            "count": 0,
            "time_ms": (time.perf_counter() - start) * 1000,
            "mode": "materialized" if materialize else "index-only",
            "io_stats": _build_io_stats(io_before),
        }

    neighbor_row_ids_by_seed = {}
    ordered_unique_neighbor_row_ids = []
    seen_neighbor_row_ids = set()

    for seed in normalized_node_ids:
        entry = entries.get(seed)
        if entry is None:
            neighbor_row_ids_by_seed[seed] = []
            continue

        neighbor_row_ids = _get_directional_neighbor_row_ids(entry, direction)
        deduped_neighbor_row_ids = []
        local_seen = set()
        for neighbor_row_id in neighbor_row_ids:
            neighbor_row_id = int(neighbor_row_id)
            if neighbor_row_id in local_seen:
                continue
            local_seen.add(neighbor_row_id)
            deduped_neighbor_row_ids.append(neighbor_row_id)
            if neighbor_row_id not in seen_neighbor_row_ids:
                seen_neighbor_row_ids.add(neighbor_row_id)
                ordered_unique_neighbor_row_ids.append(neighbor_row_id)

        neighbor_row_ids_by_seed[seed] = deduped_neighbor_row_ids

    materialized_rows_by_row_id = {}
    if materialize and ordered_unique_neighbor_row_ids:
        for row in _materialize_adj_rows(adj_index_tbl, ordered_unique_neighbor_row_ids):
            physical_row_id = row.get("physical_row_id", row.get("row_id", row.get("_rowid")))
            if physical_row_id is None:
                continue
            materialized_rows_by_row_id[int(physical_row_id)] = row

    rows = []
    total_count = 0
    for seed in normalized_node_ids:
        neighbor_row_ids = neighbor_row_ids_by_seed.get(seed, [])
        total_count += len(neighbor_row_ids)

        if materialize:
            for neighbor_row_id in neighbor_row_ids:
                row = materialized_rows_by_row_id.get(int(neighbor_row_id))
                if row is None:
                    continue
                row_with_seed = dict(row)
                row_with_seed["seed"] = seed
                if direction == "both":
                    row_with_seed["direction"] = _infer_neighbor_direction(entries.get(seed), int(neighbor_row_id))
                rows.append(row_with_seed)
        else:
            for neighbor_row_id in neighbor_row_ids:
                row = {"seed": seed, "row_id": int(neighbor_row_id)}
                if direction == "both":
                    row["direction"] = _infer_neighbor_direction(entries.get(seed), int(neighbor_row_id))
                rows.append(row)

    return {
        "rows": rows,
        "count": total_count,
        "time_ms": (time.perf_counter() - start) * 1000,
        "mode": "materialized" if materialize else "index-only",
        "io_stats": _build_io_stats(io_before),
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


def _get_adj_entries_by_node_ids(adj_index_tbl, node_ids):
    """按 node_id 批量读取邻接索引项。"""
    entries = {}
    missing_node_ids = []
    cached_row_ids_by_node_id = {}

    for node_id in node_ids:
        graph = _GRAPH_OWNER_BY_TABLE_ID.get(id(adj_index_tbl)) or getattr(adj_index_tbl, "_graph_owner", None)
        cache = getattr(graph, "node_id_to_physical_row_id", None) if graph is not None else None
        if cache is not None:
            physical_row_id = cache.get(node_id)
            if physical_row_id is not None:
                cached_row_ids_by_node_id[node_id] = int(physical_row_id)
                continue

        cached_row = _get_cached_row_by_node_id(adj_index_tbl, node_id)
        if cached_row is not None:
            entries[node_id] = cached_row
            continue

        missing_node_ids.append(node_id)

    if cached_row_ids_by_node_id:
        cached_rows_df = _take_rows_with_row_id(adj_index_tbl, list(cached_row_ids_by_node_id.values()))
        row_by_physical_row_id = {}
        if not cached_rows_df.empty:
            cached_rows = cached_rows_df.to_dict("records")
            if "_rowid" not in cached_rows_df.columns:
                sorted_row_ids = sorted(set(cached_row_ids_by_node_id.values()))
                for idx, row in enumerate(cached_rows):
                    if idx < len(sorted_row_ids):
                        row["_rowid"] = sorted_row_ids[idx]
            row_by_physical_row_id = {
                int(row["_rowid"]): row for row in cached_rows if row.get("_rowid") is not None
            }

        for node_id, physical_row_id in cached_row_ids_by_node_id.items():
            row = row_by_physical_row_id.get(int(physical_row_id))
            if row is not None:
                materialized_row = dict(row)
                materialized_row["physical_row_id"] = int(physical_row_id)
                entries[node_id] = materialized_row
            else:
                missing_node_ids.append(node_id)

    if not missing_node_ids:
        return entries

    condition = _build_string_in_filter("node_id", missing_node_ids)
    df = adj_index_tbl.search().where(condition).to_pandas()
    if df.empty:
        return entries

    for row in df.to_dict("records"):
        row_node_id = row.get("node_id")
        if row_node_id is not None:
            entries[str(row_node_id)] = row
    return entries


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


def _get_row_by_physical_row_id(adj_index_tbl, row_id: int):
    df = _take_rows_with_row_id(adj_index_tbl, [int(row_id)])
    if df.empty:
        return None
    row = df.to_dict("records")[0]
    row["physical_row_id"] = int(row_id)
    return row


def _build_row_id_filter(row_ids):
    """把物理 row_id 列表转换成 Lance 可执行的过滤表达式。"""
    unique_row_ids = sorted(set(int(row_id) for row_id in row_ids))
    if len(unique_row_ids) == 1:
        return f"_rowid = {unique_row_ids[0]}"
    joined = ", ".join(str(row_id) for row_id in unique_row_ids)
    return f"_rowid IN ({joined})"


def _build_string_in_filter(column: str, values):
    unique_values = []
    seen = set()
    for value in values:
        normalized = str(value)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_values.append(normalized)

    if not unique_values:
        return "1 = 0"

    escaped_values = ["'" + value.replace("'", "''") + "'" for value in unique_values]
    if len(escaped_values) == 1:
        return f"{column} = {escaped_values[0]}"
    return f"{column} IN ({', '.join(escaped_values)})"


def _get_directional_neighbor_row_ids(entry, direction: str):
    if entry is None:
        return []

    out_row_ids = _normalize_row_id_list(entry.get("out_neighbor_row_ids"))
    in_row_ids = _normalize_row_id_list(entry.get("in_neighbor_row_ids"))

    if direction == "out":
        return out_row_ids
    if direction == "in":
        return in_row_ids

    merged = []
    seen = set()
    for row_id in out_row_ids + in_row_ids:
        row_id = int(row_id)
        if row_id in seen:
            continue
        seen.add(row_id)
        merged.append(row_id)
    return merged


def _infer_neighbor_direction(entry, neighbor_row_id: int):
    if entry is None:
        return "unknown"

    out_row_ids = {int(row_id) for row_id in _normalize_row_id_list(entry.get("out_neighbor_row_ids"))}
    in_row_ids = {int(row_id) for row_id in _normalize_row_id_list(entry.get("in_neighbor_row_ids"))}

    is_out = neighbor_row_id in out_row_ids
    is_in = neighbor_row_id in in_row_ids

    if is_out and is_in:
        return "both"
    if is_out:
        return "out"
    if is_in:
        return "in"
    return "unknown"


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


def _build_result(rows, start_time: float, materialize: bool, io_before=None):
    """统一构造索引查询结果。"""
    return {
        "rows": rows,
        "count": len(rows),
        "time_ms": (time.time() - start_time) * 1000,
        "mode": "materialized" if materialize else "index-only",
        "io_stats": _build_io_stats(io_before),
    }


def _read_process_io_bytes():
    """读取当前进程的 Linux `/proc/self/io` 统计。"""
    try:
        with open("/proc/self/io", "r", encoding="utf-8") as f:
            values = {}
            for line in f:
                if ":" not in line:
                    continue
                key, value = line.split(":", 1)
                values[key.strip()] = int(value.strip())
        return {
            "read_bytes": values.get("read_bytes", 0),
            "write_bytes": values.get("write_bytes", 0),
        }
    except Exception:
        return {
            "read_bytes": 0,
            "write_bytes": 0,
        }


def _build_io_stats(io_before):
    """构造本次查询的 IO 增量统计。"""
    io_after = _read_process_io_bytes()
    io_before = io_before or {"read_bytes": 0, "write_bytes": 0}
    return {
        "read_bytes": max(0, io_after["read_bytes"] - io_before.get("read_bytes", 0)),
        "write_bytes": max(0, io_after["write_bytes"] - io_before.get("write_bytes", 0)),
    }
