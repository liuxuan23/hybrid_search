import time

from experiments.lancedb_graph.query_engines.adjacency_queries import _take_rows_with_row_id


def query_k_hop_index(
    adj_index_tbl,
    node_id: str,
    k: int,
    materialize: bool = False,
    direction: str = "out",
):
    """基于邻接索引执行 k-hop 扩展。

    当前实现采用最直接、最容易校验正确性的 BFS 方案：
    1. 先把整张 `adj_index` 读入内存，构建按 `node_id` 和逻辑 `row_id` 的访问视图
    2. 再按 hop 层次逐层扩展

    这样做的主要目的，是先保证阶段二多跳查询的语义正确，便于后续继续替换成：
    - 按真实 row_id 的局部读取
    - 更细粒度的 I/O locality 优化
    - 更低开销的批量 materialize

    参数：
    - direction:
        - `out`: 只沿出邻居扩展
        - `in`: 只沿入邻居扩展
        - `both`: 合并出入邻居扩展
    """
    if not isinstance(k, int) or k < 1:
        raise ValueError(f"k 必须是大于等于 1 的整数，当前为: {k}")

    if direction not in {"out", "in", "both"}:
        raise ValueError(f"不支持的 direction: {direction}")

    start = time.perf_counter()
    start_row = _get_row_by_node_id(adj_index_tbl, node_id)
    if start_row is None:
        return _build_k_hop_result([], start, materialize, k, direction)

    visited = {node_id}
    frontier_node_ids = [node_id]
    discovered_rows = []
    discovered_node_ids = set()
    row_cache_by_node_id = {node_id: start_row}
    row_cache_by_physical_row_id = {}

    start_physical_row_id = start_row.get("physical_row_id", start_row.get("_rowid"))
    if start_physical_row_id is not None:
        row_cache_by_physical_row_id[int(start_physical_row_id)] = start_row

    for _depth in range(k):
        if not frontier_node_ids:
            break

        frontier_rows = []
        for current_node_id in frontier_node_ids:
            current_row = row_cache_by_node_id.get(current_node_id)
            if current_row is None:
                current_row = _get_row_by_node_id(adj_index_tbl, current_node_id)
                if current_row is None:
                    continue
                row_cache_by_node_id[current_node_id] = current_row
                current_physical_row_id = current_row.get("physical_row_id", current_row.get("_rowid"))
                if current_physical_row_id is not None:
                    row_cache_by_physical_row_id[int(current_physical_row_id)] = current_row
            frontier_rows.append(current_row)

        aggregated_neighbor_row_ids = []
        seen_neighbor_row_ids = set()
        for current_row in frontier_rows:
            for neighbor_row_id in _get_neighbor_row_ids(current_row, direction):
                neighbor_row_id = int(neighbor_row_id)
                if neighbor_row_id in seen_neighbor_row_ids:
                    continue
                seen_neighbor_row_ids.add(neighbor_row_id)
                aggregated_neighbor_row_ids.append(neighbor_row_id)

        missing_row_ids = [
            neighbor_row_id
            for neighbor_row_id in aggregated_neighbor_row_ids
            if neighbor_row_id not in row_cache_by_physical_row_id
        ]
        if missing_row_ids:
            fetched_rows = _get_rows_by_physical_row_ids(adj_index_tbl, missing_row_ids)
            for row in fetched_rows:
                physical_row_id = row.get("physical_row_id", row.get("_rowid"))
                if physical_row_id is not None:
                    row_cache_by_physical_row_id[int(physical_row_id)] = row
                row_cache_by_node_id[row["node_id"]] = row

        next_frontier_node_ids = []
        for neighbor_row_id in aggregated_neighbor_row_ids:
            neighbor_row = row_cache_by_physical_row_id.get(int(neighbor_row_id))
            if neighbor_row is None:
                continue

            neighbor_node_id = neighbor_row["node_id"]
            if neighbor_node_id in visited:
                continue

            visited.add(neighbor_node_id)
            next_frontier_node_ids.append(neighbor_node_id)

            if neighbor_node_id not in discovered_node_ids:
                discovered_node_ids.add(neighbor_node_id)
                if materialize:
                    materialized_row = dict(neighbor_row)
                    materialized_row["row_id"] = int(neighbor_row_id)
                    materialized_row["physical_row_id"] = int(neighbor_row_id)
                    discovered_rows.append(materialized_row)
                else:
                    discovered_rows.append({"row_id": int(neighbor_row_id)})

        frontier_node_ids = next_frontier_node_ids

    return _build_k_hop_result(discovered_rows, start, materialize, k, direction)


def _get_row_by_node_id(adj_index_tbl, node_id: str):
    """按 node_id 读取单个邻接索引行。"""
    df = adj_index_tbl.search().where(f"node_id = '{node_id}'").to_pandas()
    if df.empty:
        return None
    return df.to_dict("records")[0]


def _get_rows_by_physical_row_ids(adj_index_tbl, physical_row_ids):
    """按物理 row_id 批量回表读取邻接记录。"""
    if not physical_row_ids:
        return []

    try:
        df = _take_rows_with_row_id(adj_index_tbl, physical_row_ids)
        if "_rowid" not in df.columns:
            df = df.copy()
            df["_rowid"] = sorted(set(int(row_id) for row_id in physical_row_ids))
        return df.to_dict("records")
    except Exception:
        row_id_expr = _build_row_id_filter(physical_row_ids)
        lance_ds = adj_index_tbl.to_lance()
        arrow_tbl = lance_ds.to_table(with_row_id=True, filter=row_id_expr)
        return arrow_tbl.to_pylist()


def _get_neighbor_row_ids(row, direction: str):
    """按方向提取当前节点的邻居物理 row_id 列表。"""
    out_row_ids = _normalize_row_id_list(row.get("out_neighbor_row_ids"))
    in_row_ids = _normalize_row_id_list(row.get("in_neighbor_row_ids"))

    if direction == "out":
        return out_row_ids
    if direction == "in":
        return in_row_ids

    merged = []
    seen = set()
    for row_id in out_row_ids + in_row_ids:
        if row_id in seen:
            continue
        seen.add(row_id)
        merged.append(row_id)
    return merged


def _normalize_row_id_list(value):
    """将 pandas / numpy / arrow 返回的列表值统一成 Python list[int]。"""
    if value is None:
        return []

    if isinstance(value, list):
        return [int(item) for item in value]

    if hasattr(value, "tolist"):
        converted = value.tolist()
        if converted is None:
            return []
        return [int(item) for item in converted]

    try:
        return [int(item) for item in value]
    except TypeError:
        return []


def _build_k_hop_result(rows, start_time, materialize: bool, k: int, direction: str):
    """统一封装 k-hop 查询返回格式。"""
    return {
        "rows": rows,
        "count": len(rows),
        "time_ms": (time.perf_counter() - start_time) * 1000,
        "mode": "materialized" if materialize else "index-only",
        "k": k,
        "direction": direction,
    }


def _build_row_id_filter(row_ids):
    """构造 `_rowid` 过滤表达式。"""
    unique_row_ids = sorted(set(int(row_id) for row_id in row_ids))
    if len(unique_row_ids) == 1:
        return f"_rowid = {unique_row_ids[0]}"
    joined = ", ".join(str(row_id) for row_id in unique_row_ids)
    return f"_rowid IN ({joined})"
