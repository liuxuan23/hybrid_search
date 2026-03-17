import time
from typing import Optional


def _safe_where(table, condition: str):
    return table.search().where(condition).to_pandas()


def query_node_by_id(nodes_tbl, node_id: str):
    start = time.time()
    df = _safe_where(nodes_tbl, f"node_id = '{node_id}'")
    return {
        "rows": df.to_dict("records") if not df.empty else [],
        "count": len(df),
        "time_ms": (time.time() - start) * 1000,
    }


def query_out_neighbors(edges_tbl, node_id: str, edge_type: Optional[str] = None):
    start = time.time()
    condition = f"src_id = '{node_id}'"
    if edge_type:
        condition += f" AND edge_type = '{edge_type}'"
    df = _safe_where(edges_tbl, condition)
    return {
        "rows": df.to_dict("records") if not df.empty else [],
        "count": len(df),
        "time_ms": (time.time() - start) * 1000,
    }


def query_in_neighbors(edges_tbl, node_id: str, edge_type: Optional[str] = None):
    start = time.time()
    condition = f"dst_id = '{node_id}'"
    if edge_type:
        condition += f" AND edge_type = '{edge_type}'"
    df = _safe_where(edges_tbl, condition)
    return {
        "rows": df.to_dict("records") if not df.empty else [],
        "count": len(df),
        "time_ms": (time.time() - start) * 1000,
    }


def query_neighbors(edges_tbl, node_id: str, edge_type: Optional[str] = None):
    start = time.time()
    out_result = query_out_neighbors(edges_tbl, node_id, edge_type=edge_type)
    in_result = query_in_neighbors(edges_tbl, node_id, edge_type=edge_type)

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
    }


def query_k_hop(edges_tbl, node_id: str, k: int):
    start = time.time()
    visited = {node_id}
    frontier = {node_id}
    layers = []

    for _ in range(k):
        next_frontier = set()
        layer_rows = []
        for current in frontier:
            result = query_out_neighbors(edges_tbl, current)
            for row in result["rows"]:
                target = row["dst_id"]
                if target in visited:
                    continue
                visited.add(target)
                next_frontier.add(target)
                layer_rows.append(row)
        layers.append(layer_rows)
        frontier = next_frontier
        if not frontier:
            break

    total_rows = sum(len(layer) for layer in layers)
    return {
        "rows": layers,
        "count": total_rows,
        "time_ms": (time.time() - start) * 1000,
    }
