from typing import Dict, Tuple

import pandas as pd


def build_adjacency_index_dataframe(
    nodes_df: pd.DataFrame,
    edges_df: pd.DataFrame,
    cluster_assignments: Dict[str, str] | None = None,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """构建 `adj_index` DataFrame，并返回 node_id 到 row_id 的映射。

    当前实现采用“每节点一行”的简化邻接索引模型：

    1. `nodes_df` 中每个节点对应 `adj_index` 中的一行。
    2. 这一行中保存该节点的：
       - 出邻居在 `adj_index` 中的 row id 列表
       - 入邻居在 `adj_index` 中的 row id 列表
    3. `row_id` 这里不是先去依赖 Lance 表中已经存在的物理行号，
       而是先基于当前 `nodes_df` 的顺序构建一个稳定的“预期行号映射”。

    这样做的目的，是先把阶段二最核心的数据结构表达清楚：
    - `node_id -> row_id`
    - `row_id -> neighbor_row_ids`

    后续如果需要和 Lance 中真实落盘后的 row id 做更严格对齐，可以再单独补一层校验。
    """
    # 先显式校验输入列是否齐全。
    # 这样可以尽早发现上游数据构建的问题，避免后面在邻接回填阶段出现隐蔽错误。
    required_node_columns = {"node_id", "node_type", "degree_out", "degree_in", "attrs_json"}
    required_edge_columns = {"src_id", "dst_id"}

    missing_node_columns = required_node_columns - set(nodes_df.columns)
    if missing_node_columns:
        raise ValueError(f"nodes_df 缺少列: {sorted(missing_node_columns)}")

    missing_edge_columns = required_edge_columns - set(edges_df.columns)
    if missing_edge_columns:
        raise ValueError(f"edges_df 缺少列: {sorted(missing_edge_columns)}")

    # cluster 信息在阶段二主要用于后续 clustered 写入和局部性实验。
    # 如果上游没有提供 cluster 分配，这里统一给默认值，保证构建流程不被阻断。
    cluster_assignments = cluster_assignments or {}

    # 这里显式重置 index，确保后续基于“当前行顺序”生成的 row_id 是连续且稳定的。
    # 这一步非常关键，因为后面的邻接列表会直接引用这个 row_id。
    ordered_nodes_df = nodes_df.reset_index(drop=True).copy()
    node_ids = ordered_nodes_df["node_id"].tolist()

    # `node_to_row_id` 是整个阶段二方案的核心映射之一：
    # - 查询入口通常从 `node_id` 开始
    # - 但真正的邻接跳转希望尽量走 `row_id -> row_id`
    # 因此这里先基于节点顺序构造一个 node_id 到 row_id 的字典。
    node_to_row_id = {node_id: idx for idx, node_id in enumerate(node_ids)}

    # 为每个节点预先创建出邻居/入邻居列表容器。
    # 这里选择用 node_id 作为暂存键，是因为边表里天然使用的是 src_id / dst_id。
    out_neighbors = {node_id: [] for node_id in node_ids}
    in_neighbors = {node_id: [] for node_id in node_ids}

    # 遍历边表，把边关系转成“邻居 row_id”关系。
    # 例如：A -> B
    # - 在 A 的 out_neighbor_row_ids 中追加 B 的 row_id
    # - 在 B 的 in_neighbor_row_ids 中追加 A 的 row_id
    for row in edges_df.itertuples(index=False):
        src_id = row.src_id
        dst_id = row.dst_id

        # 正常情况下，nodes_df 应该已经覆盖所有边端点。
        # 这里保留显式保护，是为了兼容未来可能出现的脏数据或不完整输入。
        if src_id not in node_to_row_id or dst_id not in node_to_row_id:
            continue

        out_neighbors[src_id].append(node_to_row_id[dst_id])
        in_neighbors[dst_id].append(node_to_row_id[src_id])

    # 将前面准备好的摘要信息和邻接列表重新组织成 `adj_index` 的行结构。
    # 这里每一行都代表一个节点的索引入口，后续查询应优先命中这里。
    adj_rows = []
    for row in ordered_nodes_df.itertuples(index=False):
        node_id = row.node_id
        adj_rows.append(
            {
                "node_id": node_id,
                "node_type": row.node_type,
                "cluster_id": cluster_assignments.get(node_id, "cluster::default"),
                "degree_out": int(row.degree_out),
                "degree_in": int(row.degree_in),
                "out_neighbor_row_ids": out_neighbors[node_id],
                "in_neighbor_row_ids": in_neighbors[node_id],
                "attrs_json": row.attrs_json,
            }
        )

    # 最终返回：
    # 1. `adj_index_df`：可直接写入 LanceDB 的邻接索引表
    # 2. `node_to_row_id`：便于后续构建、调试和查询阶段复用
    adj_index_df = pd.DataFrame(adj_rows)
    return adj_index_df, node_to_row_id
