from collections import Counter


def compute_cluster_locality_metrics(rows):
    """计算 cluster 相关局部性指标。

    当前阶段先聚焦几类最容易解释、也最适合 clustered / unclustered 对比的指标：
    1. `physical_row_span`: 命中行的物理跨度（max - min）
    2. `physical_row_gap_avg`: 相邻命中行排序后的平均间隔
    3. `unique_cluster_count`: 命中结果覆盖了多少个 cluster
    4. `top_cluster_ratio`: 命中最多的 cluster 占比
    5. `cluster_switches`: 按物理行顺序观察时，cluster 发生切换的次数

    这些指标并不依赖 Lance 更底层的 page / fragment 信息，
    因此适合作为当前阶段的 locality 近似观测量。
    """
    if not rows:
        return {
            "row_count": 0,
            "physical_row_span": 0,
            "physical_row_gap_avg": 0.0,
            "unique_cluster_count": 0,
            "top_cluster_ratio": 0.0,
            "cluster_switches": 0,
        }

    physical_row_ids = sorted(
        int(row["physical_row_id"])
        for row in rows
        if row.get("physical_row_id") is not None
    )
    cluster_ids = [row.get("cluster_id") for row in rows if row.get("cluster_id") is not None]

    physical_row_span = 0
    physical_row_gap_avg = 0.0
    if physical_row_ids:
        physical_row_span = physical_row_ids[-1] - physical_row_ids[0]
        if len(physical_row_ids) >= 2:
            gaps = [
                physical_row_ids[idx] - physical_row_ids[idx - 1]
                for idx in range(1, len(physical_row_ids))
            ]
            physical_row_gap_avg = sum(gaps) / len(gaps)

    unique_cluster_count = len(set(cluster_ids))
    top_cluster_ratio = 0.0
    cluster_switches = 0

    if cluster_ids:
        counter = Counter(cluster_ids)
        top_cluster_ratio = max(counter.values()) / len(cluster_ids)

        ordered_rows = sorted(
            [row for row in rows if row.get("physical_row_id") is not None],
            key=lambda row: int(row["physical_row_id"]),
        )
        ordered_clusters = [row.get("cluster_id") for row in ordered_rows if row.get("cluster_id") is not None]
        for idx in range(1, len(ordered_clusters)):
            if ordered_clusters[idx] != ordered_clusters[idx - 1]:
                cluster_switches += 1

    return {
        "row_count": len(rows),
        "physical_row_span": physical_row_span,
        "physical_row_gap_avg": physical_row_gap_avg,
        "unique_cluster_count": unique_cluster_count,
        "top_cluster_ratio": top_cluster_ratio,
        "cluster_switches": cluster_switches,
    }
