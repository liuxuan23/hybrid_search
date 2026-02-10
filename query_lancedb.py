import os
import time
import lancedb
import pandas as pd
from typing import Optional, Dict, List

"""
查询 LanceDB 中的三元组数据，统计关系数量并测量查询时间。
"""

# LanceDB 数据库路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LANCEDB_PATH = os.path.join(BASE_DIR, "storage", "lance")
TABLE_NAME_FULL = "triples"
TABLE_NAME_SAMPLE = "triples_sample_10k"


def connect_db(table_name: str = TABLE_NAME_FULL):
    """连接到 LanceDB 数据库"""
    if not os.path.exists(LANCEDB_PATH):
        raise FileNotFoundError(f"数据库不存在: {LANCEDB_PATH}")
    
    db = lancedb.connect(LANCEDB_PATH)
    
    if table_name not in db.table_names():
        raise ValueError(f"表不存在: {table_name}。可用表: {db.table_names()}")
    
    return db[table_name]


def query_relation_count(
    entity_id: str,
    relation: Optional[str] = None,
    direction: str = "out",
    table_name: str = TABLE_NAME_FULL
) -> Dict:
    """
    查询某个对象满足某个关系的数量。
    
    Args:
        entity_id: 实体 ID（如 "space:xxx" 或 "model:xxx"）
        relation: 关系类型（如 "space_use_model"），None 表示所有关系
        direction: "out" 表示出边（head），"in" 表示入边（tail），"both" 表示双向
        table_name: 表名
    
    Returns:
        包含统计信息和时间的字典
    """
    start_time = time.time()
    tbl = connect_db(table_name)
    
    results = []
    
    # 读取数据到 pandas（LanceDB 支持直接转换为 pandas）
    # 对于大数据集，可以分批读取，这里先读取全部
    df_all = tbl.to_pandas()
    
    # 构建查询条件
    if direction in ["out", "both"]:
        # 查询出边：head = entity_id
        df_out = df_all[df_all["head"] == entity_id]
        if relation:
            df_out = df_out[df_out["relation"] == relation]
        
        if not df_out.empty:
            results.append({
                "direction": "out",
                "count": len(df_out),
                "relations": df_out["relation"].value_counts().to_dict() if relation is None else {relation: len(df_out)}
            })
    
    if direction in ["in", "both"]:
        # 查询入边：tail = entity_id
        df_in = df_all[df_all["tail"] == entity_id]
        if relation:
            df_in = df_in[df_in["relation"] == relation]
        
        if not df_in.empty:
            results.append({
                "direction": "in",
                "count": len(df_in),
                "relations": df_in["relation"].value_counts().to_dict() if relation is None else {relation: len(df_in)}
            })
    
    elapsed_time = time.time() - start_time
    
    # 汇总结果
    total_count = sum(r["count"] for r in results)
    
    return {
        "entity_id": entity_id,
        "relation": relation or "all",
        "direction": direction,
        "total_count": total_count,
        "details": results,
        "query_time_ms": elapsed_time * 1000,
        "query_time_sec": elapsed_time
    }


def query_relation_statistics(
    relation: str,
    table_name: str = TABLE_NAME_FULL
) -> Dict:
    """
    统计某个关系的总体数量。
    
    Args:
        relation: 关系类型
        table_name: 表名
    
    Returns:
        包含统计信息和时间的字典
    """
    start_time = time.time()
    tbl = connect_db(table_name)
    
    # 读取数据并过滤
    df_all = tbl.to_pandas()
    df = df_all[df_all["relation"] == relation]
    
    elapsed_time = time.time() - start_time
    
    return {
        "relation": relation,
        "total_count": len(df),
        "unique_heads": df["head"].nunique() if not df.empty else 0,
        "unique_tails": df["tail"].nunique() if not df.empty else 0,
        "query_time_ms": elapsed_time * 1000,
        "query_time_sec": elapsed_time
    }


def query_entity_relations(
    entity_id: str,
    table_name: str = TABLE_NAME_FULL,
    limit: int = 100
) -> Dict:
    """
    查询某个实体的所有关系（带详细信息）。
    
    Args:
        entity_id: 实体 ID
        table_name: 表名
        limit: 返回结果数量限制
    
    Returns:
        包含详细信息和时间的字典
    """
    start_time = time.time()
    tbl = connect_db(table_name)
    
    # 读取数据到 pandas
    df_all = tbl.to_pandas()
    
    # 查询出边和入边
    df_out = df_all[df_all["head"] == entity_id].head(limit)
    df_in = df_all[df_all["tail"] == entity_id].head(limit)
    
    elapsed_time = time.time() - start_time
    
    return {
        "entity_id": entity_id,
        "outgoing_relations": df_out.to_dict("records") if not df_out.empty else [],
        "incoming_relations": df_in.to_dict("records") if not df_in.empty else [],
        "outgoing_count": len(df_all[df_all["head"] == entity_id]),
        "incoming_count": len(df_all[df_all["tail"] == entity_id]),
        "query_time_ms": elapsed_time * 1000,
        "query_time_sec": elapsed_time
    }


def batch_query_relation_counts(
    entity_ids: List[str],
    relation: Optional[str] = None,
    direction: str = "out",
    table_name: str = TABLE_NAME_FULL
) -> Dict:
    """
    批量查询多个对象的关系数量。
    
    Args:
        entity_ids: 实体 ID 列表
        relation: 关系类型，None 表示所有关系
        direction: "out", "in", "both"
        table_name: 表名
    
    Returns:
        包含批量统计信息和时间的字典
    """
    start_time = time.time()
    results = []
    
    for entity_id in entity_ids:
        result = query_relation_count(entity_id, relation, direction, table_name)
        results.append({
            "entity_id": entity_id,
            "count": result["total_count"],
            "query_time_ms": result["query_time_ms"]
        })
    
    total_time = time.time() - start_time
    
    return {
        "total_entities": len(entity_ids),
        "results": results,
        "total_time_ms": total_time * 1000,
        "total_time_sec": total_time,
        "avg_time_per_query_ms": (total_time * 1000) / len(entity_ids) if entity_ids else 0
    }


def query_dataset_statistics(table_name: str = TABLE_NAME_FULL) -> Dict:
    """
    查询数据集中包含多少类节点和边。
    
    Args:
        table_name: 表名
    
    Returns:
        包含节点类型、边类型统计信息和时间的字典
    """
    start_time = time.time()
    tbl = connect_db(table_name)
    
    # 读取数据到 pandas
    df_all = tbl.to_pandas()
    
    # 统计节点类型（从 head_type 和 tail_type）
    head_types = df_all["head_type"].value_counts().to_dict()
    tail_types = df_all["tail_type"].value_counts().to_dict()
    
    # 合并所有节点类型（去重）
    all_node_types = set(head_types.keys()) | set(tail_types.keys())
    
    # 统计每种节点类型在 head 和 tail 中的出现次数
    node_type_stats = {}
    for node_type in all_node_types:
        node_type_stats[node_type] = {
            "as_head_count": head_types.get(node_type, 0),
            "as_tail_count": tail_types.get(node_type, 0),
            "total_count": head_types.get(node_type, 0) + tail_types.get(node_type, 0)
        }
    
    # 统计边类型（relation）
    relation_types = df_all["relation"].value_counts().to_dict()
    
    # 统计唯一实体数量
    unique_heads = df_all["head"].nunique()
    unique_tails = df_all["tail"].nunique()
    unique_entities = pd.concat([df_all["head"], df_all["tail"]]).nunique()
    
    elapsed_time = time.time() - start_time
    
    return {
        "total_triples": len(df_all),
        "unique_entities": unique_entities,
        "unique_heads": unique_heads,
        "unique_tails": unique_tails,
        "node_type_count": len(all_node_types),
        "node_types": sorted(list(all_node_types)),
        "node_type_statistics": {
            k: node_type_stats[k] 
            for k in sorted(node_type_stats.keys())
        },
        "edge_type_count": len(relation_types),
        "edge_types": sorted(list(relation_types.keys())),
        "edge_type_statistics": {
            k: relation_types[k] 
            for k in sorted(relation_types.keys(), key=lambda x: relation_types[x], reverse=True)
        },
        "query_time_ms": elapsed_time * 1000,
        "query_time_sec": elapsed_time
    }


def print_query_result(result: Dict):
    """格式化打印查询结果"""
    print("=" * 60)
    if "entity_id" in result:
        print(f"实体 ID: {result['entity_id']}")
    if "relation" in result:
        print(f"关系类型: {result['relation']}")
    if "direction" in result:
        print(f"查询方向: {result['direction']}")
    if "total_count" in result:
        print(f"总数量: {result['total_count']:,}")
    
    if "details" in result:
        print("\n详细信息:")
        for detail in result["details"]:
            print(f"  {detail['direction']} 边: {detail['count']:,} 条")
            if "relations" in detail:
                print(f"    关系分布:")
                for rel, count in detail["relations"].items():
                    print(f"      {rel}: {count:,}")
    
    if "unique_heads" in result:
        print(f"唯一 head 数量: {result['unique_heads']:,}")
        print(f"唯一 tail 数量: {result['unique_tails']:,}")
    
    print(f"\n查询时间: {result.get('query_time_ms', 0):.2f} ms ({result.get('query_time_sec', 0):.4f} 秒)")
    print("=" * 60)


def print_dataset_statistics(result: Dict):
    """格式化打印数据集统计信息"""
    print("=" * 60)
    print("数据集统计信息")
    print("=" * 60)
    print(f"总三元组数: {result['total_triples']:,}")
    print(f"唯一实体数: {result['unique_entities']:,}")
    print(f"唯一 head 数: {result['unique_heads']:,}")
    print(f"唯一 tail 数: {result['unique_tails']:,}")
    
    print(f"\n节点类型数量: {result['node_type_count']}")
    print(f"节点类型列表: {', '.join(result['node_types'])}")
    print("\n节点类型统计:")
    for node_type, stats in result['node_type_statistics'].items():
        print(f"  {node_type}:")
        print(f"    作为 head: {stats['as_head_count']:,} 次")
        print(f"    作为 tail: {stats['as_tail_count']:,} 次")
        print(f"    总计: {stats['total_count']:,} 次")
    
    print(f"\n边类型数量: {result['edge_type_count']}")
    print(f"边类型列表: {', '.join(result['edge_types'])}")
    print("\n边类型统计（按数量排序）:")
    for edge_type, count in result['edge_type_statistics'].items():
        print(f"  {edge_type}: {count:,} 条")
    
    print(f"\n查询时间: {result.get('query_time_ms', 0):.2f} ms ({result.get('query_time_sec', 0):.4f} 秒)")
    print("=" * 60)


def main():
    """示例查询"""
    print("LanceDB 三元组查询工具")
    print("=" * 60)
    
    # 示例 1: 查询某个实体的特定关系数量
    print("\n1️⃣  查询示例：某个 Space 使用的 Model 数量")
    result1 = query_relation_count(
        entity_id="space:JeffreyXiang/TRELLIS",
        relation="space_use_model",
        direction="out",
        table_name=TABLE_NAME_FULL
    )
    print_query_result(result1)
    
    # 示例 2: 查询某个实体的所有关系
    print("\n2️⃣  查询示例：某个实体的所有关系数量")
    result2 = query_relation_count(
        entity_id="space:JeffreyXiang/TRELLIS",
        relation=None,
        direction="both",
        table_name=TABLE_NAME_FULL
    )
    print_query_result(result2)
    
    # 示例 3: 统计某个关系的总体数量
    print("\n3️⃣  查询示例：统计某个关系的总体数量")
    result3 = query_relation_statistics(
        relation="space_use_model",
        table_name=TABLE_NAME_FULL
    )
    print_query_result(result3)
    
    # 示例 4: 查询实体的详细关系信息
    print("\n4️⃣  查询示例：查询实体的详细关系信息（前5条）")
    result4 = query_entity_relations(
        entity_id="space:JeffreyXiang/TRELLIS",
        table_name=TABLE_NAME_FULL,
        limit=5
    )
    print(f"实体 ID: {result4['entity_id']}")
    print(f"出边数量: {result4['outgoing_count']:,}")
    print(f"入边数量: {result4['incoming_count']:,}")
    print(f"查询时间: {result4['query_time_ms']:.2f} ms")
    if result4['outgoing_relations']:
        print("\n前5条出边关系:")
        for i, rel in enumerate(result4['outgoing_relations'][:5], 1):
            print(f"  {i}. {rel['head']} --[{rel['relation']}]--> {rel['tail']}")
    
    # 示例 5: 批量查询
    print("\n5️⃣  批量查询示例：查询多个实体的关系数量")
    entity_ids = [
        "space:JeffreyXiang/TRELLIS",
        "space:black-forest-labs/FLUX.1-dev",
        "model:black-forest-labs/FLUX.1-dev"
    ]
    result5 = batch_query_relation_counts(
        entity_ids=entity_ids,
        relation="space_use_model",
        direction="out",
        table_name=TABLE_NAME_FULL
    )
    print(f"查询实体数: {result5['total_entities']}")
    print(f"总查询时间: {result5['total_time_ms']:.2f} ms")
    print(f"平均每个查询: {result5['avg_time_per_query_ms']:.2f} ms")
    print("\n结果:")
    for r in result5['results']:
        print(f"  {r['entity_id']}: {r['count']:,} 条关系 (查询时间: {r['query_time_ms']:.2f} ms)")
    
    # 示例 6: 查询数据集统计信息
    print("\n6️⃣  数据集统计示例：查询数据集中包含多少类节点和边")
    result6 = query_dataset_statistics(table_name=TABLE_NAME_FULL)
    print_dataset_statistics(result6)


if __name__ == "__main__":
    main()

