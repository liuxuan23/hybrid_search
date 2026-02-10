"""
三种方案写入性能对比测试

方案一: LanceDB 纯三元组表
方案二: LanceDB 三元组 + 邻接索引表
方案三: Neo4j 图数据库

本文件分步骤实现，便于逐步测试验证。
"""

import os
import csv
import time
import lancedb
import pandas as pd
from tqdm import tqdm

# ==================== 配置 ====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # 项目根目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "huggingkg_tiny")
TRIPLES_TSV = os.path.join(DATA_DIR, "triples.tsv")

# 测试数据库路径（与原数据库分开，避免污染）
TEST_DB_PATH = os.path.join(PROJECT_ROOT, "storage", "lanceDB_benchmark")

# 测试参数
TEST_ROW_COUNT = 6_246_353  # 测试全部条目
BATCH_SIZE = 10_000       # 每批写入 1 万条


# ==================== 工具函数 ====================

def get_dir_size(path: str) -> int:
    """计算目录的总大小（字节）"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                total_size += os.path.getsize(filepath)
    return total_size


def format_size(size_bytes: int) -> str:
    """格式化文件大小为人类可读格式"""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


# ==================== 方案一: LanceDB 纯三元组 ====================

def benchmark_scheme1_lancedb_triples(
    tsv_path: str,
    db_path: str,
    table_name: str = "triples_scheme1",
    row_count: int = TEST_ROW_COUNT,
    batch_size: int = BATCH_SIZE
):
    """
    方案一: 直接将三元组导入 LanceDB 表
    
    Args:
        tsv_path: TSV 文件路径
        db_path: LanceDB 数据库路径
        table_name: 表名
        row_count: 导入行数
        batch_size: 每批写入行数
    
    Returns:
        dict: 包含写入时间和统计信息
    """
    print("=" * 70)
    print("方案一: LanceDB 纯三元组表")
    print("=" * 70)
    print(f"数据源: {tsv_path}")
    print(f"目标表: {table_name}")
    print(f"导入行数: {row_count:,}")
    print(f"批次大小: {batch_size:,}")
    print("-" * 70)
    
    # 确保数据库目录存在
    os.makedirs(db_path, exist_ok=True)
    
    # 连接数据库
    db = lancedb.connect(db_path)
    
    # 如果表已存在，先删除
    if table_name in db.table_names():
        db.drop_table(table_name)
        print(f"已删除旧表: {table_name}")
    
    # 开始计时
    start_time = time.time()
    
    # 读取并写入数据
    batch_data = []
    rows_written = 0
    first_batch = True
    tbl = None
    
    with open(tsv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        
        for row in tqdm(reader, total=row_count, desc="写入三元组"):
            # 达到目标行数则停止
            if rows_written >= row_count:
                break
            
            batch_data.append({
                "head_type": row["head_type"],
                "head": row["head"],
                "relation": row["relation"],
                "tail_type": row["tail_type"],
                "tail": row["tail"],
            })
            
            # 达到批次大小时写入
            if len(batch_data) >= batch_size:
                df = pd.DataFrame(batch_data)
                
                if first_batch:
                    tbl = db.create_table(table_name, data=df, mode="overwrite")
                    first_batch = False
                else:
                    tbl.add(df)
                
                rows_written += len(batch_data)
                batch_data = []
        
        # 写入剩余数据
        if batch_data:
            df = pd.DataFrame(batch_data)
            if first_batch:
                tbl = db.create_table(table_name, data=df, mode="overwrite")
            else:
                tbl.add(df)
            rows_written += len(batch_data)
    
    # 结束计时
    end_time = time.time()
    elapsed = end_time - start_time
    
    # 计算存储占用
    table_path = os.path.join(db_path, f"{table_name}.lance")
    storage_bytes = get_dir_size(table_path) if os.path.exists(table_path) else 0
    bytes_per_row = storage_bytes / rows_written if rows_written > 0 else 0
    
    # 统计结果
    result = {
        "scheme": "方案一: LanceDB 纯三元组",
        "rows_written": rows_written,
        "elapsed_seconds": elapsed,
        "throughput": rows_written / elapsed,  # 行/秒
        "storage_bytes": storage_bytes,
        "bytes_per_row": bytes_per_row,
        "table_name": table_name
    }
    
    # 打印结果
    print("-" * 70)
    print(f"✅ 写入完成")
    print(f"   写入行数: {result['rows_written']:,}")
    print(f"   耗时: {result['elapsed_seconds']:.2f} 秒")
    print(f"   吞吐量: {result['throughput']:,.0f} 行/秒")
    print(f"   存储占用: {format_size(storage_bytes)} ({bytes_per_row:.1f} 字节/行)")
    print("=" * 70)
    
    return result


# ==================== 方案二: LanceDB 三元组 + 邻接索引 ====================

def benchmark_scheme2_lancedb_with_adjacency(
    tsv_path: str,
    db_path: str,
    triples_table: str = "triples_scheme2",
    adjacency_table: str = "adjacency_scheme2",
    row_count: int = TEST_ROW_COUNT,
    batch_size: int = BATCH_SIZE
):
    """
    方案二: 三元组表 + 邻接索引表
    
    步骤:
    1. 写入三元组表（同方案一）
    2. 构建邻接索引表（记录每个节点的出边/入边 row_id）
    
    Args:
        tsv_path: TSV 文件路径
        db_path: LanceDB 数据库路径
        triples_table: 三元组表名
        adjacency_table: 邻接表名
        row_count: 导入行数
        batch_size: 每批写入行数
    
    Returns:
        dict: 包含写入时间和统计信息
    """
    import json
    from collections import defaultdict
    
    print("=" * 70)
    print("方案二: LanceDB 三元组 + 邻接索引")
    print("=" * 70)
    print(f"数据源: {tsv_path}")
    print(f"三元组表: {triples_table}")
    print(f"邻接索引表: {adjacency_table}")
    print(f"导入行数: {row_count:,}")
    print(f"批次大小: {batch_size:,}")
    print("-" * 70)
    
    # 确保数据库目录存在
    os.makedirs(db_path, exist_ok=True)
    db = lancedb.connect(db_path)
    
    # 删除旧表
    for tbl_name in [triples_table, adjacency_table]:
        if tbl_name in db.table_names():
            db.drop_table(tbl_name)
            print(f"已删除旧表: {tbl_name}")
    
    # ========== 步骤1: 写入三元组表 ==========
    print("\n步骤1: 写入三元组表...")
    step1_start = time.time()
    
    batch_data = []
    rows_written = 0
    first_batch = True
    tbl = None
    
    # 同时收集邻接信息
    adjacency = defaultdict(lambda: {"node_type": None, "out_edges": [], "in_edges": []})
    
    with open(tsv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        
        for row in tqdm(reader, total=row_count, desc="写入三元组"):
            if rows_written >= row_count:
                break
            
            # 当前行的 row_id（在无删除情况下等于 offset）
            current_row_id = rows_written
            
            # 收集邻接信息
            head, tail = row["head"], row["tail"]
            relation = row["relation"]
            
            # 更新 head 节点的出边（存储目标节点ID，后续会替换为邻接表的row_id）
            adjacency[head]["node_type"] = row["head_type"]
            adjacency[head]["out_edges"].append({"target_node": tail, "rel": relation})
            
            # 更新 tail 节点的入边（存储源节点ID，后续会替换为邻接表的row_id）
            adjacency[tail]["node_type"] = row["tail_type"]
            adjacency[tail]["in_edges"].append({"source_node": head, "rel": relation})
            
            batch_data.append({
                "head_type": row["head_type"],
                "head": head,
                "relation": relation,
                "tail_type": row["tail_type"],
                "tail": tail,
            })
            
            if len(batch_data) >= batch_size:
                df = pd.DataFrame(batch_data)
                if first_batch:
                    tbl = db.create_table(triples_table, data=df, mode="overwrite")
                    first_batch = False
                else:
                    tbl.add(df)
                rows_written += len(batch_data)
                batch_data = []
        
        if batch_data:
            df = pd.DataFrame(batch_data)
            if first_batch:
                tbl = db.create_table(triples_table, data=df, mode="overwrite")
            else:
                tbl.add(df)
            rows_written += len(batch_data)
    
    step1_time = time.time() - step1_start
    print(f"   三元组写入完成: {rows_written:,} 行, 耗时 {step1_time:.2f} 秒")
    
    # ========== 步骤2: 构建邻接索引表 ==========
    print("\n步骤2: 构建邻接索引表...")
    step2_start = time.time()
    
    # 先将邻接信息转换为 DataFrame（临时写入，用于获取row_id）
    adjacency_data = []
    for node_id, info in tqdm(adjacency.items(), desc="准备邻接数据"):
        adjacency_data.append({
            "node_id": node_id,
            "node_type": info["node_type"],
            "out_degree": len(info["out_edges"]),
            "in_degree": len(info["in_edges"]),
            "out_edges": json.dumps([]),  # 临时为空，后续更新
            "in_edges": json.dumps([]),   # 临时为空，后续更新
        })
    
    # 写入邻接表（获取row_id）
    df_adj = pd.DataFrame(adjacency_data)
    adj_tbl = db.create_table(adjacency_table, data=df_adj, mode="overwrite")
    
    # 建立节点ID到邻接表row_id的映射
    print("   建立节点ID到row_id映射...")
    node_id_to_row_id = {}
    adj_df = adj_tbl.search().with_row_id(True).limit(len(adjacency_data)).to_pandas()
    for _, row in adj_df.iterrows():
        node_id_to_row_id[row["node_id"]] = int(row["_rowid"])
    
    # 更新边信息：将节点ID替换为邻接表的row_id
    print("   更新边信息（节点ID -> 邻接表row_id）...")
    updated_adjacency_data = []
    for node_id, info in tqdm(adjacency.items(), desc="更新边信息"):
        # 更新出边：将target_node替换为邻接表的row_id
        updated_out_edges = []
        for edge in info["out_edges"]:
            target_node = edge["target_node"]
            if target_node in node_id_to_row_id:
                updated_out_edges.append({
                    "row_id": node_id_to_row_id[target_node],
                    "rel": edge["rel"]
                })
        
        # 更新入边：将source_node替换为邻接表的row_id
        updated_in_edges = []
        for edge in info["in_edges"]:
            source_node = edge["source_node"]
            if source_node in node_id_to_row_id:
                updated_in_edges.append({
                    "row_id": node_id_to_row_id[source_node],
                    "rel": edge["rel"]
                })
        
        updated_adjacency_data.append({
            "node_id": node_id,
            "node_type": info["node_type"],
            "out_degree": len(updated_out_edges),
            "in_degree": len(updated_in_edges),
            "out_edges": json.dumps(updated_out_edges),
            "in_edges": json.dumps(updated_in_edges),
        })
    
    # 重新写入邻接表（使用更新后的边信息）
    print("   重新写入邻接表...")
    df_adj_updated = pd.DataFrame(updated_adjacency_data)
    db.drop_table(adjacency_table)
    adj_tbl = db.create_table(adjacency_table, data=df_adj_updated, mode="overwrite")
    
    step2_time = time.time() - step2_start
    print(f"   邻接索引构建完成: {len(updated_adjacency_data):,} 节点, 耗时 {step2_time:.2f} 秒")
    
    # ========== 统计结果 ==========
    total_time = step1_time + step2_time
    
    # 计算存储占用
    triples_path = os.path.join(db_path, f"{triples_table}.lance")
    adjacency_path = os.path.join(db_path, f"{adjacency_table}.lance")
    triples_bytes = get_dir_size(triples_path) if os.path.exists(triples_path) else 0
    adjacency_bytes = get_dir_size(adjacency_path) if os.path.exists(adjacency_path) else 0
    total_bytes = triples_bytes + adjacency_bytes
    bytes_per_row = total_bytes / rows_written if rows_written > 0 else 0
    
    result = {
        "scheme": "方案二: LanceDB + 邻接索引",
        "rows_written": rows_written,
        "nodes_count": len(updated_adjacency_data),
        "elapsed_seconds": total_time,
        "step1_time": step1_time,
        "step2_time": step2_time,
        "throughput": rows_written / total_time,
        "storage_bytes": total_bytes,
        "triples_bytes": triples_bytes,
        "adjacency_bytes": adjacency_bytes,
        "bytes_per_row": bytes_per_row,
    }
    
    # 打印结果
    print("-" * 70)
    print(f"✅ 写入完成")
    print(f"   三元组: {rows_written:,} 行 ({step1_time:.2f} 秒)")
    print(f"   邻接索引: {len(updated_adjacency_data):,} 节点 ({step2_time:.2f} 秒)")
    print(f"   总耗时: {total_time:.2f} 秒")
    print(f"   吞吐量: {result['throughput']:,.0f} 行/秒")
    print(f"   存储占用: {format_size(total_bytes)} (三元组 {format_size(triples_bytes)} + 邻接 {format_size(adjacency_bytes)})")
    print("=" * 70)
    
    return result


# ==================== 方案三: Neo4j ====================

# Neo4j 配置
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "b230b230"
NEO4J_DATABASE = "neo4j"
NEO4J_BATCH_SIZE = 1000  # Neo4j 事务批次较小


def benchmark_scheme3_neo4j(
    tsv_path: str,
    row_count: int = TEST_ROW_COUNT,
    batch_size: int = NEO4J_BATCH_SIZE,
    clear_existing: bool = True
):
    """
    方案三: Neo4j 图数据库
    
    步骤:
    1. 清空现有数据（可选）
    2. 批量创建节点和关系
    
    Args:
        tsv_path: TSV 文件路径
        row_count: 导入行数
        batch_size: Neo4j 事务批次大小
        clear_existing: 是否清空现有数据
    
    Returns:
        dict: 包含写入时间和统计信息
    """
    from neo4j import GraphDatabase
    
    print("=" * 70)
    print("方案三: Neo4j 图数据库")
    print("=" * 70)
    print(f"数据源: {tsv_path}")
    print(f"Neo4j: {NEO4J_URI}")
    print(f"导入行数: {row_count:,}")
    print(f"批次大小: {batch_size:,}")
    print("-" * 70)
    
    # 连接 Neo4j
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        driver.verify_connectivity()
        print("✅ 已连接到 Neo4j")
    except Exception as e:
        print(f"❌ 无法连接到 Neo4j: {e}")
        print("   请确保 Neo4j 服务正在运行")
        return None
    
    try:
        # 清空数据（如果需要）
        if clear_existing:
            with driver.session(database=NEO4J_DATABASE) as session:
                session.run("MATCH (n) DETACH DELETE n")
                print("已清空数据库")
        
        # 创建索引
        with driver.session(database=NEO4J_DATABASE) as session:
            try:
                session.run("CREATE INDEX entity_id_index IF NOT EXISTS FOR (n:Entity) ON (n.id)")
                print("已创建索引")
            except:
                pass
        
        # 开始计时
        start_time = time.time()
        
        # 读取数据并批量导入
        rows_written = 0
        batch_data = []
        
        # Cypher 批量导入查询
        query = """
        UNWIND $batch AS row
        MERGE (head:Entity {id: row.head})
        ON CREATE SET head.type = row.head_type
        MERGE (tail:Entity {id: row.tail})
        ON CREATE SET tail.type = row.tail_type
        MERGE (head)-[r:REL {type: row.relation}]->(tail)
        """
        
        with open(tsv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            
            for row in tqdm(reader, total=row_count, desc="导入 Neo4j"):
                if rows_written >= row_count:
                    break
                
                batch_data.append({
                    "head_type": row["head_type"],
                    "head": row["head"],
                    "relation": row["relation"],
                    "tail_type": row["tail_type"],
                    "tail": row["tail"],
                })
                
                # 达到批次大小时写入
                if len(batch_data) >= batch_size:
                    with driver.session(database=NEO4J_DATABASE) as session:
                        session.run(query, batch=batch_data)
                    rows_written += len(batch_data)
                    batch_data = []
            
            # 写入剩余数据
            if batch_data:
                with driver.session(database=NEO4J_DATABASE) as session:
                    session.run(query, batch=batch_data)
                rows_written += len(batch_data)
        
        # 结束计时
        elapsed = time.time() - start_time
        
        # 统计节点和关系数量
        with driver.session(database=NEO4J_DATABASE) as session:
            node_count = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        
        result = {
            "scheme": "方案三: Neo4j",
            "rows_written": rows_written,
            "nodes_count": node_count,
            "relations_count": rel_count,
            "elapsed_seconds": elapsed,
            "throughput": rows_written / elapsed,
            "storage_bytes": 0,  # Neo4j 存储不容易获取
            "bytes_per_row": 0,
        }
        
        # 打印结果
        print("-" * 70)
        print(f"✅ 写入完成")
        print(f"   导入行数: {rows_written:,}")
        print(f"   节点数: {node_count:,}")
        print(f"   关系数: {rel_count:,}")
        print(f"   耗时: {elapsed:.2f} 秒")
        print(f"   吞吐量: {result['throughput']:,.0f} 行/秒")
        print("=" * 70)
        
        return result
        
    finally:
        driver.close()


# ==================== 主函数 ====================

def main():
    """运行写入性能测试"""
    print("\n")
    print("=" * 70)
    print("三种方案写入性能对比测试")
    print("=" * 70)
    print(f"测试数据量: {TEST_ROW_COUNT:,} 条三元组")
    print(f"批次大小: {BATCH_SIZE:,}")
    print("=" * 70)
    
    # 检查数据文件
    if not os.path.exists(TRIPLES_TSV):
        print(f"❌ 数据文件不存在: {TRIPLES_TSV}")
        return
    
    # 运行方案一测试
    print("\n")
    result1 = benchmark_scheme1_lancedb_triples(
        tsv_path=TRIPLES_TSV,
        db_path=TEST_DB_PATH,
        row_count=TEST_ROW_COUNT,
        batch_size=BATCH_SIZE
    )
    
    # 运行方案二测试
    print("\n")
    result2 = benchmark_scheme2_lancedb_with_adjacency(
        tsv_path=TRIPLES_TSV,
        db_path=TEST_DB_PATH,
        row_count=TEST_ROW_COUNT,
        batch_size=BATCH_SIZE
    )
    
    # 运行方案三测试
    print("\n")
    result3 = benchmark_scheme3_neo4j(
        tsv_path=TRIPLES_TSV,
        row_count=TEST_ROW_COUNT,
        batch_size=NEO4J_BATCH_SIZE,
        clear_existing=True
    )
    
    # 汇总结果
    print("\n")
    print("=" * 70)
    print("📊 测试结果汇总")
    print("=" * 70)
    print(f"{'方案':<30} {'耗时(秒)':<10} {'吞吐量(行/秒)':<15} {'存储占用':<12}")
    print("-" * 70)
    print(f"{result1['scheme']:<30} {result1['elapsed_seconds']:<10.2f} {result1['throughput']:<15,.0f} {format_size(result1['storage_bytes']):<12}")
    print(f"{result2['scheme']:<30} {result2['elapsed_seconds']:<10.2f} {result2['throughput']:<15,.0f} {format_size(result2['storage_bytes']):<12}")
    if result3:
        print(f"{result3['scheme']:<30} {result3['elapsed_seconds']:<10.2f} {result3['throughput']:<15,.0f} {'N/A':<12}")
    else:
        print(f"{'方案三: Neo4j':<30} {'连接失败':<10}")
    print("=" * 70)


if __name__ == "__main__":
    main()

