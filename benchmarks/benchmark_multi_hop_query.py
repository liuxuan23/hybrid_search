#!/usr/bin/env python3
"""
GraphRAG 多跳查询性能测试

测试多跳查询场景:
1. 2跳邻居查询 - 扩展实体关联信息
2. 3跳邻居查询 - 更深层的关联

按关联度分类测试:
- 低关联度: 出度/入度 <= 5
- 中关联度: 出度/入度 6-20
- 高关联度: 出度/入度 > 20
"""

import os
import csv
import time
import random
import statistics
import json
from collections import defaultdict
import lancedb
import pandas as pd

# ==================== 配置 ====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # 项目根目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "huggingkg_tiny")
TRIPLES_TSV = os.path.join(DATA_DIR, "triples.tsv")

# LanceDB 配置
TEST_DB_PATH = os.path.join(PROJECT_ROOT, "storage", "lanceDB_multi_hop_test")

# 测试参数
BASELINE_ROW_COUNT = 6_246_353    # 基础数据量
TEST_COUNT_PER_GROUP = 30         # 每组测试次数


# ==================== 数据加载与分析 ====================

class DataAnalyzer:
    """数据加载与节点分类"""
    
    def __init__(self, tsv_path: str, row_count: int):
        self.tsv_path = tsv_path
        self.row_count = row_count
        self.data = []
        self.out_degree = defaultdict(int)
        self.in_degree = defaultdict(int)
        self.node_types = {}
        self._load_and_analyze()
    
    def _load_and_analyze(self):
        """加载数据并分析"""
        print(f"加载数据: {self.tsv_path}")
        print(f"加载行数: {self.row_count:,}")
        
        with open(self.tsv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for i, row in enumerate(reader):
                if i >= self.row_count:
                    break
                self.data.append({
                    "head_type": row["head_type"],
                    "head": row["head"],
                    "relation": row["relation"],
                    "tail_type": row["tail_type"],
                    "tail": row["tail"],
                })
                self.out_degree[row["head"]] += 1
                self.in_degree[row["tail"]] += 1
                self.node_types[row["head"]] = row["head_type"]
                self.node_types[row["tail"]] = row["tail_type"]
        
        print(f"已加载 {len(self.data):,} 条三元组")
        print(f"唯一节点数: {len(self.node_types):,}")
    
    def get_node_degree(self, node: str) -> int:
        """获取节点总关联度"""
        return self.out_degree[node] + self.in_degree[node]
    
    def classify_nodes(self):
        """将节点按关联度分类"""
        classified = {"low": [], "medium": [], "high": []}
        
        for node in self.node_types.keys():
            degree = self.get_node_degree(node)
            if degree <= 5:
                classified["low"].append(node)
            elif degree <= 20:
                classified["medium"].append(node)
            else:
                classified["high"].append(node)
        
        print(f"\n节点关联度分类:")
        print(f"  低 (<=5):  {len(classified['low']):,} 个")
        print(f"  中 (6-20): {len(classified['medium']):,} 个")
        print(f"  高 (>20):  {len(classified['high']):,} 个")
        
        return classified


# ==================== 工具函数 ====================

def print_query_stats(name: str, times_ms: list, result_counts: list):
    """打印查询统计"""
    if not times_ms:
        print(f"  {name}: 无数据")
        return
    
    avg_time = statistics.mean(times_ms)
    median_time = statistics.median(times_ms)
    avg_results = statistics.mean(result_counts) if result_counts else 0
    
    print(f"  {name}:")
    print(f"    平均耗时: {avg_time:.3f} ms")
    print(f"    中位数: {median_time:.3f} ms")
    print(f"    最小/最大: {min(times_ms):.3f} / {max(times_ms):.3f} ms")
    print(f"    平均结果数: {avg_results:.1f}")


# ==================== 方案一: LanceDB 纯三元组 ====================

class Scheme1LanceDB:
    """方案一: LanceDB 纯三元组表"""
    
    def __init__(self, db_path: str, table_name: str = "triples_multi_hop_test"):
        self.db_path = db_path
        self.table_name = table_name
        self.db = lancedb.connect(db_path)
        self.tbl = None
    
    def setup(self, data: list):
        """初始化数据"""
        print(f"\n[方案一] 初始化: {len(data):,} 条")
        
        if self.table_name in self.db.list_tables():
            self.db.drop_table(self.table_name)
        
        df = pd.DataFrame(data)
        self.tbl = self.db.create_table(self.table_name, data=df, mode="overwrite")
        print(f"  已导入 {self.tbl.count_rows():,} 条")
    
    # ========== 2跳邻居查询 ==========
    
    def query_2hop_neighbors(self, entity_id: str) -> tuple:
        """
        查询2跳邻居: 从 entity_id 出发，经过2条边能到达的所有节点
        
        实现方式:
        1. 查询 entity_id 的1跳邻居
        2. 对每个1跳邻居，查询其1跳邻居
        3. 去重合并
        
        返回: (耗时ms, 结果节点列表)
        """
        start = time.time()
        
        # 第1跳：获取直接邻居
        hop1_df = self.tbl.search().where(f"head = '{entity_id}'").to_pandas()
        hop1_tails = set(hop1_df["tail"].tolist() if not hop1_df.empty else [])
        
        # 第2跳：对每个1跳邻居，查询其出边邻居
        hop2_nodes = set()
        for tail in hop1_tails:
            hop2_df = self.tbl.search().where(f"head = '{tail}'").to_pandas()
            if not hop2_df.empty:
                hop2_nodes.update(hop2_df["tail"].tolist())
        
        # 排除起始节点和1跳邻居
        hop2_nodes.discard(entity_id)
        hop2_nodes = hop2_nodes - hop1_tails
        
        elapsed = (time.time() - start) * 1000
        return elapsed, list(hop2_nodes)
    
    # ========== 3跳邻居查询 ==========
    
    def query_3hop_neighbors(self, entity_id: str) -> tuple:
        """
        查询3跳邻居: 从 entity_id 出发，经过3条边能到达的所有节点
        
        返回: (耗时ms, 结果节点列表)
        """
        start = time.time()
        
        # 第1跳
        hop1_df = self.tbl.search().where(f"head = '{entity_id}'").to_pandas()
        hop1_tails = set(hop1_df["tail"].tolist() if not hop1_df.empty else [])
        
        # 第2跳
        hop2_nodes = set()
        for tail in hop1_tails:
            hop2_df = self.tbl.search().where(f"head = '{tail}'").to_pandas()
            if not hop2_df.empty:
                hop2_nodes.update(hop2_df["tail"].tolist())
        
        # 第3跳
        hop3_nodes = set()
        for node in hop2_nodes:
            hop3_df = self.tbl.search().where(f"head = '{node}'").to_pandas()
            if not hop3_df.empty:
                hop3_nodes.update(hop3_df["tail"].tolist())
        
        # 排除已访问的节点
        visited = {entity_id} | hop1_tails | hop2_nodes
        hop3_nodes = hop3_nodes - visited
        
        elapsed = (time.time() - start) * 1000
        return elapsed, list(hop3_nodes)


def test_scheme1_multi_hop(analyzer: DataAnalyzer, test_nodes: list):
    """测试方案一的多跳查询
    
    Args:
        analyzer: 数据分析器
        test_nodes: 测试节点列表（确保三种方案使用相同样本）
    """
    print("\n" + "=" * 70)
    print("方案一: LanceDB 纯三元组 - 多跳查询")
    print("=" * 70)
    
    # 初始化
    scheme = Scheme1LanceDB(TEST_DB_PATH)
    scheme.setup(analyzer.data)
    
    results = {}
    
    # 只测试低度数节点
    degree_type = "low"
    if len(test_nodes) < TEST_COUNT_PER_GROUP:
        print(f"\n⚠️ 测试节点不足，跳过")
        return results
    
    print(f"\n{'='*70}")
    print(f"关联度: {degree_type.upper()} (测试 {len(test_nodes)} 个节点)")
    print(f"{'='*70}")
    
    # ---------- 2跳邻居查询 ----------
    print("\n🔗 2跳邻居查询")
    hop2_times, hop2_counts = [], []
    for node in test_nodes:
        elapsed, neighbors = scheme.query_2hop_neighbors(node)
        hop2_times.append(elapsed)
        hop2_counts.append(len(neighbors))
    print_query_stats("2跳邻居", hop2_times, hop2_counts)
    
    # ---------- 3跳邻居查询 ----------
    print("\n🔗 3跳邻居查询")
    hop3_times, hop3_counts = [], []
    for node in test_nodes:
        elapsed, neighbors = scheme.query_3hop_neighbors(node)
        hop3_times.append(elapsed)
        hop3_counts.append(len(neighbors))
    print_query_stats("3跳邻居", hop3_times, hop3_counts)
    
    # 保存结果
    results[degree_type] = {
        "2hop": {"time": statistics.mean(hop2_times), "count": statistics.mean(hop2_counts)},
        "3hop": {"time": statistics.mean(hop3_times), "count": statistics.mean(hop3_counts)},
    }
    
    # 打印汇总
    print("\n" + "=" * 70)
    print("📊 方案一多跳查询汇总 (低度数节点, 平均耗时 ms)")
    print("=" * 70)
    if results:
        data = results.get("low", {})
        print(f"2跳邻居查询: {data.get('2hop', {}).get('time', 0):.2f} ms (平均结果数: {data.get('2hop', {}).get('count', 0):.1f})")
        print(f"3跳邻居查询: {data.get('3hop', {}).get('time', 0):.2f} ms (平均结果数: {data.get('3hop', {}).get('count', 0):.1f})")
    print("=" * 70)
    
    return results


# ==================== 方案二: LanceDB + 邻接索引 ====================

class Scheme2LanceDB:
    """方案二: LanceDB 三元组表 + 邻接索引表"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = lancedb.connect(db_path)
        self.triples_tbl = None
        self.adj_tbl = None
        self.triples_name = "triples_scheme2_multi_hop"
        self.adj_name = "adjacency_scheme2_multi_hop"
    
    def setup(self, data: list):
        """初始化：创建三元组表和邻接索引表"""
        print(f"\n[方案二] 初始化: {len(data):,} 条")
        
        # 清理旧表
        for name in [self.triples_name, self.adj_name]:
            if name in self.db.list_tables():
                self.db.drop_table(name)
        
        # 1. 创建三元组表
        df_triples = pd.DataFrame(data)
        self.triples_tbl = self.db.create_table(self.triples_name, data=df_triples, mode="overwrite")
        print(f"  三元组表: {self.triples_tbl.count_rows():,} 条")
        
        # 2. 构建邻接索引
        print("  构建邻接索引...")
        adjacency = defaultdict(lambda: {
            "node_type": None,
            "out_edges_temp": [],  # 临时存储，包含三元组信息
            "in_edges_temp": []
        })
        
        # 获取所有三元组
        all_df = self.triples_tbl.search().limit(len(data)).to_pandas()
        
        for _, row in all_df.iterrows():
            head, tail = row["head"], row["tail"]
            relation = row["relation"]
            
            # 更新 head 节点的 out_edges（临时存储）
            if adjacency[head]["node_type"] is None:
                adjacency[head]["node_type"] = row["head_type"]
            adjacency[head]["out_edges_temp"].append({
                "target": tail,
                "relation": relation
            })
            
            # 更新 tail 节点的 in_edges（临时存储）
            if adjacency[tail]["node_type"] is None:
                adjacency[tail]["node_type"] = row["tail_type"]
            adjacency[tail]["in_edges_temp"].append({
                "source": head,
                "relation": relation
            })
        
        # 先创建邻接表（不带 row_id 信息）
        adj_data = []
        for node_id, info in adjacency.items():
            adj_data.append({
                "node_id": node_id,
                "node_type": info["node_type"] or "Unknown",
                "out_degree": len(info["out_edges_temp"]),
                "in_degree": len(info["in_edges_temp"]),
                "out_edges": "[]",  # 临时占位
                "in_edges": "[]"
            })
        
        df_adj = pd.DataFrame(adj_data)
        self.adj_tbl = self.db.create_table(self.adj_name, data=df_adj, mode="overwrite")
        
        # 获取邻接表中每个节点的 row_id
        all_adj_df = self.adj_tbl.search().with_row_id(True).limit(len(adj_data)).to_pandas()
        node_to_adj_rowid = {}
        for _, row in all_adj_df.iterrows():
            node_to_adj_rowid[row["node_id"]] = int(row["_rowid"])
        
        # 重新构建 out_edges 和 in_edges，使用邻接表的 row_id
        adj_data_updated = []
        for node_id, info in adjacency.items():
            # 构建 out_edges：存储目标节点在邻接表中的 row_id
            out_edges = []
            for edge in info["out_edges_temp"]:
                target = edge["target"]
                target_adj_rowid = node_to_adj_rowid.get(target)
                if target_adj_rowid is not None:
                    out_edges.append({
                        "row_id": target_adj_rowid,  # 邻接表的 row_id
                        "relation": edge["relation"],
                        "target": target
                    })
            
            # 构建 in_edges：存储源节点在邻接表中的 row_id
            in_edges = []
            for edge in info["in_edges_temp"]:
                source = edge["source"]
                source_adj_rowid = node_to_adj_rowid.get(source)
                if source_adj_rowid is not None:
                    in_edges.append({
                        "row_id": source_adj_rowid,  # 邻接表的 row_id
                        "relation": edge["relation"],
                        "source": source
                    })
            
            adj_data_updated.append({
                "node_id": node_id,
                "node_type": info["node_type"] or "Unknown",
                "out_degree": len(out_edges),
                "in_degree": len(in_edges),
                "out_edges": json.dumps(out_edges),
                "in_edges": json.dumps(in_edges)
            })
        
        # 更新邻接表
        df_adj_updated = pd.DataFrame(adj_data_updated)
        self.db.drop_table(self.adj_name)
        self.adj_tbl = self.db.create_table(self.adj_name, data=df_adj_updated, mode="overwrite")
        print(f"  邻接表: {self.adj_tbl.count_rows():,} 节点")
    
    def _get_node_adj_record(self, entity_id: str):
        """获取节点的邻接表记录（用于起始节点）"""
        result = self.adj_tbl.search().where(f"node_id = '{entity_id}'").limit(1).to_pandas()
        if result.empty:
            return None
        return result.iloc[0]
    
    # ========== 2跳邻居查询 ==========
    
    def query_2hop_neighbors(self, entity_id: str) -> tuple:
        """
        查询2跳邻居: 使用 take_row_ids 批量获取
        
        返回: (耗时ms, 结果节点列表)
        """
        start = time.time()
        
        # 第1跳：获取起始节点的邻接表记录
        start_record = self._get_node_adj_record(entity_id)
        if start_record is None:
            return (time.time() - start) * 1000, []
        
        hop1_edges = json.loads(start_record["out_edges"])
        hop1_adj_row_ids = [e["row_id"] for e in hop1_edges]  # 1跳节点在邻接表中的row_id
        
        if not hop1_adj_row_ids:
            return (time.time() - start) * 1000, []
        
        # 使用 take_row_ids 批量获取1跳节点的邻接表记录
        hop1_adj_records = self.adj_tbl.take_row_ids(hop1_adj_row_ids).to_pandas()
        
        # 从这些记录中提取它们的 out_edges，收集2跳节点的邻接表row_id
        hop2_adj_row_ids = []
        hop1_nodes = set()
        for _, record in hop1_adj_records.iterrows():
            hop1_nodes.add(record["node_id"])
            out_edges = json.loads(record["out_edges"])
            hop2_adj_row_ids.extend([e["row_id"] for e in out_edges])
        
        if not hop2_adj_row_ids:
            return (time.time() - start) * 1000, []
        
        # 使用 take_row_ids 批量获取2跳节点的邻接表记录
        hop2_adj_records = self.adj_tbl.take_row_ids(hop2_adj_row_ids).to_pandas()
        hop2_nodes = set(hop2_adj_records["node_id"].tolist())
        
        # 排除起始节点和1跳邻居
        hop2_nodes.discard(entity_id)
        hop2_nodes = hop2_nodes - hop1_nodes
        
        elapsed = (time.time() - start) * 1000
        return elapsed, list(hop2_nodes)
    
    # ========== 3跳邻居查询 ==========
    
    def query_3hop_neighbors(self, entity_id: str) -> tuple:
        """
        查询3跳邻居: 使用 take_row_ids 批量获取
        
        返回: (耗时ms, 结果节点列表)
        """
        start = time.time()
        
        # 第1跳：获取起始节点的邻接表记录
        start_record = self._get_node_adj_record(entity_id)
        if start_record is None:
            return (time.time() - start) * 1000, []
        
        hop1_edges = json.loads(start_record["out_edges"])
        hop1_adj_row_ids = [e["row_id"] for e in hop1_edges]
        
        if not hop1_adj_row_ids:
            return (time.time() - start) * 1000, []
        
        # 使用 take_row_ids 批量获取1跳节点的邻接表记录
        hop1_adj_records = self.adj_tbl.take_row_ids(hop1_adj_row_ids).to_pandas()
        
        # 第2跳：收集2跳节点的邻接表row_id
        hop2_adj_row_ids = []
        hop1_nodes = set()
        for _, record in hop1_adj_records.iterrows():
            hop1_nodes.add(record["node_id"])
            out_edges = json.loads(record["out_edges"])
            hop2_adj_row_ids.extend([e["row_id"] for e in out_edges])
        
        if not hop2_adj_row_ids:
            return (time.time() - start) * 1000, []
        
        # 使用 take_row_ids 批量获取2跳节点的邻接表记录
        hop2_adj_records = self.adj_tbl.take_row_ids(hop2_adj_row_ids).to_pandas()
        
        # 第3跳：收集3跳节点的邻接表row_id
        hop3_adj_row_ids = []
        hop2_nodes = set()
        for _, record in hop2_adj_records.iterrows():
            hop2_nodes.add(record["node_id"])
            out_edges = json.loads(record["out_edges"])
            hop3_adj_row_ids.extend([e["row_id"] for e in out_edges])
        
        if not hop3_adj_row_ids:
            return (time.time() - start) * 1000, []
        
        # 使用 take_row_ids 批量获取3跳节点的邻接表记录
        hop3_adj_records = self.adj_tbl.take_row_ids(hop3_adj_row_ids).to_pandas()
        hop3_nodes = set(hop3_adj_records["node_id"].tolist())
        
        # 排除已访问的节点
        visited = {entity_id} | hop1_nodes | hop2_nodes
        hop3_nodes = hop3_nodes - visited
        
        elapsed = (time.time() - start) * 1000
        return elapsed, list(hop3_nodes)


def test_scheme2_multi_hop(analyzer: DataAnalyzer, test_nodes: list):
    """测试方案二的多跳查询
    
    Args:
        analyzer: 数据分析器
        test_nodes: 测试节点列表（确保三种方案使用相同样本）
    """
    print("\n" + "=" * 70)
    print("方案二: LanceDB + 邻接索引 - 多跳查询")
    print("=" * 70)
    
    # 初始化
    scheme = Scheme2LanceDB(TEST_DB_PATH)
    scheme.setup(analyzer.data)
    
    results = {}
    
    # 只测试低度数节点
    degree_type = "low"
    if len(test_nodes) < TEST_COUNT_PER_GROUP:
        print(f"\n⚠️ 测试节点不足，跳过")
        return results
    
    print(f"\n{'='*70}")
    print(f"关联度: {degree_type.upper()} (测试 {len(test_nodes)} 个节点)")
    print(f"{'='*70}")
    
    # ---------- 2跳邻居查询 ----------
    print("\n🔗 2跳邻居查询 (从邻接表获取)")
    hop2_times, hop2_counts = [], []
    for node in test_nodes:
        elapsed, neighbors = scheme.query_2hop_neighbors(node)
        hop2_times.append(elapsed)
        hop2_counts.append(len(neighbors))
    print_query_stats("2跳邻居", hop2_times, hop2_counts)
    
    # ---------- 3跳邻居查询 ----------
    print("\n🔗 3跳邻居查询 (从邻接表获取)")
    hop3_times, hop3_counts = [], []
    for node in test_nodes:
        elapsed, neighbors = scheme.query_3hop_neighbors(node)
        hop3_times.append(elapsed)
        hop3_counts.append(len(neighbors))
    print_query_stats("3跳邻居", hop3_times, hop3_counts)
    
    # 保存结果
    results[degree_type] = {
        "2hop": {"time": statistics.mean(hop2_times), "count": statistics.mean(hop2_counts)},
        "3hop": {"time": statistics.mean(hop3_times), "count": statistics.mean(hop3_counts)},
    }
    
    # 打印汇总
    print("\n" + "=" * 70)
    print("📊 方案二多跳查询汇总 (低度数节点, 平均耗时 ms)")
    print("=" * 70)
    if results:
        data = results.get("low", {})
        print(f"2跳邻居查询: {data.get('2hop', {}).get('time', 0):.2f} ms (平均结果数: {data.get('2hop', {}).get('count', 0):.1f})")
        print(f"3跳邻居查询: {data.get('3hop', {}).get('time', 0):.2f} ms (平均结果数: {data.get('3hop', {}).get('count', 0):.1f})")
    print("=" * 70)
    
    return results


# ==================== 方案三: Neo4j ====================

# Neo4j 配置
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "b230b230"
NEO4J_DATABASE = "neo4j"
CLEAR_NEO4J_CACHE = True  # 是否清除缓存


class Scheme3Neo4j:
    """方案三: Neo4j 图数据库"""
    
    def __init__(self):
        self.driver = None
        self.db = NEO4J_DATABASE
    
    def connect(self) -> bool:
        """连接 Neo4j"""
        try:
            from neo4j import GraphDatabase
            self.driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            self.driver.verify_connectivity()
            return True
        except Exception as e:
            print(f"❌ 无法连接 Neo4j: {e}")
            return False
    
    def close(self):
        """关闭连接"""
        if self.driver:
            self.driver.close()
    
    def clear_caches(self):
        """清除 Neo4j 查询计划缓存"""
        try:
            with self.driver.session(database=self.db) as session:
                session.run("CALL db.clearQueryCaches()")
        except:
            try:
                with self.driver.session(database=self.db) as session:
                    session.run("CALL dbms.clearQueryCaches()")
            except:
                pass
    
    def setup(self, data: list, clear_cache: bool = True):
        """初始化：导入基础数据"""
        print(f"\n[方案三] 初始化: {len(data):,} 条")
        
        # 清空数据库
        with self.driver.session(database=self.db) as session:
            session.run("MATCH (n) DETACH DELETE n")
        
        if clear_cache:
            self.clear_caches()
        
        # 创建索引
        with self.driver.session(database=self.db) as session:
            try:
                session.run("CREATE INDEX entity_id_index IF NOT EXISTS FOR (n:Entity) ON (n.id)")
            except:
                pass
        
        # 批量导入
        batch_size = 1000
        query = """
        UNWIND $batch AS row
        MERGE (head:Entity {id: row.head})
        ON CREATE SET head.type = row.head_type
        MERGE (tail:Entity {id: row.tail})
        ON CREATE SET tail.type = row.tail_type
        MERGE (head)-[r:REL {type: row.relation}]->(tail)
        """
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            with self.driver.session(database=self.db) as session:
                session.run(query, batch=batch)
        
        # 统计
        with self.driver.session(database=self.db) as session:
            node_count = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        print(f"  节点: {node_count:,}, 关系: {rel_count:,}")
    
    # ========== 2跳邻居查询 ==========
    
    def query_2hop_neighbors(self, entity_id: str) -> tuple:
        """
        查询2跳邻居: 使用 Cypher 路径查询
        
        返回: (耗时ms, 结果节点列表)
        """
        query = """
        MATCH (n:Entity {id: $entity_id})-[:REL*2..2]->(m:Entity)
        RETURN DISTINCT m.id AS node_id
        """
        start = time.time()
        with self.driver.session(database=self.db) as session:
            result = session.run(query, entity_id=entity_id)
            neighbors = [record["node_id"] for record in result]
        elapsed = (time.time() - start) * 1000
        return elapsed, neighbors
    
    # ========== 3跳邻居查询 ==========
    
    def query_3hop_neighbors(self, entity_id: str) -> tuple:
        """
        查询3跳邻居: 使用 Cypher 路径查询
        
        返回: (耗时ms, 结果节点列表)
        """
        query = """
        MATCH (n:Entity {id: $entity_id})-[:REL*3..3]->(m:Entity)
        RETURN DISTINCT m.id AS node_id
        """
        start = time.time()
        with self.driver.session(database=self.db) as session:
            result = session.run(query, entity_id=entity_id)
            neighbors = [record["node_id"] for record in result]
        elapsed = (time.time() - start) * 1000
        return elapsed, neighbors


def test_scheme3_multi_hop(analyzer: DataAnalyzer, test_nodes: list):
    """测试方案三的多跳查询
    
    Args:
        analyzer: 数据分析器
        test_nodes: 测试节点列表（确保三种方案使用相同样本）
    """
    print("\n" + "=" * 70)
    print("方案三: Neo4j - 多跳查询")
    if CLEAR_NEO4J_CACHE:
        print("⚠️  缓存控制: 每次测试前清除查询计划缓存")
    print("=" * 70)
    
    # 连接 Neo4j
    scheme = Scheme3Neo4j()
    if not scheme.connect():
        print("跳过方案三测试")
        return {}
    
    try:
        # 初始化数据
        scheme.setup(analyzer.data, clear_cache=False)
        
        results = {}
        
        # 只测试低度数节点
        degree_type = "low"
        if len(test_nodes) < TEST_COUNT_PER_GROUP:
            print(f"\n⚠️ 测试节点不足，跳过")
            return results
        
        print(f"\n{'='*70}")
        print(f"关联度: {degree_type.upper()} (测试 {len(test_nodes)} 个节点)")
        print(f"{'='*70}")
        
        # 清除缓存（如果启用）
        if CLEAR_NEO4J_CACHE:
            scheme.clear_caches()
            print("  (已清除查询计划缓存)")
        
        # ---------- 2跳邻居查询 ----------
        print("\n🔗 2跳邻居查询 (Cypher: MATCH (n)-[:REL*2..2]->(m))")
        hop2_times, hop2_counts = [], []
        for node in test_nodes:
            elapsed, neighbors = scheme.query_2hop_neighbors(node)
            hop2_times.append(elapsed)
            hop2_counts.append(len(neighbors))
        print_query_stats("2跳邻居", hop2_times, hop2_counts)
        
        # 清除缓存（如果启用）
        if CLEAR_NEO4J_CACHE:
            scheme.clear_caches()
        
        # ---------- 3跳邻居查询 ----------
        print("\n🔗 3跳邻居查询 (Cypher: MATCH (n)-[:REL*3..3]->(m))")
        hop3_times, hop3_counts = [], []
        for node in test_nodes:
            elapsed, neighbors = scheme.query_3hop_neighbors(node)
            hop3_times.append(elapsed)
            hop3_counts.append(len(neighbors))
        print_query_stats("3跳邻居", hop3_times, hop3_counts)
        
        # 保存结果
        results[degree_type] = {
            "2hop": {"time": statistics.mean(hop2_times), "count": statistics.mean(hop2_counts)},
            "3hop": {"time": statistics.mean(hop3_times), "count": statistics.mean(hop3_counts)},
        }
        
        # 打印汇总
        print("\n" + "=" * 70)
        print("📊 方案三多跳查询汇总 (低度数节点, 平均耗时 ms)")
        print("=" * 70)
        if results:
            data = results.get("low", {})
            print(f"2跳邻居查询: {data.get('2hop', {}).get('time', 0):.2f} ms (平均结果数: {data.get('2hop', {}).get('count', 0):.1f})")
            print(f"3跳邻居查询: {data.get('3hop', {}).get('time', 0):.2f} ms (平均结果数: {data.get('3hop', {}).get('count', 0):.1f})")
        print("=" * 70)
        
        return results
    
    finally:
        scheme.close()


# ==================== 主函数 ====================

def main():
    """运行多跳查询性能测试"""
    print("\n")
    print("=" * 70)
    print("GraphRAG 多跳查询性能测试")
    print("=" * 70)
    print(f"数据源: {TRIPLES_TSV}")
    print(f"数据量: {BASELINE_ROW_COUNT:,} 条")
    print(f"每组测试: {TEST_COUNT_PER_GROUP} 次")
    print(f"测试节点: 仅低度数节点 (关联度 <= 5)")
    print("=" * 70)
    
    # 检查数据文件
    if not os.path.exists(TRIPLES_TSV):
        print(f"❌ 数据文件不存在: {TRIPLES_TSV}")
        return
    
    # 加载数据
    analyzer = DataAnalyzer(TRIPLES_TSV, BASELINE_ROW_COUNT)
    
    # 提前生成测试样本（确保三种方案使用相同样本）
    print("\n" + "=" * 70)
    print("📋 生成测试样本")
    print("=" * 70)
    classified = analyzer.classify_nodes()
    low_degree_nodes = classified.get("low", [])
    
    if len(low_degree_nodes) < TEST_COUNT_PER_GROUP:
        print(f"❌ 低度数节点不足 ({len(low_degree_nodes)} < {TEST_COUNT_PER_GROUP})，无法进行测试")
        return
    
    # 随机选取测试节点（固定随机种子以确保可复现）
    random.seed(123)  # 固定随机种子
    test_nodes = random.sample(low_degree_nodes, TEST_COUNT_PER_GROUP)
    print(f"✅ 已生成 {len(test_nodes)} 个测试节点样本（低度数节点）")
    print(f"   样本节点示例: {test_nodes[:3]}...")
    print("=" * 70)
    
    # 运行方案一多跳查询测试
    results1 = test_scheme1_multi_hop(analyzer, test_nodes)
    
    # 运行方案二多跳查询测试
    results2 = test_scheme2_multi_hop(analyzer, test_nodes)
    
    # 运行方案三多跳查询测试
    results3 = test_scheme3_multi_hop(analyzer, test_nodes)
    
    # 打印三种方案对比汇总
    print("\n" + "=" * 90)
    print("📊 三种方案对比汇总 (低度数节点, 平均耗时 ms)")
    print("=" * 90)
    
    print("\n【2跳邻居查询】")
    print(f"{'方案':<15} {'平均耗时(ms)':<15} {'平均结果数':<15} {'方案二加速比':<18} {'方案三加速比':<18}")
    print("-" * 81)
    r1_2hop = results1.get("low", {}).get("2hop", {})
    r2_2hop = results2.get("low", {}).get("2hop", {})
    r3_2hop = results3.get("low", {}).get("2hop", {}) if results3 else {}
    speedup2_2hop = r1_2hop.get("time", 0) / r2_2hop.get("time", 1) if r2_2hop.get("time", 0) > 0 else 0
    speedup3_2hop = r1_2hop.get("time", 0) / r3_2hop.get("time", 1) if r3_2hop.get("time", 0) > 0 else 0
    print(f"{'方案一':<15} {r1_2hop.get('time', 0):<15.2f} {r1_2hop.get('count', 0):<15.1f}")
    print(f"{'方案二':<15} {r2_2hop.get('time', 0):<15.2f} {r2_2hop.get('count', 0):<15.1f} {speedup2_2hop:<18.2f}x")
    if r3_2hop:
        print(f"{'方案三':<15} {r3_2hop.get('time', 0):<15.2f} {r3_2hop.get('count', 0):<15.1f} {'':<18} {speedup3_2hop:<18.2f}x")
    
    print("\n【3跳邻居查询】")
    print(f"{'方案':<15} {'平均耗时(ms)':<15} {'平均结果数':<15} {'方案二加速比':<18} {'方案三加速比':<18}")
    print("-" * 81)
    r1_3hop = results1.get("low", {}).get("3hop", {})
    r2_3hop = results2.get("low", {}).get("3hop", {})
    r3_3hop = results3.get("low", {}).get("3hop", {}) if results3 else {}
    speedup2_3hop = r1_3hop.get("time", 0) / r2_3hop.get("time", 1) if r2_3hop.get("time", 0) > 0 else 0
    speedup3_3hop = r1_3hop.get("time", 0) / r3_3hop.get("time", 1) if r3_3hop.get("time", 0) > 0 else 0
    print(f"{'方案一':<15} {r1_3hop.get('time', 0):<15.2f} {r1_3hop.get('count', 0):<15.1f}")
    print(f"{'方案二':<15} {r2_3hop.get('time', 0):<15.2f} {r2_3hop.get('count', 0):<15.1f} {speedup2_3hop:<18.2f}x")
    if r3_3hop:
        print(f"{'方案三':<15} {r3_3hop.get('time', 0):<15.2f} {r3_3hop.get('count', 0):<15.1f} {'':<18} {speedup3_3hop:<18.2f}x")
    
    print("=" * 90)
    print("\n✅ 多跳查询测试完成")


if __name__ == "__main__":
    main()

