#!/usr/bin/env python3
"""
GraphRAG 查询性能测试

测试四类核心查询:
1. 单跳邻居查询 - RAG检索时获取实体上下文
2. 2跳邻居查询 - 扩展实体关联信息
3. 子图提取 - 构建实体相关子图用于上下文
4. 路径查询 - 解释两个实体间的关系

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
TEST_DB_PATH = os.path.join(PROJECT_ROOT, "storage", "lanceDB_query_test")

# 测试参数
BASELINE_ROW_COUNT = 6_246_353    # 基础数据量
TEST_COUNT_PER_GROUP = 50      # 每组测试次数
CLEAR_NEO4J_CACHE = True        # 是否在每次测试前清除 Neo4j 缓存（减少缓存影响）


# ==================== 数据加载与分析 ====================

class DataAnalyzer:
    """数据加载与节点分类"""
    
    def __init__(self, tsv_path: str, row_count: int):
        self.tsv_path = tsv_path
        self.row_count = row_count
        self.data = []
        self.out_degree = defaultdict(int)
        self.in_degree = defaultdict(int)
        self.node_types = {}  # node_id -> type
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
        """
        将节点按关联度分类
        返回: {"low": [...], "medium": [...], "high": [...]}
        """
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
    
    def __init__(self, db_path: str, table_name: str = "triples_query_test"):
        self.db_path = db_path
        self.table_name = table_name
        self.db = lancedb.connect(db_path)
        self.tbl = None
    
    def setup(self, data: list):
        """初始化数据"""
        print(f"\n[方案一] 初始化: {len(data):,} 条")
        
        if self.table_name in self.db.table_names():
            self.db.drop_table(self.table_name)
        
        df = pd.DataFrame(data)
        self.tbl = self.db.create_table(self.table_name, data=df, mode="overwrite")
        print(f"  已导入 {self.tbl.count_rows():,} 条")
    
    # ========== 单跳邻居查询 ==========
    
    def query_out_neighbors(self, entity_id: str) -> tuple:
        """
        查询出边邻居: head=entity_id 的所有 (relation, tail)
        返回: (耗时ms, 结果列表)
        """
        start = time.time()
        result = self.tbl.search().where(f"head = '{entity_id}'").to_pandas()
        elapsed = (time.time() - start) * 1000
        
        neighbors = result[["relation", "tail"]].to_dict("records") if not result.empty else []
        return elapsed, neighbors
    
    def query_in_neighbors(self, entity_id: str) -> tuple:
        """
        查询入边邻居: tail=entity_id 的所有 (head, relation)
        返回: (耗时ms, 结果列表)
        """
        start = time.time()
        result = self.tbl.search().where(f"tail = '{entity_id}'").to_pandas()
        elapsed = (time.time() - start) * 1000
        
        neighbors = result[["head", "relation"]].to_dict("records") if not result.empty else []
        return elapsed, neighbors
    
    def query_all_neighbors(self, entity_id: str) -> tuple:
        """
        查询全部邻居: head=entity_id OR tail=entity_id
        返回: (耗时ms, 结果列表)
        """
        start = time.time()
        
        # 出边
        out_result = self.tbl.search().where(f"head = '{entity_id}'").to_pandas()
        # 入边
        in_result = self.tbl.search().where(f"tail = '{entity_id}'").to_pandas()
        
        elapsed = (time.time() - start) * 1000
        
        neighbors = []
        if not out_result.empty:
            for _, row in out_result.iterrows():
                neighbors.append({"direction": "out", "relation": row["relation"], "node": row["tail"]})
        if not in_result.empty:
            for _, row in in_result.iterrows():
                neighbors.append({"direction": "in", "relation": row["relation"], "node": row["head"]})
        
        return elapsed, neighbors


def test_scheme1_1hop(analyzer: DataAnalyzer):
    """测试方案一的单跳邻居查询"""
    print("\n" + "=" * 70)
    print("方案一: LanceDB 纯三元组 - 单跳邻居查询")
    print("=" * 70)
    
    # 初始化
    scheme = Scheme1LanceDB(TEST_DB_PATH)
    scheme.setup(analyzer.data)
    
    # 分类节点
    classified = analyzer.classify_nodes()
    
    results = {}
    
    for degree_type in ["low", "medium", "high"]:
        nodes = classified[degree_type]
        if len(nodes) < TEST_COUNT_PER_GROUP:
            print(f"\n⚠️ {degree_type} 组节点不足，跳过")
            continue
        
        # 随机选取测试节点
        test_nodes = random.sample(nodes, TEST_COUNT_PER_GROUP)
        
        print(f"\n{'='*70}")
        print(f"关联度: {degree_type.upper()} (测试 {TEST_COUNT_PER_GROUP} 个节点)")
        print(f"{'='*70}")
        
        # ---------- 出边查询 ----------
        print("\n📤 出边邻居查询 (head=entity)")
        out_times, out_counts = [], []
        for node in test_nodes:
            elapsed, neighbors = scheme.query_out_neighbors(node)
            out_times.append(elapsed)
            out_counts.append(len(neighbors))
        print_query_stats("出边查询", out_times, out_counts)
        
        # ---------- 入边查询 ----------
        print("\n📥 入边邻居查询 (tail=entity)")
        in_times, in_counts = [], []
        for node in test_nodes:
            elapsed, neighbors = scheme.query_in_neighbors(node)
            in_times.append(elapsed)
            in_counts.append(len(neighbors))
        print_query_stats("入边查询", in_times, in_counts)
        
        # ---------- 全部邻居查询 ----------
        print("\n🔄 全部邻居查询 (出边+入边)")
        all_times, all_counts = [], []
        for node in test_nodes:
            elapsed, neighbors = scheme.query_all_neighbors(node)
            all_times.append(elapsed)
            all_counts.append(len(neighbors))
        print_query_stats("全部邻居", all_times, all_counts)
        
        # 保存结果
        results[degree_type] = {
            "out": {"time": statistics.mean(out_times), "count": statistics.mean(out_counts)},
            "in": {"time": statistics.mean(in_times), "count": statistics.mean(in_counts)},
            "all": {"time": statistics.mean(all_times), "count": statistics.mean(all_counts)},
        }
    
    # 打印汇总
    print("\n" + "=" * 70)
    print("📊 方案一单跳查询汇总 (平均耗时 ms)")
    print("=" * 70)
    print(f"{'关联度':<10} {'出边查询':<12} {'入边查询':<12} {'全部邻居':<12}")
    print("-" * 46)
    for deg, data in results.items():
        print(f"{deg:<10} {data['out']['time']:<12.2f} {data['in']['time']:<12.2f} {data['all']['time']:<12.2f}")
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
        self.triples_name = "triples_scheme2_query"
        self.adj_name = "adjacency_scheme2_query"
    
    def setup(self, data: list):
        """初始化：创建三元组表和邻接索引表"""
        print(f"\n[方案二] 初始化: {len(data):,} 条")
        
        # 清理旧表
        for name in [self.triples_name, self.adj_name]:
            if name in self.db.table_names():
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
    
    # ========== 单跳邻居查询 ==========
    
    def query_out_neighbors(self, entity_id: str) -> tuple:
        """
        查询出边邻居: 从邻接表获取 out_edges
        返回: (耗时ms, 结果列表)
        """
        start = time.time()
        
        # 查询邻接表
        result = self.adj_tbl.search().where(f"node_id = '{entity_id}'").limit(1).to_pandas()
        
        if result.empty:
            return (time.time() - start) * 1000, []
        
        # 解析 JSON
        out_edges_json = result.iloc[0]["out_edges"]
        out_edges = json.loads(out_edges_json) if out_edges_json else []
        
        # 转换为统一格式
        neighbors = [{"relation": e["relation"], "tail": e["target"]} for e in out_edges]
        
        elapsed = (time.time() - start) * 1000
        return elapsed, neighbors
    
    def query_in_neighbors(self, entity_id: str) -> tuple:
        """
        查询入边邻居: 从邻接表获取 in_edges
        返回: (耗时ms, 结果列表)
        """
        start = time.time()
        
        # 查询邻接表
        result = self.adj_tbl.search().where(f"node_id = '{entity_id}'").limit(1).to_pandas()
        
        if result.empty:
            return (time.time() - start) * 1000, []
        
        # 解析 JSON
        in_edges_json = result.iloc[0]["in_edges"]
        in_edges = json.loads(in_edges_json) if in_edges_json else []
        
        # 转换为统一格式
        neighbors = [{"head": e["source"], "relation": e["relation"]} for e in in_edges]
        
        elapsed = (time.time() - start) * 1000
        return elapsed, neighbors
    
    def query_all_neighbors(self, entity_id: str) -> tuple:
        """
        查询全部邻居: 从邻接表获取 out_edges + in_edges
        返回: (耗时ms, 结果列表)
        """
        start = time.time()
        
        # 查询邻接表
        result = self.adj_tbl.search().where(f"node_id = '{entity_id}'").limit(1).to_pandas()
        
        if result.empty:
            return (time.time() - start) * 1000, []
        
        neighbors = []
        
        # 解析出边
        out_edges_json = result.iloc[0]["out_edges"]
        if out_edges_json:
            out_edges = json.loads(out_edges_json)
            for e in out_edges:
                neighbors.append({"direction": "out", "relation": e["relation"], "node": e["target"]})
        
        # 解析入边
        in_edges_json = result.iloc[0]["in_edges"]
        if in_edges_json:
            in_edges = json.loads(in_edges_json)
            for e in in_edges:
                neighbors.append({"direction": "in", "relation": e["relation"], "node": e["source"]})
        
        elapsed = (time.time() - start) * 1000
        return elapsed, neighbors


def test_scheme2_1hop(analyzer: DataAnalyzer):
    """测试方案二的单跳邻居查询"""
    print("\n" + "=" * 70)
    print("方案二: LanceDB + 邻接索引 - 单跳邻居查询")
    print("=" * 70)
    
    # 初始化
    scheme = Scheme2LanceDB(TEST_DB_PATH)
    scheme.setup(analyzer.data)
    
    # 分类节点（复用方案一的分类结果）
    classified = analyzer.classify_nodes()
    
    results = {}
    
    for degree_type in ["low", "medium", "high"]:
        nodes = classified[degree_type]
        if len(nodes) < TEST_COUNT_PER_GROUP:
            print(f"\n⚠️ {degree_type} 组节点不足，跳过")
            continue
        
        # 随机选取测试节点（使用相同的节点以便对比）
        test_nodes = random.sample(nodes, TEST_COUNT_PER_GROUP)
        
        print(f"\n{'='*70}")
        print(f"关联度: {degree_type.upper()} (测试 {TEST_COUNT_PER_GROUP} 个节点)")
        print(f"{'='*70}")
        
        # ---------- 出边查询 ----------
        print("\n📤 出边邻居查询 (从邻接表获取 out_edges)")
        out_times, out_counts = [], []
        for node in test_nodes:
            elapsed, neighbors = scheme.query_out_neighbors(node)
            out_times.append(elapsed)
            out_counts.append(len(neighbors))
        print_query_stats("出边查询", out_times, out_counts)
        
        # ---------- 入边查询 ----------
        print("\n📥 入边邻居查询 (从邻接表获取 in_edges)")
        in_times, in_counts = [], []
        for node in test_nodes:
            elapsed, neighbors = scheme.query_in_neighbors(node)
            in_times.append(elapsed)
            in_counts.append(len(neighbors))
        print_query_stats("入边查询", in_times, in_counts)
        
        # ---------- 全部邻居查询 ----------
        print("\n🔄 全部邻居查询 (out_edges + in_edges)")
        all_times, all_counts = [], []
        for node in test_nodes:
            elapsed, neighbors = scheme.query_all_neighbors(node)
            all_times.append(elapsed)
            all_counts.append(len(neighbors))
        print_query_stats("全部邻居", all_times, all_counts)
        
        # 保存结果
        results[degree_type] = {
            "out": {"time": statistics.mean(out_times), "count": statistics.mean(out_counts)},
            "in": {"time": statistics.mean(in_times), "count": statistics.mean(in_counts)},
            "all": {"time": statistics.mean(all_times), "count": statistics.mean(all_counts)},
        }
    
    # 打印汇总
    print("\n" + "=" * 70)
    print("📊 方案二单跳查询汇总 (平均耗时 ms)")
    print("=" * 70)
    print(f"{'关联度':<10} {'出边查询':<12} {'入边查询':<12} {'全部邻居':<12}")
    print("-" * 46)
    for deg, data in results.items():
        print(f"{deg:<10} {data['out']['time']:<12.2f} {data['in']['time']:<12.2f} {data['all']['time']:<12.2f}")
    print("=" * 70)
    
    return results


# ==================== 方案三: Neo4j ====================

# Neo4j 配置
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "b230b230"
NEO4J_DATABASE = "neo4j"


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
                # Neo4j 4.x+
                session.run("CALL db.clearQueryCaches()")
        except:
            try:
                # Neo4j 3.x
                with self.driver.session(database=self.db) as session:
                    session.run("CALL dbms.clearQueryCaches()")
            except:
                pass  # 如果都不支持，忽略
    
    def warmup(self, sample_nodes: list, count: int = 5):
        """预热：执行几次查询以加载缓存"""
        print(f"  预热缓存（执行 {count} 次查询）...")
        warmup_nodes = random.sample(sample_nodes, min(count, len(sample_nodes)))
        for node in warmup_nodes:
            # 预热出边和入边查询
            self.query_out_neighbors(node)
            self.query_in_neighbors(node)
            self.query_all_neighbors(node)
    
    def setup(self, data: list, clear_cache: bool = True):
        """初始化：导入基础数据"""
        print(f"\n[方案三] 初始化: {len(data):,} 条")
        
        # 清空数据库
        with self.driver.session(database=self.db) as session:
            session.run("MATCH (n) DETACH DELETE n")
        
        # 清除缓存（如果数据库已存在）
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
    
    # ========== 单跳邻居查询 ==========
    
    def query_out_neighbors(self, entity_id: str) -> tuple:
        """
        查询出边邻居: (entity_id)-[r]->(neighbor)
        返回: (耗时ms, 结果列表)
        """
        query = """
        MATCH (n:Entity {id: $entity_id})-[r:REL]->(m:Entity)
        RETURN r.type AS relation, m.id AS tail
        """
        start = time.time()
        with self.driver.session(database=self.db) as session:
            result = session.run(query, entity_id=entity_id)
            neighbors = [{"relation": record["relation"], "tail": record["tail"]} for record in result]
        elapsed = (time.time() - start) * 1000
        return elapsed, neighbors
    
    def query_in_neighbors(self, entity_id: str) -> tuple:
        """
        查询入边邻居: (neighbor)-[r]->(entity_id)
        返回: (耗时ms, 结果列表)
        """
        query = """
        MATCH (n:Entity {id: $entity_id})<-[r:REL]-(m:Entity)
        RETURN m.id AS head, r.type AS relation
        """
        start = time.time()
        with self.driver.session(database=self.db) as session:
            result = session.run(query, entity_id=entity_id)
            neighbors = [{"head": record["head"], "relation": record["relation"]} for record in result]
        elapsed = (time.time() - start) * 1000
        return elapsed, neighbors
    
    def query_all_neighbors(self, entity_id: str) -> tuple:
        """
        查询全部邻居: (entity_id)-[r]-(neighbor)
        返回: (耗时ms, 结果列表)
        """
        query = """
        MATCH (n:Entity {id: $entity_id})-[r:REL]-(m:Entity)
        RETURN 
            CASE WHEN startNode(r) = n THEN 'out' ELSE 'in' END AS direction,
            r.type AS relation,
            m.id AS node
        """
        start = time.time()
        with self.driver.session(database=self.db) as session:
            result = session.run(query, entity_id=entity_id)
            neighbors = [
                {
                    "direction": record["direction"],
                    "relation": record["relation"],
                    "node": record["node"]
                }
                for record in result
            ]
        elapsed = (time.time() - start) * 1000
        return elapsed, neighbors


def test_scheme3_1hop(analyzer: DataAnalyzer):
    """测试方案三的单跳邻居查询"""
    print("\n" + "=" * 70)
    print("方案三: Neo4j - 单跳邻居查询")
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
        
        # 分类节点
        classified = analyzer.classify_nodes()
        
        results = {}
        
        for degree_type in ["low", "medium", "high"]:
            nodes = classified[degree_type]
            if len(nodes) < TEST_COUNT_PER_GROUP:
                print(f"\n⚠️ {degree_type} 组节点不足，跳过")
                continue
            
            # 随机选取测试节点
            test_nodes = random.sample(nodes, TEST_COUNT_PER_GROUP)
            
            print(f"\n{'='*70}")
            print(f"关联度: {degree_type.upper()} (测试 {TEST_COUNT_PER_GROUP} 个节点)")
            print(f"{'='*70}")
            
            # 清除缓存（如果启用）- 每组测试前清除一次
            if CLEAR_NEO4J_CACHE:
                scheme.clear_caches()
                print("  (已清除查询计划缓存)")
            
            # ---------- 出边查询 ----------
            print("\n📤 出边邻居查询 (Cypher: MATCH (n)-[r]->(m))")
            out_times, out_counts = [], []
            for node in test_nodes:
                elapsed, neighbors = scheme.query_out_neighbors(node)
                out_times.append(elapsed)
                out_counts.append(len(neighbors))
            print_query_stats("出边查询", out_times, out_counts)
            
            # 清除缓存（如果启用）
            if CLEAR_NEO4J_CACHE:
                scheme.clear_caches()
            
            # ---------- 入边查询 ----------
            print("\n📥 入边邻居查询 (Cypher: MATCH (n)<-[r]-(m))")
            in_times, in_counts = [], []
            for node in test_nodes:
                elapsed, neighbors = scheme.query_in_neighbors(node)
                in_times.append(elapsed)
                in_counts.append(len(neighbors))
            print_query_stats("入边查询", in_times, in_counts)
            
            # 清除缓存（如果启用）
            if CLEAR_NEO4J_CACHE:
                scheme.clear_caches()
            
            # ---------- 全部邻居查询 ----------
            print("\n🔄 全部邻居查询 (Cypher: MATCH (n)-[r]-(m))")
            all_times, all_counts = [], []
            for node in test_nodes:
                elapsed, neighbors = scheme.query_all_neighbors(node)
                all_times.append(elapsed)
                all_counts.append(len(neighbors))
            print_query_stats("全部邻居", all_times, all_counts)
            
            # 保存结果
            results[degree_type] = {
                "out": {"time": statistics.mean(out_times), "count": statistics.mean(out_counts)},
                "in": {"time": statistics.mean(in_times), "count": statistics.mean(in_counts)},
                "all": {"time": statistics.mean(all_times), "count": statistics.mean(all_counts)},
            }
        
        # 打印汇总
        print("\n" + "=" * 70)
        print("📊 方案三单跳查询汇总 (平均耗时 ms)")
        print("=" * 70)
        print(f"{'关联度':<10} {'出边查询':<12} {'入边查询':<12} {'全部邻居':<12}")
        print("-" * 46)
        for deg, data in results.items():
            print(f"{deg:<10} {data['out']['time']:<12.2f} {data['in']['time']:<12.2f} {data['all']['time']:<12.2f}")
        print("=" * 70)
        
        return results
    
    finally:
        scheme.close()


# ==================== 主函数 ====================

def main():
    """运行查询性能测试"""
    print("\n")
    print("=" * 70)
    print("GraphRAG 查询性能测试 - 单跳邻居查询")
    print("=" * 70)
    print(f"数据源: {TRIPLES_TSV}")
    print(f"数据量: {BASELINE_ROW_COUNT:,} 条")
    print(f"每组测试: {TEST_COUNT_PER_GROUP} 次")
    print("=" * 70)
    
    # 检查数据文件
    if not os.path.exists(TRIPLES_TSV):
        print(f"❌ 数据文件不存在: {TRIPLES_TSV}")
        return
    
    # 加载数据
    analyzer = DataAnalyzer(TRIPLES_TSV, BASELINE_ROW_COUNT)
    
    # 运行方案一单跳查询测试
    results1 = test_scheme1_1hop(analyzer)
    
    # 运行方案二单跳查询测试
    results2 = test_scheme2_1hop(analyzer)
    
    # 运行方案三单跳查询测试
    results3 = test_scheme3_1hop(analyzer)
    
    # 打印三种方案对比汇总
    print("\n" + "=" * 90)
    print("📊 三种方案对比汇总 (平均耗时 ms)")
    print("=" * 90)
    
    print("\n【出边查询】")
    print(f"{'关联度':<10} {'方案一':<12} {'方案二':<12} {'方案三':<12} {'方案二加速比':<15} {'方案三加速比':<15}")
    print("-" * 74)
    for deg in ["low", "medium", "high"]:
        r1 = results1.get(deg, {}).get("out", {}).get("time", 0)
        r2 = results2.get(deg, {}).get("out", {}).get("time", 0)
        r3 = results3.get(deg, {}).get("out", {}).get("time", 0) if results3 else 0
        speedup2 = r1 / r2 if r2 > 0 else 0
        speedup3 = r1 / r3 if r3 > 0 else 0
        print(f"{deg:<10} {r1:<12.2f} {r2:<12.2f} {r3:<12.2f} {speedup2:<15.2f}x {speedup3:<15.2f}x")
    
    print("\n【入边查询】")
    print(f"{'关联度':<10} {'方案一':<12} {'方案二':<12} {'方案三':<12} {'方案二加速比':<15} {'方案三加速比':<15}")
    print("-" * 74)
    for deg in ["low", "medium", "high"]:
        r1 = results1.get(deg, {}).get("in", {}).get("time", 0)
        r2 = results2.get(deg, {}).get("in", {}).get("time", 0)
        r3 = results3.get(deg, {}).get("in", {}).get("time", 0) if results3 else 0
        speedup2 = r1 / r2 if r2 > 0 else 0
        speedup3 = r1 / r3 if r3 > 0 else 0
        print(f"{deg:<10} {r1:<12.2f} {r2:<12.2f} {r3:<12.2f} {speedup2:<15.2f}x {speedup3:<15.2f}x")
    
    print("\n【全部邻居查询】")
    print(f"{'关联度':<10} {'方案一':<12} {'方案二':<12} {'方案三':<12} {'方案二加速比':<15} {'方案三加速比':<15}")
    print("-" * 74)
    for deg in ["low", "medium", "high"]:
        r1 = results1.get(deg, {}).get("all", {}).get("time", 0)
        r2 = results2.get(deg, {}).get("all", {}).get("time", 0)
        r3 = results3.get(deg, {}).get("all", {}).get("time", 0) if results3 else 0
        speedup2 = r1 / r2 if r2 > 0 else 0
        speedup3 = r1 / r3 if r3 > 0 else 0
        print(f"{deg:<10} {r1:<12.2f} {r2:<12.2f} {r3:<12.2f} {speedup2:<15.2f}x {speedup3:<15.2f}x")
    
    print("=" * 90)
    print("\n✅ 单跳查询测试完成")


if __name__ == "__main__":
    main()

