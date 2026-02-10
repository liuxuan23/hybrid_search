#!/usr/bin/env python3
"""
节点跳数分布统计

使用Neo4j统计数据集中每个节点的最大跳数（从该节点出发最远可达多少跳）
"""

import os
import csv
import time
from collections import defaultdict, Counter
from neo4j import GraphDatabase

# ==================== 配置 ====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # 项目根目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "huggingkg_tiny")
TRIPLES_TSV = os.path.join(DATA_DIR, "triples.tsv")

# Neo4j 配置
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "b230b230"
NEO4J_DATABASE = "neo4j"

# 统计配置
MAX_HOP_TO_CHECK = 10  # 最大检查跳数（超过此跳数认为无限制）
SAMPLE_SIZE = None  # 采样节点数（None表示全部节点）
BATCH_SIZE = 100  # 批量处理大小


# ==================== Neo4j 连接 ====================

class Neo4jAnalyzer:
    """Neo4j 分析器"""
    
    def __init__(self):
        self.driver = None
        self.db = NEO4J_DATABASE
    
    def connect(self) -> bool:
        """连接 Neo4j"""
        try:
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
    
    def get_all_nodes(self) -> list:
        """获取所有节点ID"""
        with self.driver.session(database=self.db) as session:
            result = session.run("MATCH (n:Entity) RETURN n.id AS node_id")
            nodes = [record["node_id"] for record in result]
        return nodes
    
    def find_max_hop_bfs(self, node_id: str, max_hop: int = MAX_HOP_TO_CHECK) -> int:
        """
        使用BFS方法找到节点的最大跳数
        
        Args:
            node_id: 节点ID
            max_hop: 最大检查跳数
        
        Returns:
            最大跳数（如果超过max_hop，返回max_hop+1）
        """
        # 使用Cypher查询找到从该节点出发的最长路径
        # 方法：逐步增加跳数，直到找不到更多节点
        query = """
        MATCH (n:Entity {id: $node_id})-[:REL*1..{max_hop}]->(m:Entity)
        RETURN max(length(path)) AS max_length
        """
        
        # 更高效的方法：使用广度优先的方式，逐步检查每一跳
        for hop in range(1, max_hop + 1):
            check_query = f"""
            MATCH (n:Entity {{id: $node_id}})-[:REL*{hop}..{hop}]->(m:Entity)
            RETURN count(DISTINCT m) AS count
            """
            with self.driver.session(database=self.db) as session:
                result = session.run(check_query, node_id=node_id)
                count = result.single()["count"]
                if count == 0:
                    # 这一跳没有节点，说明最大跳数是 hop-1
                    return hop - 1
        
        # 如果所有跳数都有节点，说明可能超过max_hop
        return max_hop + 1
    
    def find_max_hop_optimized(self, node_id: str, max_hop: int = MAX_HOP_TO_CHECK) -> int:
        """
        优化的方法：使用单次查询找到最大跳数
        
        Args:
            node_id: 节点ID
            max_hop: 最大检查跳数
        
        Returns:
            最大跳数
        """
        # 使用一个查询找到所有可达节点及其距离
        query = """
        MATCH path = (n:Entity {id: $node_id})-[:REL*1..{max_hop}]->(m:Entity)
        RETURN max(length(path)) AS max_hop
        """
        
        try:
            with self.driver.session(database=self.db) as session:
                # 替换查询中的占位符
                actual_query = query.replace("{max_hop}", str(max_hop))
                result = session.run(actual_query, node_id=node_id)
                record = result.single()
                if record and record["max_hop"] is not None:
                    return int(record["max_hop"])
                else:
                    return 0  # 没有可达节点
        except Exception as e:
            print(f"  警告: 查询节点 {node_id} 时出错: {e}")
            return 0
    
    def find_max_hop_incremental(self, node_id: str, max_hop: int = MAX_HOP_TO_CHECK) -> int:
        """
        增量方法：逐步检查每一跳，找到最大跳数
        
        Args:
            node_id: 节点ID
            max_hop: 最大检查跳数
        
        Returns:
            最大跳数
        """
        # 从1跳开始，逐步增加，直到找不到节点
        for hop in range(1, max_hop + 1):
            query = f"""
            MATCH (n:Entity {{id: $node_id}})-[:REL*{hop}..{hop}]->(m:Entity)
            RETURN count(DISTINCT m) AS count
            """
            with self.driver.session(database=self.db) as session:
                result = session.run(query, node_id=node_id)
                count = result.single()["count"]
                if count == 0:
                    # 这一跳没有节点，最大跳数是 hop-1
                    return hop - 1
        
        # 如果所有跳数都有节点，返回max_hop（可能还有更远的）
        return max_hop


# ==================== 统计函数 ====================

def analyze_hop_distribution(analyzer: Neo4jAnalyzer, sample_size: int = None):
    """
    分析节点跳数分布
    
    Args:
        analyzer: Neo4j分析器
        sample_size: 采样节点数（None表示全部）
    """
    print("\n" + "=" * 70)
    print("📊 节点跳数分布统计")
    print("=" * 70)
    
    # 获取所有节点
    print("\n📋 获取节点列表...")
    all_nodes = analyzer.get_all_nodes()
    print(f"✅ 总节点数: {len(all_nodes):,}")
    
    # 采样（如果需要）
    if sample_size and sample_size < len(all_nodes):
        import random
        random.seed(42)
        nodes_to_analyze = random.sample(all_nodes, sample_size)
        print(f"📊 采样节点数: {len(nodes_to_analyze):,}")
    else:
        nodes_to_analyze = all_nodes
        print(f"📊 分析全部节点: {len(nodes_to_analyze):,}")
    
    # 统计每个节点的最大跳数
    print(f"\n🔍 开始分析节点最大跳数（最大检查跳数: {MAX_HOP_TO_CHECK}）...")
    print(f"   批量大小: {BATCH_SIZE}")
    
    node_max_hops = {}
    hop_distribution = Counter()
    
    start_time = time.time()
    for i, node_id in enumerate(nodes_to_analyze):
        if (i + 1) % BATCH_SIZE == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            remaining = (len(nodes_to_analyze) - i - 1) / rate if rate > 0 else 0
            percentage = (i + 1) / len(nodes_to_analyze) * 100
            print(f"   进度: {i + 1}/{len(nodes_to_analyze)} ({percentage:.1f}%) "
                  f"| 已用: {elapsed:.1f}s | 剩余: {remaining:.1f}s")
        
        # 找到该节点的最大跳数
        max_hop = analyzer.find_max_hop_incremental(node_id, MAX_HOP_TO_CHECK)
        node_max_hops[node_id] = max_hop
        hop_distribution[max_hop] += 1
    
    total_time = time.time() - start_time
    print(f"\n✅ 分析完成！总耗时: {total_time:.1f}s")
    print(f"   平均每个节点: {total_time/len(nodes_to_analyze)*1000:.2f}ms")
    
    # 打印统计结果
    print("\n" + "=" * 70)
    print("📊 跳数分布统计")
    print("=" * 70)
    
    print(f"\n{'最大跳数':<12} {'节点数量':<15} {'占比':<15}")
    print("-" * 42)
    
    total_nodes = len(nodes_to_analyze)
    for hop in sorted(hop_distribution.keys()):
        count = hop_distribution[hop]
        percentage = count / total_nodes * 100
        hop_label = f"{hop}跳" if hop < MAX_HOP_TO_CHECK else f"{hop}跳+"
        print(f"{hop_label:<12} {count:<15,} {percentage:<15.2f}%")
    
    # 统计摘要
    print("\n" + "=" * 70)
    print("📈 统计摘要")
    print("=" * 70)
    
    max_hop_values = list(node_max_hops.values())
    if max_hop_values:
        print(f"最大跳数范围: {min(max_hop_values)} - {max(max_hop_values)}")
        print(f"平均最大跳数: {sum(max_hop_values) / len(max_hop_values):.2f}")
        
        # 计算中位数
        sorted_hops = sorted(max_hop_values)
        n = len(sorted_hops)
        if n % 2 == 0:
            median = (sorted_hops[n//2 - 1] + sorted_hops[n//2]) / 2
        else:
            median = sorted_hops[n//2]
        print(f"中位数最大跳数: {median:.2f}")
        
        # 统计各跳数的节点数
        print(f"\n各跳数节点数:")
        for hop in range(0, min(MAX_HOP_TO_CHECK + 2, max(max_hop_values) + 2)):
            count = hop_distribution.get(hop, 0)
            if count > 0:
                print(f"  {hop}跳: {count:,} 个节点 ({count/total_nodes*100:.2f}%)")
    
    print("=" * 70)
    
    return node_max_hops, hop_distribution


# ==================== 主函数 ====================

def main():
    """运行跳数分布分析"""
    print("\n")
    print("=" * 70)
    print("节点跳数分布统计")
    print("=" * 70)
    print(f"数据源: {TRIPLES_TSV}")
    print(f"最大检查跳数: {MAX_HOP_TO_CHECK}")
    print(f"采样节点数: {SAMPLE_SIZE if SAMPLE_SIZE else '全部'}")
    print("=" * 70)
    
    # 连接Neo4j
    analyzer = Neo4jAnalyzer()
    if not analyzer.connect():
        print("❌ 无法连接Neo4j，请确保Neo4j正在运行")
        return
    
    try:
        # 分析跳数分布
        node_max_hops, hop_distribution = analyze_hop_distribution(analyzer, SAMPLE_SIZE)
        
        print("\n✅ 跳数分布分析完成")
        
    finally:
        analyzer.close()


if __name__ == "__main__":
    main()

