#!/usr/bin/env python3
"""
方案二多跳查询性能分析

测试不同跳数对方案二性能的影响：
- 1跳、2跳、3跳、4跳、5跳等
- 分析查询时间、结果数量、查询次数等指标
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
TEST_DB_PATH = os.path.join(PROJECT_ROOT, "storage", "test_db_scheme2_hop_analysis")
BASELINE_ROW_COUNT = 6246353  # 全量数据
TEST_COUNT_PER_GROUP = 50  # 每组测试节点数
MAX_HOPS = 5  # 最大跳数


# ==================== 数据加载 ====================

class DataAnalyzer:
    """数据分析器"""
    
    def __init__(self, tsv_path: str, max_rows: int = None):
        self.tsv_path = tsv_path
        self.data = []
        self.max_rows = max_rows
        self._load_data()
    
    def _load_data(self):
        """加载TSV数据"""
        print(f"📂 加载数据: {self.tsv_path}")
        with open(self.tsv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for i, row in enumerate(reader):
                if self.max_rows and i >= self.max_rows:
                    break
                self.data.append({
                    "head": row["head"],
                    "relation": row["relation"],
                    "tail": row["tail"],
                    "head_type": row.get("head_type", "Unknown"),
                    "tail_type": row.get("tail_type", "Unknown")
                })
        print(f"✅ 已加载 {len(self.data):,} 条数据")
    
    def classify_nodes(self) -> dict:
        """按度数分类节点"""
        print("\n📊 分析节点度数...")
        out_degree = defaultdict(int)
        in_degree = defaultdict(int)
        
        for row in self.data:
            out_degree[row["head"]] += 1
            in_degree[row["tail"]] += 1
        
        # 计算总度数
        total_degree = defaultdict(int)
        for node in set(list(out_degree.keys()) + list(in_degree.keys())):
            total_degree[node] = out_degree[node] + in_degree[node]
        
        # 分类
        low = [n for n, d in total_degree.items() if d <= 5]
        medium = [n for n, d in total_degree.items() if 6 <= d <= 20]
        high = [n for n, d in total_degree.items() if d > 20]
        
        print(f"  低度数 (<=5): {len(low):,}")
        print(f"  中度数 (6-20): {len(medium):,}")
        print(f"  高度数 (>20): {len(high):,}")
        
        return {"low": low, "medium": medium, "high": high}


# ==================== 方案二: LanceDB + 邻接索引 ====================

class Scheme2LanceDB:
    """方案二: LanceDB 三元组表 + 邻接索引表"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = lancedb.connect(db_path)
        self.triples_tbl = None
        self.adj_tbl = None
        self.triples_name = "triples_scheme2_hop_analysis"
        self.adj_name = "adjacency_scheme2_hop_analysis"
    
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
            "out_edges_temp": [],
            "in_edges_temp": []
        })
        
        # 获取所有三元组
        all_df = self.triples_tbl.search().limit(len(data)).to_pandas()
        
        for _, row in all_df.iterrows():
            head, tail = row["head"], row["tail"]
            relation = row["relation"]
            
            adjacency[head]["out_edges_temp"].append({
                "target": tail,
                "relation": relation
            })
            
            if adjacency[head]["node_type"] is None:
                adjacency[head]["node_type"] = row["head_type"]
            
            adjacency[tail]["in_edges_temp"].append({
                "source": head,
                "relation": relation
            })
            
            if adjacency[tail]["node_type"] is None:
                adjacency[tail]["node_type"] = row["tail_type"]
        
        # 先创建邻接表（不带 row_id 信息）
        adj_data = []
        for node_id, info in adjacency.items():
            adj_data.append({
                "node_id": node_id,
                "node_type": info["node_type"] or "Unknown",
                "out_degree": len(info["out_edges_temp"]),
                "in_degree": len(info["in_edges_temp"]),
                "out_edges": "[]",
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
            out_edges = []
            for edge in info["out_edges_temp"]:
                target = edge["target"]
                target_adj_rowid = node_to_adj_rowid.get(target)
                if target_adj_rowid is not None:
                    out_edges.append({
                        "row_id": target_adj_rowid,
                        "relation": edge["relation"],
                        "target": target
                    })
            
            in_edges = []
            for edge in info["in_edges_temp"]:
                source = edge["source"]
                source_adj_rowid = node_to_adj_rowid.get(source)
                if source_adj_rowid is not None:
                    in_edges.append({
                        "row_id": source_adj_rowid,
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
        self.db.drop_table(self.adj_name)
        self.adj_tbl = self.db.create_table(self.adj_name, data=pd.DataFrame(adj_data_updated), mode="overwrite")
        print(f"  邻接表: {self.adj_tbl.count_rows():,} 个节点")
    
    def _get_node_adj_record(self, entity_id: str):
        """获取节点的邻接表记录"""
        result = self.adj_tbl.search().where(f"node_id = '{entity_id}'").limit(1).to_pandas()
        if result.empty:
            return None
        return result.iloc[0]
    
    def query_nhop_neighbors(self, entity_id: str, n: int) -> tuple:
        """
        查询N跳邻居: 使用 take_row_ids 批量获取
        
        Args:
            entity_id: 起始节点ID
            n: 跳数 (1, 2, 3, ...)
        
        Returns:
            (耗时ms, 结果节点列表, 查询统计信息)
        """
        start = time.time()
        query_count = 0  # 记录查询次数
        
        # 第1跳：获取起始节点的邻接表记录
        start_record = self._get_node_adj_record(entity_id)
        query_count += 1
        
        if start_record is None:
            return (time.time() - start) * 1000, [], {
                "query_count": query_count, 
                "take_row_ids_count": 0,
                "total_operations": query_count
            }
        
        # 初始化
        hop1_edges = json.loads(start_record["out_edges"])
        hop1_adj_row_ids = [e["row_id"] for e in hop1_edges]
        
        if not hop1_adj_row_ids:
            return (time.time() - start) * 1000, [], {
                "query_count": query_count, 
                "take_row_ids_count": 0,
                "total_operations": query_count
            }
        
        # 特殊情况：1跳查询，直接返回起始节点的邻居
        if n == 1:
            final_nodes = {e["target"] for e in hop1_edges}
            elapsed = (time.time() - start) * 1000
            stats = {
                "query_count": query_count,
                "take_row_ids_count": 0,
                "total_operations": query_count
            }
            return elapsed, list(final_nodes), stats
        
        # 记录已访问的节点
        visited_nodes = {entity_id}
        take_row_ids_count = 0
        current_adj_row_ids = hop1_adj_row_ids
        final_nodes = set()  # 初始化最终节点集合
        
        # 逐跳遍历（从第2跳到第n跳）
        reached_final_hop = False
        for hop in range(2, n + 1):
            if not current_adj_row_ids:
                # 提前退出，没有更多节点可访问
                final_nodes = set()
                break
            
            # 使用 take_row_ids 批量获取当前跳的邻接表记录
            current_adj_records = self.adj_tbl.take_row_ids(current_adj_row_ids).to_pandas()
            take_row_ids_count += 1
            
            # 收集当前跳的节点和下一跳的 row_id
            current_hop_nodes = set()
            next_adj_row_ids = []
            
            for _, record in current_adj_records.iterrows():
                node_id = record["node_id"]
                current_hop_nodes.add(node_id)
                
                # 如果不是最后一跳，收集下一跳的 row_id
                if hop < n:
                    out_edges = json.loads(record["out_edges"])
                    next_adj_row_ids.extend([e["row_id"] for e in out_edges])
            
            # 更新已访问节点
            visited_nodes.update(current_hop_nodes)
            
            # 如果是最后一跳，这就是最终结果
            if hop == n:
                final_nodes = current_hop_nodes
                reached_final_hop = True
                break
            
            # 准备下一跳
            current_adj_row_ids = next_adj_row_ids
        
        # 确保 final_nodes 已初始化（防止某些边界情况）
        if not reached_final_hop:
            final_nodes = set()
        
        # 排除已访问的节点（起始节点和中间跳的节点）
        final_nodes = final_nodes - visited_nodes
        
        elapsed = (time.time() - start) * 1000
        stats = {
            "query_count": query_count,
            "take_row_ids_count": take_row_ids_count,
            "total_operations": query_count + take_row_ids_count
        }
        
        return elapsed, list(final_nodes), stats


# ==================== 测试函数 ====================

def print_hop_stats(hop_num: int, times: list, counts: list, stats_list: list):
    """打印跳数统计信息"""
    if not times:
        print(f"  ⚠️  无有效数据")
        return
    
    avg_time = statistics.mean(times)
    median_time = statistics.median(times)
    min_time = min(times)
    max_time = max(times)
    avg_count = statistics.mean(counts)
    
    avg_query_count = statistics.mean([s["query_count"] for s in stats_list])
    avg_take_row_ids_count = statistics.mean([s["take_row_ids_count"] for s in stats_list])
    avg_total_ops = statistics.mean([s["total_operations"] for s in stats_list])
    
    print(f"\n  📊 {hop_num}跳查询统计:")
    print(f"     平均耗时: {avg_time:.2f} ms")
    print(f"     中位数耗时: {median_time:.2f} ms")
    print(f"     最小/最大耗时: {min_time:.2f} / {max_time:.2f} ms")
    print(f"     平均结果数: {avg_count:.1f}")
    print(f"     平均查询次数: {avg_query_count:.1f} (where查询)")
    print(f"     平均 take_row_ids 次数: {avg_take_row_ids_count:.1f}")
    print(f"     平均总操作数: {avg_total_ops:.1f}")


def test_scheme2_hop_analysis(analyzer: DataAnalyzer, test_nodes: list, max_hops: int = 5):
    """测试方案二在不同跳数下的性能"""
    print("\n" + "=" * 70)
    print("方案二: 多跳查询性能分析")
    print("=" * 70)
    
    # 初始化
    scheme = Scheme2LanceDB(TEST_DB_PATH)
    scheme.setup(analyzer.data)
    
    print(f"\n{'='*70}")
    print(f"测试节点数: {len(test_nodes)}")
    print(f"最大跳数: {max_hops}")
    print(f"{'='*70}")
    
    # 存储所有跳数的结果
    all_results = {}
    
    # 测试每个跳数
    for hop_num in range(1, max_hops + 1):
        print(f"\n{'='*70}")
        print(f"🔗 {hop_num}跳邻居查询")
        print(f"{'='*70}")
        
        hop_times = []
        hop_counts = []
        hop_stats_list = []
        
        for node in test_nodes:
            elapsed, neighbors, stats = scheme.query_nhop_neighbors(node, hop_num)
            hop_times.append(elapsed)
            hop_counts.append(len(neighbors))
            hop_stats_list.append(stats)
        
        print_hop_stats(hop_num, hop_times, hop_counts, hop_stats_list)
        
        # 保存结果
        all_results[hop_num] = {
            "times": hop_times,
            "counts": hop_counts,
            "stats": hop_stats_list,
            "avg_time": statistics.mean(hop_times),
            "avg_count": statistics.mean(hop_counts),
            "avg_query_count": statistics.mean([s["query_count"] for s in hop_stats_list]),
            "avg_take_row_ids_count": statistics.mean([s["take_row_ids_count"] for s in hop_stats_list]),
            "avg_total_ops": statistics.mean([s["total_operations"] for s in hop_stats_list])
        }
    
    # 打印汇总对比
    print("\n" + "=" * 90)
    print("📊 不同跳数性能对比汇总")
    print("=" * 90)
    
    print(f"\n{'跳数':<8} {'平均耗时(ms)':<15} {'平均结果数':<15} {'where查询':<12} {'take_row_ids':<15} {'总操作数':<12}")
    print("-" * 90)
    
    for hop_num in range(1, max_hops + 1):
        r = all_results[hop_num]
        print(f"{hop_num:<8} {r['avg_time']:<15.2f} {r['avg_count']:<15.1f} "
              f"{r['avg_query_count']:<12.1f} {r['avg_take_row_ids_count']:<15.1f} {r['avg_total_ops']:<12.1f}")
    
    # 分析性能趋势
    print("\n" + "=" * 90)
    print("📈 性能趋势分析")
    print("=" * 90)
    
    times = [all_results[h]["avg_time"] for h in range(1, max_hops + 1)]
    counts = [all_results[h]["avg_count"] for h in range(1, max_hops + 1)]
    ops = [all_results[h]["avg_total_ops"] for h in range(1, max_hops + 1)]
    
    print(f"\n⏱️  查询时间趋势:")
    for i in range(1, len(times)):
        ratio = times[i] / times[i-1] if times[i-1] > 0 else 0
        print(f"   {i}跳 → {i+1}跳: {times[i]:.2f} ms / {times[i-1]:.2f} ms = {ratio:.2f}x")
    
    print(f"\n📊 结果数量趋势:")
    for i in range(1, len(counts)):
        ratio = counts[i] / counts[i-1] if counts[i-1] > 0 else 0
        print(f"   {i}跳 → {i+1}跳: {counts[i]:.1f} / {counts[i-1]:.1f} = {ratio:.2f}x")
    
    print(f"\n🔧 操作次数趋势:")
    for i in range(1, len(ops)):
        ratio = ops[i] / ops[i-1] if ops[i-1] > 0 else 0
        print(f"   {i}跳 → {i+1}跳: {ops[i]:.1f} / {ops[i-1]:.1f} = {ratio:.2f}x")
    
    print("=" * 90)
    
    return all_results


# ==================== 主函数 ====================

def main():
    """运行多跳查询性能分析"""
    print("\n")
    print("=" * 70)
    print("方案二多跳查询性能分析")
    print("=" * 70)
    print(f"数据源: {TRIPLES_TSV}")
    print(f"数据量: {BASELINE_ROW_COUNT:,} 条")
    print(f"测试节点数: {TEST_COUNT_PER_GROUP}")
    print(f"最大跳数: {MAX_HOPS}")
    print("=" * 70)
    
    # 检查数据文件
    if not os.path.exists(TRIPLES_TSV):
        print(f"❌ 数据文件不存在: {TRIPLES_TSV}")
        return
    
    # 加载数据
    analyzer = DataAnalyzer(TRIPLES_TSV, BASELINE_ROW_COUNT)
    
    # 生成测试样本
    print("\n" + "=" * 70)
    print("📋 生成测试样本")
    print("=" * 70)
    classified = analyzer.classify_nodes()
    low_degree_nodes = classified.get("low", [])
    
    if len(low_degree_nodes) < TEST_COUNT_PER_GROUP:
        print(f"❌ 低度数节点不足 ({len(low_degree_nodes)} < {TEST_COUNT_PER_GROUP})，无法进行测试")
        return
    
    # 随机选取测试节点（使用不同的随机种子，确保有足够的3跳结果）
    random.seed(123)  # 使用不同的随机种子
    test_nodes = random.sample(low_degree_nodes, TEST_COUNT_PER_GROUP)
    print(f"✅ 已生成 {len(test_nodes)} 个测试节点样本（低度数节点）")
    print(f"   样本节点示例: {test_nodes[:3]}...")
    print("=" * 70)
    
    # 运行测试
    results = test_scheme2_hop_analysis(analyzer, test_nodes, MAX_HOPS)
    
    print("\n✅ 多跳查询性能分析完成")


if __name__ == "__main__":
    main()

