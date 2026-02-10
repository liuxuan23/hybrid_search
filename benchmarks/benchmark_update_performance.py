#!/usr/bin/env python3
"""
三元组更新性能测试

测试三种方案在增量更新场景下的性能：
- 插入 (INSERT): 添加新三元组
- 删除 (DELETE): 移除现有三元组  
- 修改 (UPDATE): 更新三元组的关系或节点

考虑不同关联度的影响：
- 孤立节点: 头/尾节点无其他关联
- 低关联度: 出度/入度 <= 5
- 中关联度: 出度/入度 5-20
- 高关联度: 出度/入度 > 20

数据源: triples.tsv
"""

import os
import csv
import time
import random
import statistics
from collections import defaultdict
import lancedb
import pandas as pd

# ==================== 配置 ====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)  # 项目根目录
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "huggingkg_tiny")
TRIPLES_TSV = os.path.join(DATA_DIR, "triples.tsv")

# LanceDB 配置
TEST_DB_PATH = os.path.join(PROJECT_ROOT, "storage", "lanceDB_update_test")

# 测试参数
BASELINE_ROW_COUNT = 50_000    # 基础数据量（足够计算关联度）
TEST_COUNT_PER_GROUP = 30      # 每组关联度测试次数


# ==================== 数据加载与分析 ====================

class DataAnalyzer:
    """数据加载与关联度分析"""
    
    def __init__(self, tsv_path: str, row_count: int):
        self.tsv_path = tsv_path
        self.row_count = row_count
        self.data = []           # 三元组数据
        self.out_degree = defaultdict(int)  # 节点出度
        self.in_degree = defaultdict(int)   # 节点入度
        self._load_and_analyze()
    
    def _load_and_analyze(self):
        """加载数据并分析关联度"""
        print(f"加载数据文件: {self.tsv_path}")
        print(f"加载行数: {self.row_count:,}")
        
        with open(self.tsv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for i, row in enumerate(reader):
                if i >= self.row_count:
                    break
                triple = {
                    "head_type": row["head_type"],
                    "head": row["head"],
                    "relation": row["relation"],
                    "tail_type": row["tail_type"],
                    "tail": row["tail"],
                    "idx": i,  # 记录索引
                }
                self.data.append(triple)
                
                # 统计出度和入度
                self.out_degree[row["head"]] += 1
                self.in_degree[row["tail"]] += 1
        
        print(f"已加载 {len(self.data):,} 条三元组")
        print(f"唯一节点数: {len(set(self.out_degree.keys()) | set(self.in_degree.keys())):,}")
        
        # 统计关联度分布
        self._print_degree_stats()
    
    def _print_degree_stats(self):
        """打印关联度统计"""
        all_degrees = []
        for node in set(self.out_degree.keys()) | set(self.in_degree.keys()):
            degree = self.out_degree[node] + self.in_degree[node]
            all_degrees.append(degree)
        
        if all_degrees:
            print(f"\n关联度统计:")
            print(f"  最小: {min(all_degrees)}")
            print(f"  最大: {max(all_degrees)}")
            print(f"  平均: {statistics.mean(all_degrees):.1f}")
            print(f"  中位数: {statistics.median(all_degrees):.1f}")
    
    def get_node_degree(self, node: str) -> int:
        """获取节点总关联度（出度+入度）"""
        return self.out_degree[node] + self.in_degree[node]
    
    def get_triple_max_degree(self, triple: dict) -> int:
        """获取三元组中节点的最大关联度"""
        head_deg = self.get_node_degree(triple["head"])
        tail_deg = self.get_node_degree(triple["tail"])
        return max(head_deg, tail_deg)
    
    def classify_triples(self):
        """
        将三元组按关联度分类
        返回: {
            "isolated": [...],   # 关联度 == 1（仅当前边）
            "low": [...],        # 关联度 2-5
            "medium": [...],     # 关联度 6-20
            "high": [...]        # 关联度 > 20
        }
        """
        classified = {
            "isolated": [],
            "low": [],
            "medium": [],
            "high": []
        }
        
        for triple in self.data:
            max_deg = self.get_triple_max_degree(triple)
            
            if max_deg == 1:
                classified["isolated"].append(triple)
            elif max_deg <= 5:
                classified["low"].append(triple)
            elif max_deg <= 20:
                classified["medium"].append(triple)
            else:
                classified["high"].append(triple)
        
        # 打印分类统计
        print(f"\n三元组关联度分类:")
        for key, triples in classified.items():
            print(f"  {key}: {len(triples):,} 条")
        
        return classified
    
    def get_new_triple(self, idx: int) -> dict:
        """获取新三元组（用于插入测试，从数据末尾之后构造）"""
        # 从未使用的数据中取，或构造新数据
        if idx + self.row_count < 6_000_000:  # 确保有足够数据
            with open(self.tsv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for i, row in enumerate(reader):
                    if i == self.row_count + idx:
                        return {
                            "head_type": row["head_type"],
                            "head": row["head"],
                            "relation": row["relation"],
                            "tail_type": row["tail_type"],
                            "tail": row["tail"],
                        }
        # 回退：生成新节点
        return {
            "head_type": "NewType",
            "head": f"NEW_HEAD_{idx}",
            "relation": "new_relation",
            "tail_type": "NewType",
            "tail": f"NEW_TAIL_{idx}",
        }


# ==================== 工具函数 ====================

def print_stats(name: str, times_ms: list):
    """打印统计结果"""
    if not times_ms:
        print(f"  {name}: 无数据")
        return
    print(f"  {name} ({len(times_ms)} 次):")
    print(f"    平均: {statistics.mean(times_ms):.3f} ms")
    print(f"    中位数: {statistics.median(times_ms):.3f} ms")
    print(f"    最小/最大: {min(times_ms):.3f} / {max(times_ms):.3f} ms")


# ==================== 方案一: LanceDB 纯三元组 ====================

class Scheme1LanceDB:
    """方案一: LanceDB 纯三元组表"""
    
    def __init__(self, db_path: str, table_name: str = "triples_update_test"):
        self.db_path = db_path
        self.table_name = table_name
        self.db = lancedb.connect(db_path)
        self.tbl = None
    
    def setup(self, data: list):
        """初始化：导入基础数据"""
        print(f"\n[方案一] 初始化: {len(data):,} 条")
        
        if self.table_name in self.db.table_names():
            self.db.drop_table(self.table_name)
        
        # 移除 idx 字段
        clean_data = [{k: v for k, v in d.items() if k != "idx"} for d in data]
        df = pd.DataFrame(clean_data)
        self.tbl = self.db.create_table(self.table_name, data=df, mode="overwrite")
        print(f"  已导入 {self.tbl.count_rows():,} 条")
    
    def insert(self, triple: dict) -> float:
        """插入单条，返回耗时(ms)"""
        clean = {k: v for k, v in triple.items() if k != "idx"}
        df = pd.DataFrame([clean])
        start = time.time()
        self.tbl.add(df)
        return (time.time() - start) * 1000
    
    def delete(self, triple: dict) -> float:
        """删除单条，返回耗时(ms)"""
        condition = f"head = '{triple['head']}' AND relation = '{triple['relation']}' AND tail = '{triple['tail']}'"
        start = time.time()
        self.tbl.delete(condition)
        return (time.time() - start) * 1000
    
    def update(self, triple: dict, new_relation: str) -> float:
        """修改单条关系，返回耗时(ms)"""
        condition = f"head = '{triple['head']}' AND relation = '{triple['relation']}'"
        start = time.time()
        self.tbl.update(where=condition, values={"relation": new_relation})
        return (time.time() - start) * 1000


def test_scheme1_by_degree(analyzer: DataAnalyzer):
    """按关联度测试方案一的更新性能"""
    print("\n" + "=" * 70)
    print("方案一: LanceDB 纯三元组 - 按关联度测试")
    print("=" * 70)
    
    # 分类三元组
    classified = analyzer.classify_triples()
    
    # 初始化
    scheme = Scheme1LanceDB(TEST_DB_PATH)
    
    results = {}
    
    for degree_type in ["isolated", "low", "medium", "high"]:
        samples = classified[degree_type]
        if len(samples) < TEST_COUNT_PER_GROUP:
            print(f"\n⚠️ {degree_type} 组样本不足 ({len(samples)} 条)，跳过")
            continue
        
        print(f"\n{'='*70}")
        print(f"关联度: {degree_type.upper()}")
        print(f"{'='*70}")
        
        # 随机选取测试样本
        test_samples = random.sample(samples, TEST_COUNT_PER_GROUP)
        
        # ========== 插入测试 ==========
        print(f"\n📥 插入测试")
        scheme.setup(analyzer.data)  # 重置
        
        insert_times = []
        for i in range(TEST_COUNT_PER_GROUP):
            new_triple = analyzer.get_new_triple(i + degree_type.__hash__() % 1000)
            elapsed = scheme.insert(new_triple)
            insert_times.append(elapsed)
        print_stats("插入", insert_times)
        
        # ========== 删除测试 ==========
        print(f"\n🗑️ 删除测试")
        scheme.setup(analyzer.data)  # 重置
        
        delete_times = []
        for triple in test_samples:
            elapsed = scheme.delete(triple)
            delete_times.append(elapsed)
        print_stats("删除", delete_times)
        
        # ========== 修改测试 ==========
        print(f"\n✏️ 修改测试")
        scheme.setup(analyzer.data)  # 重置
        
        update_times = []
        for i, triple in enumerate(test_samples):
            new_rel = f"modified_{degree_type}_{i}"
            elapsed = scheme.update(triple, new_rel)
            update_times.append(elapsed)
        print_stats("修改", update_times)
        
        # 保存结果
        results[degree_type] = {
            "insert": statistics.mean(insert_times) if insert_times else 0,
            "delete": statistics.mean(delete_times) if delete_times else 0,
            "update": statistics.mean(update_times) if update_times else 0,
        }
    
    # 打印汇总
    print("\n" + "=" * 70)
    print("📊 方案一测试结果汇总 (平均耗时 ms)")
    print("=" * 70)
    print(f"{'关联度':<12} {'插入':<12} {'删除':<12} {'修改':<12}")
    print("-" * 48)
    for degree_type, times in results.items():
        print(f"{degree_type:<12} {times['insert']:<12.3f} {times['delete']:<12.3f} {times['update']:<12.3f}")
    print("=" * 70)
    
    return results


# ==================== 方案二: LanceDB + 邻接索引 ====================

import json

class Scheme2LanceDB:
    """
    方案二: LanceDB 三元组表 + 邻接索引表
    
    表结构:
    - triples: head_type, head, relation, tail_type, tail
    - adjacency: node_id, node_type, out_degree, in_degree, out_edges(JSON), in_edges(JSON)
    """
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = lancedb.connect(db_path)
        self.triples_tbl = None
        self.adj_tbl = None
        self.triples_name = "triples_scheme2"
        self.adj_name = "adjacency_scheme2"
    
    def setup(self, data: list):
        """初始化：创建三元组表和邻接索引表"""
        print(f"\n[方案二] 初始化: {len(data):,} 条")
        
        # 清理旧表
        for name in [self.triples_name, self.adj_name]:
            if name in self.db.table_names():
                self.db.drop_table(name)
        
        # 1. 创建三元组表
        clean_data = [{k: v for k, v in d.items() if k != "idx"} for d in data]
        df_triples = pd.DataFrame(clean_data)
        self.triples_tbl = self.db.create_table(self.triples_name, data=df_triples, mode="overwrite")
        print(f"  三元组表: {self.triples_tbl.count_rows():,} 条")
        
        # 2. 构建邻接索引
        adjacency = defaultdict(lambda: {
            "node_type": None,
            "out_edges": [],
            "in_edges": []
        })
        
        # 获取所有三元组及其 row_id
        all_df = self.triples_tbl.search().with_row_id(True).limit(len(data)).to_pandas()
        
        for _, row in all_df.iterrows():
            row_id = int(row["_rowid"])
            head, tail = row["head"], row["tail"]
            relation = row["relation"]
            
            # 更新 head 节点的 out_edges（存储目标节点ID，后续会替换为邻接表的row_id）
            if adjacency[head]["node_type"] is None:
                adjacency[head]["node_type"] = row["head_type"]
            adjacency[head]["out_edges"].append({
                "target_node": tail,
                "relation": relation
            })
            
            # 更新 tail 节点的 in_edges（存储源节点ID，后续会替换为邻接表的row_id）
            if adjacency[tail]["node_type"] is None:
                adjacency[tail]["node_type"] = row["tail_type"]
            adjacency[tail]["in_edges"].append({
                "source_node": head,
                "relation": relation
            })
        
        # 先将邻接信息转换为 DataFrame（临时写入，用于获取row_id）
        adj_data = []
        for node_id, info in adjacency.items():
            adj_data.append({
                "node_id": node_id,
                "node_type": info["node_type"] or "Unknown",
                "out_degree": len(info["out_edges"]),
                "in_degree": len(info["in_edges"]),
                "out_edges": json.dumps([]),  # 临时为空，后续更新
                "in_edges": json.dumps([]),   # 临时为空，后续更新
            })
        
        # 写入邻接表（获取row_id）
        df_adj = pd.DataFrame(adj_data)
        self.adj_tbl = self.db.create_table(self.adj_name, data=df_adj, mode="overwrite")
        
        # 建立节点ID到邻接表row_id的映射
        node_id_to_row_id = {}
        adj_df = self.adj_tbl.search().with_row_id(True).limit(len(adj_data)).to_pandas()
        for _, row in adj_df.iterrows():
            node_id_to_row_id[row["node_id"]] = int(row["_rowid"])
        
        # 更新边信息：将节点ID替换为邻接表的row_id
        updated_adjacency_data = []
        for node_id, info in adjacency.items():
            # 更新出边：将target_node替换为邻接表的row_id
            updated_out_edges = []
            for edge in info["out_edges"]:
                target_node = edge["target_node"]
                if target_node in node_id_to_row_id:
                    updated_out_edges.append({
                        "row_id": node_id_to_row_id[target_node],
                        "rel": edge["relation"]
                    })
            
            # 更新入边：将source_node替换为邻接表的row_id
            updated_in_edges = []
            for edge in info["in_edges"]:
                source_node = edge["source_node"]
                if source_node in node_id_to_row_id:
                    updated_in_edges.append({
                        "row_id": node_id_to_row_id[source_node],
                        "rel": edge["relation"]
                    })
            
            updated_adjacency_data.append({
                "node_id": node_id,
                "node_type": info["node_type"] or "Unknown",
                "out_degree": len(updated_out_edges),
                "in_degree": len(updated_in_edges),
                "out_edges": json.dumps(updated_out_edges),
                "in_edges": json.dumps(updated_in_edges),
            })
        
        # 重新写入邻接表（使用更新后的边信息）
        df_adj_updated = pd.DataFrame(updated_adjacency_data)
        self.db.drop_table(self.adj_name)
        self.adj_tbl = self.db.create_table(self.adj_name, data=df_adj_updated, mode="overwrite")
        print(f"  邻接表: {self.adj_tbl.count_rows():,} 节点")
    
    def insert(self, triple: dict) -> dict:
        """
        插入三元组
        
        步骤:
        1. 插入三元组表
        2. 更新 head 节点的 out_edges
        3. 更新 tail 节点的 in_edges
        
        返回: {"total": ms, "triples": ms, "adj_head": ms, "adj_tail": ms}
        """
        times = {}
        clean = {k: v for k, v in triple.items() if k != "idx"}
        head, tail, relation = clean["head"], clean["tail"], clean["relation"]
        
        # 1. 插入三元组表
        start = time.time()
        df = pd.DataFrame([clean])
        self.triples_tbl.add(df)
        times["triples"] = (time.time() - start) * 1000
        
        # 获取tail和head节点在邻接表中的row_id（如果存在）
        tail_row_id = self._get_node_row_id(tail)
        head_row_id = self._get_node_row_id(head)
        
        # 2. 更新 head 节点的 out_edges（使用tail节点在邻接表中的row_id）
        start = time.time()
        self._update_node_edges(
            node_id=head,
            node_type=clean["head_type"],
            edge_type="out",
            action="add",
            edge_info={"row_id": tail_row_id, "rel": relation} if tail_row_id is not None else {"target_node": tail, "rel": relation}
        )
        times["adj_head"] = (time.time() - start) * 1000
        
        # 3. 更新 tail 节点的 in_edges（使用head节点在邻接表中的row_id）
        start = time.time()
        self._update_node_edges(
            node_id=tail,
            node_type=clean["tail_type"],
            edge_type="in",
            action="add",
            edge_info={"row_id": head_row_id, "rel": relation} if head_row_id is not None else {"source_node": head, "rel": relation}
        )
        times["adj_tail"] = (time.time() - start) * 1000
        
        times["total"] = times["triples"] + times["adj_head"] + times["adj_tail"]
        return times
    
    def delete(self, triple: dict) -> dict:
        """
        删除三元组
        
        步骤:
        1. 查找并删除三元组表中的记录
        2. 更新 head 节点的 out_edges（移除）
        3. 更新 tail 节点的 in_edges（移除）
        
        返回: {"total": ms, "triples": ms, "adj_head": ms, "adj_tail": ms}
        """
        times = {}
        head, tail, relation = triple["head"], triple["tail"], triple["relation"]
        
        # 1. 删除三元组表
        condition = f"head = '{head}' AND relation = '{relation}' AND tail = '{tail}'"
        start = time.time()
        self.triples_tbl.delete(condition)
        times["triples"] = (time.time() - start) * 1000
        
        # 获取tail节点在邻接表中的row_id
        tail_row_id = self._get_node_row_id(tail)
        
        # 2. 更新 head 节点的 out_edges（移除匹配项）
        start = time.time()
        if tail_row_id is not None:
            self._update_node_edges(
                node_id=head,
                node_type=None,
                edge_type="out",
                action="remove",
                edge_info={"rel": relation, "row_id": tail_row_id}
            )
        times["adj_head"] = (time.time() - start) * 1000
        
        # 获取head节点在邻接表中的row_id
        head_row_id = self._get_node_row_id(head)
        
        # 3. 更新 tail 节点的 in_edges（移除匹配项）
        start = time.time()
        if head_row_id is not None:
            self._update_node_edges(
                node_id=tail,
                node_type=None,
                edge_type="in",
                action="remove",
                edge_info={"rel": relation, "row_id": head_row_id}
            )
        times["adj_tail"] = (time.time() - start) * 1000
        
        times["total"] = times["triples"] + times["adj_head"] + times["adj_tail"]
        return times
    
    def update(self, triple: dict, new_relation: str) -> dict:
        """
        修改三元组的关系
        
        步骤:
        1. 更新三元组表的 relation
        2. 更新 head 的 out_edges 中对应项
        3. 更新 tail 的 in_edges 中对应项
        
        返回: {"total": ms, "triples": ms, "adj_head": ms, "adj_tail": ms}
        """
        times = {}
        head, tail, old_relation = triple["head"], triple["tail"], triple["relation"]
        
        # 1. 更新三元组表
        condition = f"head = '{head}' AND relation = '{old_relation}' AND tail = '{tail}'"
        start = time.time()
        self.triples_tbl.update(where=condition, values={"relation": new_relation})
        times["triples"] = (time.time() - start) * 1000
        
        # 获取tail节点在邻接表中的row_id
        tail_row_id = self._get_node_row_id(tail)
        
        # 2. 更新 head 的 out_edges
        start = time.time()
        if tail_row_id is not None:
            self._update_node_edges(
                node_id=head,
                node_type=None,
                edge_type="out",
                action="modify",
                edge_info={"row_id": tail_row_id, "new_relation": new_relation}
            )
        times["adj_head"] = (time.time() - start) * 1000
        
        # 获取head节点在邻接表中的row_id
        head_row_id = self._get_node_row_id(head)
        
        # 3. 更新 tail 的 in_edges
        start = time.time()
        if head_row_id is not None:
            self._update_node_edges(
                node_id=tail,
                node_type=None,
                edge_type="in",
                action="modify",
                edge_info={"row_id": head_row_id, "new_relation": new_relation}
            )
        times["adj_tail"] = (time.time() - start) * 1000
        
        times["total"] = times["triples"] + times["adj_head"] + times["adj_tail"]
        return times
    
    def _get_node_row_id(self, node_id: str) -> int:
        """获取节点在邻接表中的row_id"""
        result = self.adj_tbl.search().with_row_id(True).where(f"node_id = '{node_id}'").limit(1).to_pandas()
        if not result.empty:
            return int(result.iloc[0]["_rowid"])
        return None
    
    def _update_node_edges(self, node_id: str, node_type: str, edge_type: str, 
                           action: str, edge_info: dict):
        """
        更新节点的边列表
        
        Args:
            node_id: 节点 ID
            node_type: 节点类型（仅 add 时使用）
            edge_type: "out" 或 "in"
            action: "add", "remove", "modify"
            edge_info: 边信息（可能包含row_id或target_node/source_node）
        """
        # 查询当前节点
        result = self.adj_tbl.search().where(f"node_id = '{node_id}'").limit(1).to_pandas()
        
        if result.empty:
            # 节点不存在，创建新节点（仅 add 操作）
            if action == "add":
                # 如果edge_info包含target_node或source_node，需要先获取或创建目标/源节点
                final_edge_info = edge_info.copy()
                if "target_node" in edge_info:
                    # 查找或创建目标节点
                    target_node_id = edge_info["target_node"]
                    target_row_id = self._get_node_row_id(target_node_id)
                    if target_row_id is None:
                        # 目标节点不存在，需要先创建（这里简化处理，假设目标节点会在后续创建）
                        # 暂时使用target_node，后续需要重新处理
                        pass
                    else:
                        final_edge_info = {"row_id": target_row_id, "rel": edge_info.get("rel", edge_info.get("relation"))}
                elif "source_node" in edge_info:
                    # 查找或创建源节点
                    source_node_id = edge_info["source_node"]
                    source_row_id = self._get_node_row_id(source_node_id)
                    if source_row_id is not None:
                        final_edge_info = {"row_id": source_row_id, "rel": edge_info.get("rel", edge_info.get("relation"))}
                
                edges_key = "out_edges" if edge_type == "out" else "in_edges"
                degree_key = "out_degree" if edge_type == "out" else "in_degree"
                new_node = {
                    "node_id": node_id,
                    "node_type": node_type or "Unknown",
                    "out_degree": 1 if edge_type == "out" else 0,
                    "in_degree": 1 if edge_type == "in" else 0,
                    "out_edges": json.dumps([final_edge_info]) if edge_type == "out" else "[]",
                    "in_edges": json.dumps([final_edge_info]) if edge_type == "in" else "[]"
                }
                self.adj_tbl.add(pd.DataFrame([new_node]))
            return
        
        # 节点存在，更新边列表
        row = result.iloc[0]
        edges_key = "out_edges" if edge_type == "out" else "in_edges"
        degree_key = "out_degree" if edge_type == "out" else "in_degree"
        
        edges = json.loads(row[edges_key])
        
        if action == "add":
            # 如果edge_info包含target_node或source_node，需要转换为row_id
            final_edge_info = edge_info.copy()
            if "target_node" in edge_info:
                target_node_id = edge_info["target_node"]
                target_row_id = self._get_node_row_id(target_node_id)
                if target_row_id is not None:
                    final_edge_info = {"row_id": target_row_id, "rel": edge_info.get("rel", edge_info.get("relation"))}
            elif "source_node" in edge_info:
                source_node_id = edge_info["source_node"]
                source_row_id = self._get_node_row_id(source_node_id)
                if source_row_id is not None:
                    final_edge_info = {"row_id": source_row_id, "rel": edge_info.get("rel", edge_info.get("relation"))}
            edges.append(final_edge_info)
        elif action == "remove":
            # 移除匹配的边（使用rel和row_id匹配）
            if edge_type == "out":
                edges = [e for e in edges if not (e.get("rel") == edge_info.get("rel", edge_info.get("relation")) 
                                                  and e.get("row_id") == edge_info.get("row_id"))]
            else:
                edges = [e for e in edges if not (e.get("rel") == edge_info.get("rel", edge_info.get("relation")) 
                                                  and e.get("row_id") == edge_info.get("row_id"))]
        elif action == "modify":
            # 修改匹配边的 relation（使用row_id匹配）
            for e in edges:
                if e.get("row_id") == edge_info.get("row_id"):
                    e["rel"] = edge_info.get("new_relation", edge_info.get("new_rel"))
                    break
        
        # 更新邻接表
        new_degree = len(edges)
        self.adj_tbl.update(
            where=f"node_id = '{node_id}'",
            values={
                edges_key: json.dumps(edges),
                degree_key: new_degree
            }
        )


def test_scheme2_by_degree(analyzer: DataAnalyzer):
    """按关联度测试方案二的更新性能"""
    print("\n" + "=" * 70)
    print("方案二: LanceDB + 邻接索引 - 按关联度测试")
    print("=" * 70)
    
    # 分类三元组
    classified = analyzer.classify_triples()
    
    # 初始化
    scheme = Scheme2LanceDB(TEST_DB_PATH)
    
    results = {}
    
    for degree_type in ["isolated", "low", "medium", "high"]:
        samples = classified[degree_type]
        if len(samples) < TEST_COUNT_PER_GROUP:
            print(f"\n⚠️ {degree_type} 组样本不足 ({len(samples)} 条)，跳过")
            continue
        
        print(f"\n{'='*70}")
        print(f"关联度: {degree_type.upper()}")
        print(f"{'='*70}")
        
        # 随机选取测试样本
        test_samples = random.sample(samples, TEST_COUNT_PER_GROUP)
        
        # ========== 插入测试 ==========
        print(f"\n📥 插入测试")
        scheme.setup(analyzer.data)
        
        insert_times = {"total": [], "triples": [], "adj_head": [], "adj_tail": []}
        for i in range(TEST_COUNT_PER_GROUP):
            new_triple = analyzer.get_new_triple(i + degree_type.__hash__() % 1000)
            times = scheme.insert(new_triple)
            for k, v in times.items():
                insert_times[k].append(v)
        
        print(f"  插入 ({TEST_COUNT_PER_GROUP} 次):")
        print(f"    总耗时: {statistics.mean(insert_times['total']):.3f} ms")
        print(f"    - 三元组表: {statistics.mean(insert_times['triples']):.3f} ms")
        print(f"    - 邻接表(head): {statistics.mean(insert_times['adj_head']):.3f} ms")
        print(f"    - 邻接表(tail): {statistics.mean(insert_times['adj_tail']):.3f} ms")
        
        # ========== 删除测试 ==========
        print(f"\n🗑️ 删除测试")
        scheme.setup(analyzer.data)
        
        delete_times = {"total": [], "triples": [], "adj_head": [], "adj_tail": []}
        for triple in test_samples:
            times = scheme.delete(triple)
            for k, v in times.items():
                delete_times[k].append(v)
        
        print(f"  删除 ({TEST_COUNT_PER_GROUP} 次):")
        print(f"    总耗时: {statistics.mean(delete_times['total']):.3f} ms")
        print(f"    - 三元组表: {statistics.mean(delete_times['triples']):.3f} ms")
        print(f"    - 邻接表(head): {statistics.mean(delete_times['adj_head']):.3f} ms")
        print(f"    - 邻接表(tail): {statistics.mean(delete_times['adj_tail']):.3f} ms")
        
        # ========== 修改测试 ==========
        print(f"\n✏️ 修改测试")
        scheme.setup(analyzer.data)
        
        update_times = {"total": [], "triples": [], "adj_head": [], "adj_tail": []}
        for i, triple in enumerate(test_samples):
            new_rel = f"modified_{degree_type}_{i}"
            times = scheme.update(triple, new_rel)
            for k, v in times.items():
                update_times[k].append(v)
        
        print(f"  修改 ({TEST_COUNT_PER_GROUP} 次):")
        print(f"    总耗时: {statistics.mean(update_times['total']):.3f} ms")
        print(f"    - 三元组表: {statistics.mean(update_times['triples']):.3f} ms")
        print(f"    - 邻接表(head): {statistics.mean(update_times['adj_head']):.3f} ms")
        print(f"    - 邻接表(tail): {statistics.mean(update_times['adj_tail']):.3f} ms")
        
        # 保存结果
        results[degree_type] = {
            "insert": statistics.mean(insert_times["total"]),
            "delete": statistics.mean(delete_times["total"]),
            "update": statistics.mean(update_times["total"]),
        }
    
    # 打印汇总
    print("\n" + "=" * 70)
    print("📊 方案二测试结果汇总 (平均总耗时 ms)")
    print("=" * 70)
    print(f"{'关联度':<12} {'插入':<12} {'删除':<12} {'修改':<12}")
    print("-" * 48)
    for degree_type, times in results.items():
        print(f"{degree_type:<12} {times['insert']:<12.3f} {times['delete']:<12.3f} {times['update']:<12.3f}")
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
        from neo4j import GraphDatabase
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
    
    def setup(self, data: list):
        """初始化：导入基础数据"""
        print(f"\n[方案三] 初始化: {len(data):,} 条")
        
        # 清空数据库
        with self.driver.session(database=self.db) as session:
            session.run("MATCH (n) DETACH DELETE n")
        
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
            batch_clean = [{k: v for k, v in d.items() if k != "idx"} for d in batch]
            with self.driver.session(database=self.db) as session:
                session.run(query, batch=batch_clean)
        
        # 统计
        with self.driver.session(database=self.db) as session:
            node_count = session.run("MATCH (n) RETURN count(n) AS c").single()["c"]
            rel_count = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
        print(f"  节点: {node_count:,}, 关系: {rel_count:,}")
    
    def insert(self, triple: dict) -> float:
        """插入三元组"""
        query = """
        MERGE (head:Entity {id: $head})
        ON CREATE SET head.type = $head_type
        MERGE (tail:Entity {id: $tail})
        ON CREATE SET tail.type = $tail_type
        CREATE (head)-[r:REL {type: $relation}]->(tail)
        """
        start = time.time()
        with self.driver.session(database=self.db) as session:
            session.run(query, 
                       head=triple["head"], head_type=triple["head_type"],
                       tail=triple["tail"], tail_type=triple["tail_type"],
                       relation=triple["relation"])
        return (time.time() - start) * 1000
    
    def delete(self, triple: dict) -> float:
        """删除三元组（关系）"""
        query = """
        MATCH (head:Entity {id: $head})-[r:REL {type: $relation}]->(tail:Entity {id: $tail})
        DELETE r
        """
        start = time.time()
        with self.driver.session(database=self.db) as session:
            session.run(query,
                       head=triple["head"], tail=triple["tail"],
                       relation=triple["relation"])
        return (time.time() - start) * 1000
    
    def update(self, triple: dict, new_relation: str) -> float:
        """修改关系类型"""
        query = """
        MATCH (head:Entity {id: $head})-[r:REL {type: $old_relation}]->(tail:Entity {id: $tail})
        SET r.type = $new_relation
        """
        start = time.time()
        with self.driver.session(database=self.db) as session:
            session.run(query,
                       head=triple["head"], tail=triple["tail"],
                       old_relation=triple["relation"], new_relation=new_relation)
        return (time.time() - start) * 1000


def test_scheme3_by_degree(analyzer: DataAnalyzer):
    """按关联度测试方案三的更新性能"""
    print("\n" + "=" * 70)
    print("方案三: Neo4j - 按关联度测试")
    print("=" * 70)
    
    # 连接 Neo4j
    scheme = Scheme3Neo4j()
    if not scheme.connect():
        print("跳过方案三测试")
        return {}
    
    try:
        # 分类三元组
        classified = analyzer.classify_triples()
        
        results = {}
        
        for degree_type in ["isolated", "low", "medium", "high"]:
            samples = classified[degree_type]
            if len(samples) < TEST_COUNT_PER_GROUP:
                print(f"\n⚠️ {degree_type} 组样本不足 ({len(samples)} 条)，跳过")
                continue
            
            print(f"\n{'='*70}")
            print(f"关联度: {degree_type.upper()}")
            print(f"{'='*70}")
            
            # 随机选取测试样本
            test_samples = random.sample(samples, TEST_COUNT_PER_GROUP)
            
            # ========== 插入测试 ==========
            print(f"\n📥 插入测试")
            scheme.setup(analyzer.data)
            
            insert_times = []
            for i in range(TEST_COUNT_PER_GROUP):
                new_triple = analyzer.get_new_triple(i + degree_type.__hash__() % 1000)
                elapsed = scheme.insert(new_triple)
                insert_times.append(elapsed)
            print_stats("插入", insert_times)
            
            # ========== 删除测试 ==========
            print(f"\n🗑️ 删除测试")
            scheme.setup(analyzer.data)
            
            delete_times = []
            for triple in test_samples:
                elapsed = scheme.delete(triple)
                delete_times.append(elapsed)
            print_stats("删除", delete_times)
            
            # ========== 修改测试 ==========
            print(f"\n✏️ 修改测试")
            scheme.setup(analyzer.data)
            
            update_times = []
            for i, triple in enumerate(test_samples):
                new_rel = f"modified_{degree_type}_{i}"
                elapsed = scheme.update(triple, new_rel)
                update_times.append(elapsed)
            print_stats("修改", update_times)
            
            # 保存结果
            results[degree_type] = {
                "insert": statistics.mean(insert_times) if insert_times else 0,
                "delete": statistics.mean(delete_times) if delete_times else 0,
                "update": statistics.mean(update_times) if update_times else 0,
            }
        
        # 打印汇总
        print("\n" + "=" * 70)
        print("📊 方案三测试结果汇总 (平均耗时 ms)")
        print("=" * 70)
        print(f"{'关联度':<12} {'插入':<12} {'删除':<12} {'修改':<12}")
        print("-" * 48)
        for degree_type, times in results.items():
            print(f"{degree_type:<12} {times['insert']:<12.3f} {times['delete']:<12.3f} {times['update']:<12.3f}")
        print("=" * 70)
        
        return results
    
    finally:
        scheme.close()


# ==================== 主函数 ====================

def main():
    """运行更新性能测试"""
    print("\n")
    print("=" * 70)
    print("三元组更新性能测试 - 按关联度分组")
    print("=" * 70)
    print(f"数据源: {TRIPLES_TSV}")
    print(f"基础数据量: {BASELINE_ROW_COUNT:,} 条")
    print(f"每组测试次数: {TEST_COUNT_PER_GROUP}")
    print("=" * 70)
    
    # 检查数据文件
    if not os.path.exists(TRIPLES_TSV):
        print(f"❌ 数据文件不存在: {TRIPLES_TSV}")
        return
    
    # 加载并分析数据
    analyzer = DataAnalyzer(TRIPLES_TSV, BASELINE_ROW_COUNT)
    
    # 运行方案一测试
    results1 = test_scheme1_by_degree(analyzer)
    
    # 运行方案二测试
    results2 = test_scheme2_by_degree(analyzer)
    
    # 运行方案三测试
    results3 = test_scheme3_by_degree(analyzer)
    
    # 打印对比汇总
    print("\n" + "=" * 80)
    print("📊 三种方案对比汇总 (平均耗时 ms)")
    print("=" * 80)
    
    # 插入对比
    print("\n【插入操作】")
    print(f"{'关联度':<10} {'方案一':<12} {'方案二':<12} {'方案三':<12}")
    print("-" * 46)
    for deg in ["isolated", "low", "medium", "high"]:
        r1 = results1.get(deg, {}).get("insert", "-")
        r2 = results2.get(deg, {}).get("insert", "-")
        r3 = results3.get(deg, {}).get("insert", "-") if results3 else "-"
        r1_str = f"{r1:.2f}" if isinstance(r1, float) else r1
        r2_str = f"{r2:.2f}" if isinstance(r2, float) else r2
        r3_str = f"{r3:.2f}" if isinstance(r3, float) else r3
        print(f"{deg:<10} {r1_str:<12} {r2_str:<12} {r3_str:<12}")
    
    # 删除对比
    print("\n【删除操作】")
    print(f"{'关联度':<10} {'方案一':<12} {'方案二':<12} {'方案三':<12}")
    print("-" * 46)
    for deg in ["isolated", "low", "medium", "high"]:
        r1 = results1.get(deg, {}).get("delete", "-")
        r2 = results2.get(deg, {}).get("delete", "-")
        r3 = results3.get(deg, {}).get("delete", "-") if results3 else "-"
        r1_str = f"{r1:.2f}" if isinstance(r1, float) else r1
        r2_str = f"{r2:.2f}" if isinstance(r2, float) else r2
        r3_str = f"{r3:.2f}" if isinstance(r3, float) else r3
        print(f"{deg:<10} {r1_str:<12} {r2_str:<12} {r3_str:<12}")
    
    # 修改对比
    print("\n【修改操作】")
    print(f"{'关联度':<10} {'方案一':<12} {'方案二':<12} {'方案三':<12}")
    print("-" * 46)
    for deg in ["isolated", "low", "medium", "high"]:
        r1 = results1.get(deg, {}).get("update", "-")
        r2 = results2.get(deg, {}).get("update", "-")
        r3 = results3.get(deg, {}).get("update", "-") if results3 else "-"
        r1_str = f"{r1:.2f}" if isinstance(r1, float) else r1
        r2_str = f"{r2:.2f}" if isinstance(r2, float) else r2
        r3_str = f"{r3:.2f}" if isinstance(r3, float) else r3
        print(f"{deg:<10} {r1_str:<12} {r2_str:<12} {r3_str:<12}")
    
    print("=" * 80)
    print("\n✅ 测试完成")


if __name__ == "__main__":
    main()
