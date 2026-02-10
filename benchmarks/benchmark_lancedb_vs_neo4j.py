import os
import sys
import time
import statistics
import lancedb
from neo4j import GraphDatabase
from typing import List, Dict

# 添加项目根目录到 Python 路径，以便导入 query_lancedb
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
sys.path.insert(0, PROJECT_ROOT)

from query_lancedb import query_relation_count, connect_db

"""
对比 LanceDB 和 Neo4j 在查询实体关系时的性能。
"""

# LanceDB 配置
LANCEDB_PATH = os.path.join(PROJECT_ROOT, "storage", "lance")
TABLE_NAME_FULL = "triples"

# Neo4j 配置
NEO4J_URI = "bolt://localhost:7687"  # 单机实例使用 bolt://
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "b230b230"
NEO4J_DATABASE = "neo4j"


def query_lancedb(entity_id: str, relation: str, direction: str = "out") -> Dict:
    """
    使用 LanceDB 查询实体关系。
    
    Args:
        entity_id: 实体 ID
        relation: 关系类型
        direction: "out" 表示出边，"in" 表示入边
    
    Returns:
        包含结果和时间的字典
    """
    start_time = time.time()
    tbl = connect_db(TABLE_NAME_FULL)
    
    # 尝试使用过滤查询（如果支持）
    # 注意：LanceDB 的过滤功能可能有限，这里先尝试直接过滤
    try:
        if direction == "out":
            # 查询出边：head = entity_id
            df = tbl.search().where(f"head = '{entity_id}' AND relation = '{relation}'").to_pandas()
            results = df["tail"].tolist() if not df.empty else []
        elif direction == "in":
            # 查询入边：tail = entity_id
            df = tbl.search().where(f"tail = '{entity_id}' AND relation = '{relation}'").to_pandas()
            results = df["head"].tolist() if not df.empty else []
        else:
            results = []
    except:
        # 如果过滤查询失败，回退到全表加载方式
        df_all = tbl.to_pandas()
        
        results = []
        if direction == "out":
            # 查询出边：head = entity_id
            df = df_all[(df_all["head"] == entity_id) & (df_all["relation"] == relation)]
            results = df["tail"].tolist()
        elif direction == "in":
            # 查询入边：tail = entity_id
            df = df_all[(df_all["tail"] == entity_id) & (df_all["relation"] == relation)]
            results = df["head"].tolist()
    
    elapsed_time = time.time() - start_time
    
    return {
        "results": results,
        "count": len(results),
        "query_time_ms": elapsed_time * 1000,
        "query_time_sec": elapsed_time
    }


def query_neo4j(entity_id: str, relation: str, direction: str = "out") -> Dict:
    """
    使用 Neo4j 查询实体关系。
    
    Args:
        entity_id: 实体 ID
        relation: 关系类型
        direction: "out" 表示出边，"in" 表示入边
    
    Returns:
        包含结果和时间的字典
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    start_time = time.time()
    results = []
    
    try:
        with driver.session(database=NEO4J_DATABASE) as session:
            if direction == "out":
                # 查询出边：(entity)-[:REL {type: relation}]->(target)
                query = """
                MATCH (src:Entity {id: $entity_id})-[r:REL {type: $relation}]->(dst:Entity)
                RETURN dst.id AS result
                """
            else:  # direction == "in"
                # 查询入边：(target)-[:REL {type: relation}]->(entity)
                query = """
                MATCH (src:Entity)-[r:REL {type: $relation}]->(dst:Entity {id: $entity_id})
                RETURN src.id AS result
                """
            
            result = session.run(query, entity_id=entity_id, relation=relation)
            results = [record["result"] for record in result]
    
    finally:
        driver.close()
    
    elapsed_time = time.time() - start_time
    
    return {
        "results": results,
        "count": len(results),
        "query_time_ms": elapsed_time * 1000,
        "query_time_sec": elapsed_time
    }


def benchmark_single_query(
    entity_id: str,
    relation: str,
    direction: str = "out",
    num_runs: int = 10
) -> Dict:
    """
    对单个查询进行多次运行，取平均值。
    
    Args:
        entity_id: 实体 ID
        relation: 关系类型
        direction: "out" 或 "in"
        num_runs: 运行次数
    
    Returns:
        包含性能对比结果的字典
    """
    print(f"\n{'='*60}")
    print(f"性能对比测试")
    print(f"{'='*60}")
    print(f"实体 ID: {entity_id}")
    print(f"关系类型: {relation}")
    print(f"查询方向: {direction}")
    print(f"运行次数: {num_runs}")
    print(f"{'='*60}")
    
    # LanceDB 性能测试
    print(f"\n📊 LanceDB 性能测试...")
    lancedb_times = []
    lancedb_results = None
    
    for i in range(num_runs):
        result = query_lancedb(entity_id, relation, direction)
        lancedb_times.append(result["query_time_ms"])
        if i == 0:  # 保存第一次的结果用于验证
            lancedb_results = result["results"]
    
    lancedb_avg = statistics.mean(lancedb_times)
    lancedb_median = statistics.median(lancedb_times)
    lancedb_min = min(lancedb_times)
    lancedb_max = max(lancedb_times)
    lancedb_std = statistics.stdev(lancedb_times) if len(lancedb_times) > 1 else 0
    
    print(f"  平均时间: {lancedb_avg:.2f} ms")
    print(f"  中位数: {lancedb_median:.2f} ms")
    print(f"  最小值: {lancedb_min:.2f} ms")
    print(f"  最大值: {lancedb_max:.2f} ms")
    print(f"  标准差: {lancedb_std:.2f} ms")
    print(f"  结果数量: {len(lancedb_results)}")
    
    # Neo4j 性能测试
    print(f"\n📊 Neo4j 性能测试...")
    neo4j_times = []
    neo4j_results = None
    
    for i in range(num_runs):
        result = query_neo4j(entity_id, relation, direction)
        neo4j_times.append(result["query_time_ms"])
        if i == 0:  # 保存第一次的结果用于验证
            neo4j_results = set(result["results"])  # 使用 set 便于比较
    
    neo4j_avg = statistics.mean(neo4j_times)
    neo4j_median = statistics.median(neo4j_times)
    neo4j_min = min(neo4j_times)
    neo4j_max = max(neo4j_times)
    neo4j_std = statistics.stdev(neo4j_times) if len(neo4j_times) > 1 else 0
    
    print(f"  平均时间: {neo4j_avg:.2f} ms")
    print(f"  中位数: {neo4j_median:.2f} ms")
    print(f"  最小值: {neo4j_min:.2f} ms")
    print(f"  最大值: {neo4j_max:.2f} ms")
    print(f"  标准差: {neo4j_std:.2f} ms")
    print(f"  结果数量: {len(neo4j_results)}")
    
    # 验证结果一致性
    lancedb_results_set = set(lancedb_results)
    results_match = lancedb_results_set == neo4j_results
    
    print(f"\n{'='*60}")
    print(f"性能对比结果")
    print(f"{'='*60}")
    print(f"LanceDB 平均时间: {lancedb_avg:.2f} ms")
    print(f"Neo4j 平均时间: {neo4j_avg:.2f} ms")
    
    if lancedb_avg < neo4j_avg:
        speedup = neo4j_avg / lancedb_avg
        print(f"✅ LanceDB 更快，快 {speedup:.2f}x")
    else:
        speedup = lancedb_avg / neo4j_avg
        print(f"✅ Neo4j 更快，快 {speedup:.2f}x")
    
    print(f"\n结果一致性: {'✅ 匹配' if results_match else '❌ 不匹配'}")
    if not results_match:
        print(f"  LanceDB 结果数: {len(lancedb_results_set)}")
        print(f"  Neo4j 结果数: {len(neo4j_results)}")
        only_in_lancedb = lancedb_results_set - neo4j_results
        only_in_neo4j = neo4j_results - lancedb_results_set
        if only_in_lancedb:
            print(f"  仅在 LanceDB 中: {list(only_in_lancedb)[:5]}...")
        if only_in_neo4j:
            print(f"  仅在 Neo4j 中: {list(only_in_neo4j)[:5]}...")
    
    print(f"{'='*60}")
    
    return {
        "entity_id": entity_id,
        "relation": relation,
        "direction": direction,
        "lancedb": {
            "avg_ms": lancedb_avg,
            "median_ms": lancedb_median,
            "min_ms": lancedb_min,
            "max_ms": lancedb_max,
            "std_ms": lancedb_std,
            "count": len(lancedb_results),
            "all_times": lancedb_times
        },
        "neo4j": {
            "avg_ms": neo4j_avg,
            "median_ms": neo4j_median,
            "min_ms": neo4j_min,
            "max_ms": neo4j_max,
            "std_ms": neo4j_std,
            "count": len(neo4j_results),
            "all_times": neo4j_times
        },
        "results_match": results_match,
        "speedup": speedup if lancedb_avg < neo4j_avg else 1/speedup,
        "faster": "lancedb" if lancedb_avg < neo4j_avg else "neo4j"
    }


def benchmark_multiple_queries(
    queries: List[Dict],
    num_runs: int = 10
) -> List[Dict]:
    """
    对多个查询进行性能对比。
    
    Args:
        queries: 查询列表，每个查询包含 entity_id, relation, direction
        num_runs: 每个查询的运行次数
    
    Returns:
        性能对比结果列表
    """
    results = []
    
    # 按类别分组统计
    category_stats = {}
    
    for i, query in enumerate(queries, 1):
        category = query.get("category", "unknown")
        category_num = category_stats.get(category, {}).get("count", 0) + 1
        category_stats[category] = category_stats.get(category, {"count": 0})
        category_stats[category]["count"] = category_num
        
        print(f"\n\n{'#'*60}")
        print(f"查询 {i}/{len(queries)} [{category}] - 样例 {category_num}/5")
        print(f"{'#'*60}")
        
        result = benchmark_single_query(
            query["entity_id"],
            query["relation"],
            query.get("direction", "out"),
            num_runs
        )
        result["category"] = category
        results.append(result)
    
    # 汇总统计
    print(f"\n\n{'='*60}")
    print(f"汇总统计")
    print(f"{'='*60}")
    
    lancedb_wins = sum(1 for r in results if r["faster"] == "lancedb")
    neo4j_wins = sum(1 for r in results if r["faster"] == "neo4j")
    
    lancedb_avg_all = statistics.mean([r["lancedb"]["avg_ms"] for r in results])
    neo4j_avg_all = statistics.mean([r["neo4j"]["avg_ms"] for r in results])
    
    print(f"总查询数: {len(results)}")
    print(f"LanceDB 更快: {lancedb_wins} 次")
    print(f"Neo4j 更快: {neo4j_wins} 次")
    print(f"\nLanceDB 平均时间: {lancedb_avg_all:.2f} ms")
    print(f"Neo4j 平均时间: {neo4j_avg_all:.2f} ms")
    
    if lancedb_avg_all < neo4j_avg_all:
        overall_speedup = neo4j_avg_all / lancedb_avg_all
        print(f"\n✅ 总体而言，LanceDB 更快，快 {overall_speedup:.2f}x")
    else:
        overall_speedup = lancedb_avg_all / neo4j_avg_all
        print(f"\n✅ 总体而言，Neo4j 更快，快 {overall_speedup:.2f}x")
    
    # 按类别统计
    print(f"\n{'='*60}")
    print(f"按查询类型统计")
    print(f"{'='*60}")
    
    categories = {}
    for r in results:
        cat = r["category"]
        if cat not in categories:
            categories[cat] = {
                "lancedb_times": [],
                "neo4j_times": [],
                "count": 0
            }
        categories[cat]["lancedb_times"].append(r["lancedb"]["avg_ms"])
        categories[cat]["neo4j_times"].append(r["neo4j"]["avg_ms"])
        categories[cat]["count"] += 1
    
    for cat, stats in sorted(categories.items()):
        lancedb_avg = statistics.mean(stats["lancedb_times"])
        neo4j_avg = statistics.mean(stats["neo4j_times"])
        speedup = lancedb_avg / neo4j_avg if neo4j_avg > 0 else 0
        print(f"\n{cat}:")
        print(f"  查询数: {stats['count']}")
        print(f"  LanceDB 平均: {lancedb_avg:.2f} ms")
        print(f"  Neo4j 平均: {neo4j_avg:.2f} ms")
        print(f"  Neo4j 快 {speedup:.2f}x")
    
    print(f"{'='*60}")
    
    return results


def main():
    """主函数：运行性能对比测试"""
    print("LanceDB vs Neo4j 性能对比测试")
    print("=" * 60)
    
    # 测试查询列表 - 每种查询类型5个样例
    test_queries = []
    
    # 1. space_use_model (出边) - space 使用哪些 model (5个样例)
    test_queries.extend([
        {
            "entity_id": "space:featherless-ai/try-this-model",
            "relation": "space_use_model",
            "direction": "out",
            "category": "space_use_model_out"
        },
        {
            "entity_id": "space:JackHoltone/try-this-model",
            "relation": "space_use_model",
            "direction": "out",
            "category": "space_use_model_out"
        },
        {
            "entity_id": "space:SC999/NV_Nemotron",
            "relation": "space_use_model",
            "direction": "out",
            "category": "space_use_model_out"
        },
        {
            "entity_id": "space:Granther/try-this-model",
            "relation": "space_use_model",
            "direction": "out",
            "category": "space_use_model_out"
        },
        {
            "entity_id": "space:emekaboris/try-this-model",
            "relation": "space_use_model",
            "direction": "out",
            "category": "space_use_model_out"
        },
    ])
    
    # 2. space_use_model (入边) - 哪些 space 使用了某个 model (5个样例)
    test_queries.extend([
        {
            "entity_id": "model:HuggingFaceH4/zephyr-7b-beta",
            "relation": "space_use_model",
            "direction": "in",
            "category": "space_use_model_in"
        },
        {
            "entity_id": "model:openai-community/gpt2",
            "relation": "space_use_model",
            "direction": "in",
            "category": "space_use_model_in"
        },
        {
            "entity_id": "model:stabilityai/sdxl-turbo",
            "relation": "space_use_model",
            "direction": "in",
            "category": "space_use_model_in"
        },
        {
            "entity_id": "model:front/assets",
            "relation": "space_use_model",
            "direction": "in",
            "category": "space_use_model_in"
        },
        {
            "entity_id": "model:stabilityai/stable-diffusion-xl-base-1.0",
            "relation": "space_use_model",
            "direction": "in",
            "category": "space_use_model_in"
        },
    ])
    
    # 3. user_like_model (出边) - user 喜欢哪些 model (5个样例)
    test_queries.extend([
        {
            "entity_id": "user:John6666",
            "relation": "user_like_model",
            "direction": "out",
            "category": "user_like_model_out"
        },
        {
            "entity_id": "user:julie555",
            "relation": "user_like_model",
            "direction": "out",
            "category": "user_like_model_out"
        },
        {
            "entity_id": "user:aaddxxz",
            "relation": "user_like_model",
            "direction": "out",
            "category": "user_like_model_out"
        },
        {
            "entity_id": "user:NikolayKozloff",
            "relation": "user_like_model",
            "direction": "out",
            "category": "user_like_model_out"
        },
        {
            "entity_id": "user:DevClouds",
            "relation": "user_like_model",
            "direction": "out",
            "category": "user_like_model_out"
        },
    ])
    
    # 4. user_like_space (入边) - 哪些 user 喜欢某个 space (5个样例)
    test_queries.extend([
        {
            "entity_id": "space:open-llm-leaderboard/open_llm_leaderboard",
            "relation": "user_like_space",
            "direction": "in",
            "category": "user_like_space_in"
        },
        {
            "entity_id": "space:stabilityai/stable-diffusion",
            "relation": "user_like_space",
            "direction": "in",
            "category": "user_like_space_in"
        },
        {
            "entity_id": "space:jbilcke-hf/ai-comic-factory",
            "relation": "user_like_space",
            "direction": "in",
            "category": "user_like_space_in"
        },
        {
            "entity_id": "space:Kwai-Kolors/Kolors-Virtual-Try-On",
            "relation": "user_like_space",
            "direction": "in",
            "category": "user_like_space_in"
        },
        {
            "entity_id": "space:black-forest-labs/FLUX.1-dev",
            "relation": "user_like_space",
            "direction": "in",
            "category": "user_like_space_in"
        },
    ])
    
    # 5. model_definedFor_task (出边) - model 定义的任务 (5个样例)
    test_queries.extend([
        {
            "entity_id": "model:Cyber-BCat/ControlNet-Preprocessors_Annotators",
            "relation": "model_definedFor_task",
            "direction": "out",
            "category": "model_definedFor_task_out"
        },
        {
            "entity_id": "model:language-ml-lab/iranian-azerbaijani-nlp",
            "relation": "model_definedFor_task",
            "direction": "out",
            "category": "model_definedFor_task_out"
        },
        {
            "entity_id": "model:dimfeld/BioLinkBERT-large-feat",
            "relation": "model_definedFor_task",
            "direction": "out",
            "category": "model_definedFor_task_out"
        },
        {
            "entity_id": "model:Hatto/Vietnamese-FlanT5-Large",
            "relation": "model_definedFor_task",
            "direction": "out",
            "category": "model_definedFor_task_out"
        },
        {
            "entity_id": "model:michiyasunaga/BioLinkBERT-base",
            "relation": "model_definedFor_task",
            "direction": "out",
            "category": "model_definedFor_task_out"
        },
    ])
    
    # 6. user_publish_model (出边) - user 发布的 model (5个样例)
    test_queries.extend([
        {
            "entity_id": "user:mradermacher",
            "relation": "user_publish_model",
            "direction": "out",
            "category": "user_publish_model_out"
        },
        {
            "entity_id": "user:Krabat",
            "relation": "user_publish_model",
            "direction": "out",
            "category": "user_publish_model_out"
        },
        {
            "entity_id": "user:xueyj",
            "relation": "user_publish_model",
            "direction": "out",
            "category": "user_publish_model_out"
        },
        {
            "entity_id": "user:RichardErkhov",
            "relation": "user_publish_model",
            "direction": "out",
            "category": "user_publish_model_out"
        },
        {
            "entity_id": "user:SALUTEASD",
            "relation": "user_publish_model",
            "direction": "out",
            "category": "user_publish_model_out"
        },
    ])
    
    print(f"总查询数: {len(test_queries)} (6种类型 × 5个样例)")
    print("=" * 60)
    
    # 运行性能对比（每个查询运行10次取平均值）
    results = benchmark_multiple_queries(test_queries, num_runs=10)
    
    # 详细结果表格
    print(f"\n\n{'='*60}")
    print(f"详细结果表格")
    print(f"{'='*60}")
    print(f"{'类别':<25} {'实体ID':<35} {'LanceDB(ms)':<15} {'Neo4j(ms)':<15} {'更快':<10} {'结果数':<10}")
    print(f"{'-'*120}")
    
    # 按类别分组显示
    current_category = None
    for r in results:
        cat = r['category']
        if cat != current_category:
            if current_category is not None:
                print()  # 类别之间空行
            current_category = cat
            print(f"\n【{cat}】")
        
        entity_short = r['entity_id'][:30] + "..." if len(r['entity_id']) > 30 else r['entity_id']
        lancedb_time = f"{r['lancedb']['avg_ms']:.2f}"
        neo4j_time = f"{r['neo4j']['avg_ms']:.2f}"
        faster = r['faster'].upper()
        result_count = r['lancedb']['count']
        print(f"  {entity_short:<35} {lancedb_time:<15} {neo4j_time:<15} {faster:<10} {result_count:<10}")


if __name__ == "__main__":
    main()

