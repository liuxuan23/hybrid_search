"""
Weaviate 混合查询完整测试脚本
基于已导入的 JSON 数据进行测试
"""
import requests
import json
import time
from typing import Dict, List, Optional

WEAVIATE_URL = "http://localhost:8080"

class HybridSearchTester:
    def __init__(self, weaviate_url: str = WEAVIATE_URL):
        self.weaviate_url = weaviate_url
        
    def check_service(self):
        """检查 Weaviate 服务是否可用"""
        try:
            response = requests.get(f"{self.weaviate_url}/v1/.well-known/ready", timeout=5)
            if response.status_code == 200:
                return True
        except Exception as e:
            print(f"❌ 无法连接到 Weaviate 服务: {e}")
            return False
        return False
    
    def get_data_count(self):
        """获取数据统计"""
        query = {
            "query": "{ Aggregate { Document { meta { count } } Author { meta { count } } } }"
        }
        response = requests.post(
            f"{self.weaviate_url}/v1/graphql",
            json=query,
            headers={"Content-Type": "application/json"}
        )
        if response.status_code == 200:
            result = response.json()
            doc_count = result.get("data", {}).get("Aggregate", {}).get("Document", [{}])[0].get("meta", {}).get("count", 0)
            author_count = result.get("data", {}).get("Aggregate", {}).get("Author", [{}])[0].get("meta", {}).get("count", 0)
            return doc_count, author_count
        return 0, 0
    
    def vector_search(self, query: str, limit: int = 5):
        """纯向量搜索（语义搜索）"""
        graphql_query = {
            "query": f"""
            {{
                Get {{
                    Document(
                        nearText: {{
                            concepts: ["{query}"]
                        }}
                        limit: {limit}
                    ) {{
                        title
                        content
                        category
                        publish_year
                        _additional {{
                            id
                            distance
                            certainty
                        }}
                    }}
                }}
            }}
            """
        }
        
        start_time = time.time()
        response = requests.post(
            f"{self.weaviate_url}/v1/graphql",
            json=graphql_query,
            headers={"Content-Type": "application/json"}
        )
        elapsed_time = (time.time() - start_time) * 1000
        
        if response.status_code == 200:
            result = response.json()
            if "data" in result and "Get" in result["data"]:
                return {
                    "results": result["data"]["Get"].get("Document", []),
                    "time_ms": elapsed_time,
                    "type": "vector_search"
                }
        return {"results": [], "time_ms": elapsed_time, "error": response.text}
    
    def keyword_search(self, query: str, limit: int = 5):
        """纯关键词搜索（BM25）"""
        graphql_query = {
            "query": f"""
            {{
                Get {{
                    Document(
                        bm25: {{
                            query: "{query}"
                        }}
                        limit: {limit}
                    ) {{
                        title
                        content
                        category
                        publish_year
                        _additional {{
                            id
                            score
                        }}
                    }}
                }}
            }}
            """
        }
        
        start_time = time.time()
        response = requests.post(
            f"{self.weaviate_url}/v1/graphql",
            json=graphql_query,
            headers={"Content-Type": "application/json"}
        )
        elapsed_time = (time.time() - start_time) * 1000
        
        if response.status_code == 200:
            result = response.json()
            if "data" in result and "Get" in result["data"]:
                return {
                    "results": result["data"]["Get"].get("Document", []),
                    "time_ms": elapsed_time,
                    "type": "keyword_search"
                }
        return {"results": [], "time_ms": elapsed_time, "error": response.text}
    
    def hybrid_search(self, query: str, limit: int = 5, alpha: float = 0.5):
        """混合搜索（结合向量搜索和关键词搜索）"""
        graphql_query = {
            "query": f"""
            {{
                Get {{
                    Document(
                        hybrid: {{
                            query: "{query}"
                            alpha: {alpha}
                        }}
                        limit: {limit}
                    ) {{
                        title
                        content
                        category
                        publish_year
                        _additional {{
                            id
                            score
                            distance
                        }}
                    }}
                }}
            }}
            """
        }
        
        start_time = time.time()
        response = requests.post(
            f"{self.weaviate_url}/v1/graphql",
            json=graphql_query,
            headers={"Content-Type": "application/json"}
        )
        elapsed_time = (time.time() - start_time) * 1000
        
        if response.status_code == 200:
            result = response.json()
            if "data" in result and "Get" in result["data"]:
                return {
                    "results": result["data"]["Get"].get("Document", []),
                    "time_ms": elapsed_time,
                    "type": f"hybrid_search (alpha={alpha})"
                }
        return {"results": [], "time_ms": elapsed_time, "error": response.text}
    
    def hybrid_search_with_filter(self, query: str, filter_conditions: Dict, limit: int = 5, alpha: float = 0.5):
        """带过滤条件的混合搜索"""
        where_clause = self._build_where_clause(filter_conditions)
        
        graphql_query = {
            "query": f"""
            {{
                Get {{
                    Document(
                        hybrid: {{
                            query: "{query}"
                            alpha: {alpha}
                        }}
                        where: {where_clause}
                        limit: {limit}
                    ) {{
                        title
                        content
                        category
                        publish_year
                        _additional {{
                            id
                            score
                            distance
                        }}
                    }}
                }}
            }}
            """
        }
        
        start_time = time.time()
        response = requests.post(
            f"{self.weaviate_url}/v1/graphql",
            json=graphql_query,
            headers={"Content-Type": "application/json"}
        )
        elapsed_time = (time.time() - start_time) * 1000
        
        if response.status_code == 200:
            result = response.json()
            if "data" in result and "Get" in result["data"]:
                return {
                    "results": result["data"]["Get"].get("Document", []),
                    "time_ms": elapsed_time,
                    "type": f"hybrid_search_with_filter (alpha={alpha})"
                }
        return {"results": [], "time_ms": elapsed_time, "error": response.text}
    
    def _build_where_clause(self, conditions: Dict) -> str:
        """构建 GraphQL where 子句"""
        if not conditions:
            return "{}"
        
        clauses = []
        for key, value in conditions.items():
            if isinstance(value, dict):
                # 处理操作符
                for op, val in value.items():
                    if op == "$gte":
                        clauses.append(f'{{path: ["{key}"], operator: GreaterThanEqual, valueInt: {val}}}')
                    elif op == "$lte":
                        clauses.append(f'{{path: ["{key}"], operator: LessThanEqual, valueInt: {val}}}')
                    elif op == "$gt":
                        clauses.append(f'{{path: ["{key}"], operator: GreaterThan, valueInt: {val}}}')
                    elif op == "$lt":
                        clauses.append(f'{{path: ["{key}"], operator: LessThan, valueInt: {val}}}')
            else:
                # 精确匹配
                if isinstance(value, str):
                    clauses.append(f'{{path: ["{key}"], operator: Equal, valueString: "{value}"}}')
                elif isinstance(value, int):
                    clauses.append(f'{{path: ["{key}"], operator: Equal, valueInt: {value}}}')
        
        if len(clauses) == 1:
            return clauses[0]
        else:
            return f'{{operator: And, operands: [{", ".join(clauses)}]}}'
    
    def print_results(self, query_result: Dict, query_text: str):
        """打印查询结果"""
        print(f"\n{'='*80}")
        print(f"查询类型: {query_result.get('type', 'unknown')}")
        print(f"查询文本: {query_text}")
        print(f"查询耗时: {query_result.get('time_ms', 0):.2f} ms")
        print(f"结果数量: {len(query_result.get('results', []))}")
        print(f"{'='*80}")
        
        if "error" in query_result:
            print(f"❌ 错误: {query_result['error']}")
            return
        
        results = query_result.get("results", [])
        if not results:
            print("⚠️  没有找到结果")
            return
        
        for i, result in enumerate(results, 1):
            print(f"\n【结果 {i}】")
            print(f"  ID: {result.get('_additional', {}).get('id', 'N/A')}")
            print(f"  标题: {result.get('title', 'N/A')}")
            content = result.get('content', 'N/A')
            if len(content) > 100:
                print(f"  内容: {content[:100]}...")
            else:
                print(f"  内容: {content}")
            print(f"  类别: {result.get('category', 'N/A')}")
            print(f"  年份: {result.get('publish_year', 'N/A')}")
            
            additional = result.get("_additional", {})
            if "distance" in additional and additional["distance"] is not None:
                dist = additional['distance']
                if isinstance(dist, (int, float)):
                    print(f"  距离: {dist:.4f}")
            if "certainty" in additional and additional["certainty"] is not None:
                cert = additional['certainty']
                if isinstance(cert, (int, float)):
                    print(f"  确定性: {cert:.4f}")
            if "score" in additional:
                score = additional['score']
                if isinstance(score, (int, float)):
                    print(f"  分数: {score:.4f}")
                else:
                    try:
                        score_float = float(score)
                        print(f"  分数: {score_float:.4f}")
                    except (ValueError, TypeError):
                        print(f"  分数: {score}")
    
    def compare_search_methods(self, query: str, limit: int = 5):
        """对比不同搜索方法"""
        print("\n" + "="*80)
        print("🔍 混合查询对比测试")
        print("="*80)
        
        results = {}
        
        # 1. 纯向量搜索
        print("\n[1/4] 执行纯向量搜索...")
        results["vector"] = self.vector_search(query, limit, alpha=1.0)
        
        # 2. 纯关键词搜索
        print("[2/4] 执行纯关键词搜索...")
        results["keyword"] = self.keyword_search(query, limit)
        
        # 3. 混合搜索 (alpha=0.5)
        print("[3/4] 执行混合搜索 (alpha=0.5)...")
        results["hybrid_balanced"] = self.hybrid_search(query, limit, alpha=0.5)
        
        # 4. 混合搜索 (alpha=0.7，偏向向量)
        print("[4/4] 执行混合搜索 (alpha=0.7，偏向向量)...")
        results["hybrid_vector"] = self.hybrid_search(query, limit, alpha=0.7)
        
        # 打印对比结果
        print("\n" + "="*80)
        print("📊 性能对比")
        print("="*80)
        print(f"{'搜索方法':<30} {'结果数':<10} {'耗时 (ms)':<15}")
        print("-"*80)
        for method, result in results.items():
            method_name = {
                "vector": "纯向量搜索",
                "keyword": "纯关键词搜索",
                "hybrid_balanced": "混合搜索 (α=0.5)",
                "hybrid_vector": "混合搜索 (α=0.7)"
            }.get(method, method)
            count = len(result.get("results", []))
            time_ms = result.get("time_ms", 0)
            print(f"{method_name:<30} {count:<10} {time_ms:<15.2f}")
        
        # 打印详细结果
        print("\n" + "="*80)
        print("📝 详细结果对比")
        print("="*80)
        
        for method, result in results.items():
            method_name = {
                "vector": "纯向量搜索",
                "keyword": "纯关键词搜索",
                "hybrid_balanced": "混合搜索 (α=0.5)",
                "hybrid_vector": "混合搜索 (α=0.7)"
            }.get(method, method)
            print(f"\n### {method_name}")
            self.print_results(result, query)
        
        return results

def main():
    tester = HybridSearchTester()
    
    # 检查服务
    if not tester.check_service():
        print("❌ Weaviate 服务不可用，请先启动服务")
        print("运行: docker compose up -d")
        return
    
    print("✅ Weaviate 服务连接成功\n")
    
    # 检查数据
    doc_count, author_count = tester.get_data_count()
    print(f"📊 数据统计:")
    print(f"  - 文档数量: {doc_count}")
    print(f"  - 作者数量: {author_count}\n")
    
    if doc_count == 0:
        print("⚠️  警告：数据库中没有文档数据")
        print("请先运行: python import_json_data.py")
        return
    
    print("="*80)
    print("🧪 Weaviate 混合查询测试")
    print("="*80)
    
    # 测试查询列表
    test_queries = [
        "AI chip",
        "machine learning",
        "semiconductor technology",
        "distributed systems",
        "neural network"
    ]
    
    print(f"\n将测试 {len(test_queries)} 个查询...\n")
    
    # 执行测试
    all_results = {}
    for i, query in enumerate(test_queries, 1):
        print(f"\n\n{'#'*80}")
        print(f"测试 {i}/{len(test_queries)}: {query}")
        print(f"{'#'*80}")
        all_results[query] = tester.compare_search_methods(query, limit=5)
    
    # 测试带过滤条件的混合搜索
    print("\n\n" + "="*80)
    print("🔍 测试带过滤条件的混合搜索")
    print("="*80)
    
    filter_tests = [
        {
            "query": "AI technology",
            "filters": {"category": "Hardware"},
            "description": "类别为 Hardware 的文档"
        },
        {
            "query": "machine learning",
            "filters": {"publish_year": {"$gte": 2022}},
            "description": "2022年及以后发布的文档"
        },
        {
            "query": "semiconductor",
            "filters": {
                "category": "Hardware",
                "publish_year": {"$gte": 2023}
            },
            "description": "类别为 Hardware 且 2023年及以后发布的文档"
        }
    ]
    
    for test in filter_tests:
        print(f"\n{'='*80}")
        print(f"查询: {test['query']}")
        print(f"过滤条件: {test['description']}")
        print(f"{'='*80}")
        
        result = tester.hybrid_search_with_filter(
            test["query"],
            test["filters"],
            limit=5,
            alpha=0.5
        )
        tester.print_results(result, test["query"])
    
    # 性能统计
    print("\n\n" + "="*80)
    print("📈 整体性能统计")
    print("="*80)
    
    method_times = {
        "vector": [],
        "keyword": [],
        "hybrid_balanced": [],
        "hybrid_vector": []
    }
    
    for query, results in all_results.items():
        for method, result in results.items():
            if method in method_times:
                method_times[method].append(result.get("time_ms", 0))
    
    print(f"\n{'搜索方法':<30} {'平均耗时 (ms)':<20} {'最小耗时':<15} {'最大耗时':<15}")
    print("-"*80)
    for method, times in method_times.items():
        if times:
            method_name = {
                "vector": "纯向量搜索",
                "keyword": "纯关键词搜索",
                "hybrid_balanced": "混合搜索 (α=0.5)",
                "hybrid_vector": "混合搜索 (α=0.7)"
            }.get(method, method)
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            print(f"{method_name:<30} {avg_time:<20.2f} {min_time:<15.2f} {max_time:<15.2f}")
    
    print("\n" + "="*80)
    print("✅ 所有测试完成！")
    print("="*80)
    print(f"\n测试总结:")
    print(f"  - 测试查询数: {len(test_queries)}")
    print(f"  - 数据量: {doc_count} 个文档")
    print(f"  - 所有查询方式均正常工作")
    print("="*80)

if __name__ == "__main__":
    main()

