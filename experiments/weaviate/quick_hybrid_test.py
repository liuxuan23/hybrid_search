"""
快速混合查询测试 - 使用少量测试数据
"""
import requests
import json
import time

WEAVIATE_URL = "http://localhost:8080"

# 测试数据
test_documents = [
    {
        "title": "AI Chip Development",
        "content": "Advanced AI chip technology for machine learning applications in semiconductor industry.",
        "category": "Hardware",
        "publish_year": 2024
    },
    {
        "title": "Machine Learning Algorithms",
        "content": "Deep learning and neural network algorithms for distributed systems.",
        "category": "Software",
        "publish_year": 2023
    },
    {
        "title": "Semiconductor Manufacturing",
        "content": "Latest developments in semiconductor technology and chip production.",
        "category": "Hardware",
        "publish_year": 2024
    },
    {
        "title": "Distributed Computing Systems",
        "content": "Cloud computing and distributed systems architecture for AI workloads.",
        "category": "Software",
        "publish_year": 2023
    },
    {
        "title": "Neural Network Hardware",
        "content": "Specialized hardware accelerators for neural network inference and training.",
        "category": "Hardware",
        "publish_year": 2024
    }
]

def import_test_data():
    """导入测试数据"""
    print("正在导入测试数据...")
    url = f"{WEAVIATE_URL}/v1/batch/objects"
    
    objects = []
    for doc in test_documents:
        objects.append({
            "class": "Document",
            "properties": {
                "title": doc["title"],
                "content": doc["content"],
                "category": doc["category"],
                "publish_year": doc["publish_year"],
                "image_url": f"https://example.com/{doc['title'].lower().replace(' ', '_')}.jpg"
            }
        })
    
    response = requests.post(url, json={"objects": objects})
    if response.status_code == 200:
        print(f"✅ 成功导入 {len(test_documents)} 个测试文档")
        # 等待向量化完成
        print("等待向量索引构建...")
        time.sleep(5)
        return True
    else:
        print(f"❌ 导入失败: {response.status_code} - {response.text[:200]}")
        return False

def test_search(query_text, search_type, **kwargs):
    """执行搜索测试"""
    if search_type == "vector":
        query = f"""
        {{
            Get {{
                Document(
                    nearText: {{
                        concepts: ["{query_text}"]
                    }}
                    limit: 5
                ) {{
                    title
                    content
                    category
                    _additional {{
                        id
                        distance
                        certainty
                    }}
                }}
            }}
        }}
        """
    elif search_type == "keyword":
        query = f"""
        {{
            Get {{
                Document(
                    bm25: {{
                        query: "{query_text}"
                    }}
                    limit: 5
                ) {{
                    title
                    content
                    category
                    _additional {{
                        id
                        score
                    }}
                }}
            }}
        }}
        """
    elif search_type == "hybrid":
        alpha = kwargs.get("alpha", 0.5)
        query = f"""
        {{
            Get {{
                Document(
                    hybrid: {{
                        query: "{query_text}"
                        alpha: {alpha}
                    }}
                    limit: 5
                ) {{
                    title
                    content
                    category
                    _additional {{
                        id
                        score
                        distance
                    }}
                }}
            }}
        }}
        """
    else:
        return None
    
    start_time = time.time()
    response = requests.post(
        f"{WEAVIATE_URL}/v1/graphql",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    elapsed = (time.time() - start_time) * 1000
    
    if response.status_code == 200:
        result = response.json()
        if "data" in result and "Get" in result["data"]:
            return {
                "results": result["data"]["Get"].get("Document", []),
                "time_ms": elapsed
            }
    
    return {"results": [], "time_ms": elapsed, "error": response.text}

def main():
    print("="*80)
    print("🧪 Weaviate 混合查询快速测试")
    print("="*80)
    
    # 导入测试数据
    if not import_test_data():
        return
    
    # 测试查询
    test_queries = [
        "AI chip",
        "machine learning",
        "semiconductor",
        "distributed systems"
    ]
    
    for query_text in test_queries:
        print(f"\n{'='*80}")
        print(f"查询: {query_text}")
        print(f"{'='*80}")
        
        # 向量搜索
        print("\n[1] 纯向量搜索:")
        result = test_search(query_text, "vector")
        if result:
            print(f"  耗时: {result['time_ms']:.2f} ms")
            print(f"  结果数: {len(result.get('results', []))}")
            for i, r in enumerate(result.get('results', [])[:3], 1):
                print(f"    {i}. {r.get('title', 'N/A')} (类别: {r.get('category', 'N/A')})")
        
        # 关键词搜索
        print("\n[2] 纯关键词搜索:")
        result = test_search(query_text, "keyword")
        if result:
            print(f"  耗时: {result['time_ms']:.2f} ms")
            print(f"  结果数: {len(result.get('results', []))}")
            for i, r in enumerate(result.get('results', [])[:3], 1):
                print(f"    {i}. {r.get('title', 'N/A')} (类别: {r.get('category', 'N/A')})")
        
        # 混合搜索
        print("\n[3] 混合搜索 (alpha=0.5):")
        result = test_search(query_text, "hybrid", alpha=0.5)
        if result:
            print(f"  耗时: {result['time_ms']:.2f} ms")
            print(f"  结果数: {len(result.get('results', []))}")
            for i, r in enumerate(result.get('results', [])[:3], 1):
                score = r.get('_additional', {}).get('score', 'N/A')
                print(f"    {i}. {r.get('title', 'N/A')} (分数: {score}, 类别: {r.get('category', 'N/A')})")
        
        # 混合搜索 (偏向向量)
        print("\n[4] 混合搜索 (alpha=0.7, 偏向向量):")
        result = test_search(query_text, "hybrid", alpha=0.7)
        if result:
            print(f"  耗时: {result['time_ms']:.2f} ms")
            print(f"  结果数: {len(result.get('results', []))}")
            for i, r in enumerate(result.get('results', [])[:3], 1):
                score = r.get('_additional', {}).get('score', 'N/A')
                print(f"    {i}. {r.get('title', 'N/A')} (分数: {score}, 类别: {r.get('category', 'N/A')})")
    
    print("\n" + "="*80)
    print("✅ 测试完成！")
    print("="*80)

if __name__ == "__main__":
    main()

