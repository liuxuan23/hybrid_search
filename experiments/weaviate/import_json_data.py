"""
导入 JSON 文件数据到 Weaviate
修复版本：确保数据能正确导入
"""
import json
import requests
import time
from typing import Dict, List

WEAVIATE_URL = "http://localhost:8080"

def check_weaviate_service():
    """检查 Weaviate 服务是否可用"""
    try:
        response = requests.get(f"{WEAVIATE_URL}/v1/.well-known/ready", timeout=5)
        return response.status_code == 200
    except:
        return False

def delete_schema(class_name: str):
    """删除已存在的 schema"""
    url = f"{WEAVIATE_URL}/v1/schema/{class_name}"
    response = requests.delete(url)
    if response.status_code in [200, 404]:
        print(f"✓ 已删除现有的 {class_name} schema（如果存在）")
    return response.status_code in [200, 404]

def create_author_schema():
    """创建 Author schema"""
    schema = {
        "class": "Author",
        "properties": [
            {"name": "name", "dataType": ["string"]},
            {"name": "department", "dataType": ["string"]}
        ]
    }
    
    url = f"{WEAVIATE_URL}/v1/schema"
    response = requests.post(url, json=schema)
    if response.status_code == 200:
        print("✓ Author schema 创建成功")
        return True
    else:
        print(f"✗ 创建 Author schema 失败: {response.status_code} - {response.text[:200]}")
        return False

def create_document_schema():
    """创建 Document schema（使用 text2vec-model2vec）"""
    schema = {
        "class": "Document",
        "vectorizer": "text2vec-model2vec",
        "moduleConfig": {
            "text2vec-model2vec": {
                "inferenceUrl": "http://text2vec-model2vec:8080"
            }
        },
        "properties": [
            {"name": "title", "dataType": ["text"]},
            {"name": "content", "dataType": ["text"]},
            {"name": "category", "dataType": ["string"]},
            {"name": "publish_year", "dataType": ["int"]},
            {"name": "image_url", "dataType": ["string"]},
            {"name": "author", "dataType": ["Author"]}  # 引用 Author
        ]
    }
    
    url = f"{WEAVIATE_URL}/v1/schema"
    response = requests.post(url, json=schema)
    if response.status_code == 200:
        print("✓ Document schema 创建成功")
        return True
    else:
        print(f"✗ 创建 Document schema 失败: {response.status_code} - {response.text[:200]}")
        return False

def import_authors(authors: List[Dict]) -> Dict[str, str]:
    """导入作者数据，返回 author_id 到 UUID 的映射"""
    print(f"\n正在导入 {len(authors)} 个作者...")
    
    author_uuid_map = {}
    url = f"{WEAVIATE_URL}/v1/batch/objects"
    
    batch_size = 50
    total_success = 0
    
    for i in range(0, len(authors), batch_size):
        batch = authors[i:i + batch_size]
        objects = []
        
        for auth in batch:
            objects.append({
                "class": "Author",
                "properties": {
                    "name": auth["name"],
                    "department": auth["department"]
                }
            })
        
        response = requests.post(url, json={"objects": objects})
        
        if response.status_code == 200:
            result = response.json()
            if isinstance(result, list):
                # 统计成功导入的数量
                batch_success = sum(1 for r in result if "id" in r)
                total_success += batch_success
                
                # 建立映射关系
                for idx, r in enumerate(result):
                    if "id" in r:
                        author_uuid_map[batch[idx]["id"]] = r["id"]
                
                print(f"  进度: {min(i + batch_size, len(authors))}/{len(authors)} (成功: {batch_success}/{len(batch)})")
            else:
                print(f"  警告: 批次 {i//batch_size + 1} 响应格式异常")
        else:
            print(f"  错误: 批次 {i//batch_size + 1} 导入失败 - {response.status_code}")
    
    print(f"✓ 作者导入完成！成功导入 {total_success}/{len(authors)} 个作者")
    return author_uuid_map

def import_documents(documents: List[Dict], author_uuid_map: Dict[str, str]):
    """导入文档数据"""
    print(f"\n正在导入 {len(documents)} 个文档...")
    
    url = f"{WEAVIATE_URL}/v1/batch/objects"
    batch_size = 100
    total_success = 0
    
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i + batch_size]
        objects = []
        
        for doc in batch:
            # 构建文档对象
            obj = {
                "class": "Document",
                "properties": {
                    "title": doc["title"],
                    "content": doc["content"],
                    "category": doc["category"],
                    "publish_year": doc["publish_year"],
                    "image_url": doc["image_url"]
                }
            }
            
            # 添加 author 引用（如果存在）
            author_id = doc.get("author_id")
            if author_id and author_id in author_uuid_map:
                # 使用正确的引用格式
                obj["properties"]["author"] = [{
                    "beacon": f"weaviate://localhost/Author/{author_uuid_map[author_id]}"
                }]
            
            objects.append(obj)
        
        # 发送批量导入请求
        response = requests.post(url, json={"objects": objects})
        
        if response.status_code == 200:
            result = response.json()
            if isinstance(result, list):
                # 统计成功导入的数量
                batch_success = sum(1 for r in result if "id" in r)
                total_success += batch_success
                
                # 检查是否有错误
                batch_errors = [r for r in result if "errors" in r]
                if batch_errors:
                    print(f"  警告: 批次 {i//batch_size + 1} 有 {len(batch_errors)} 个错误")
                
                print(f"  进度: {min(i + batch_size, len(documents))}/{len(documents)} (成功: {batch_success}/{len(batch)})")
            else:
                print(f"  警告: 批次 {i//batch_size + 1} 响应格式异常")
        else:
            print(f"  错误: 批次 {i//batch_size + 1} 导入失败 - {response.status_code}: {response.text[:200]}")
        
        # 每 5 个批次等待一下，避免过载
        if (i // batch_size + 1) % 5 == 0:
            time.sleep(0.5)
    
    print(f"✓ 文档导入完成！成功导入 {total_success}/{len(documents)} 个文档")
    return total_success

def verify_import():
    """验证导入结果"""
    print("\n正在验证导入结果...")
    time.sleep(3)  # 等待索引更新
    
    # 查询 Author 数量
    author_query = {
        "query": "{ Aggregate { Author { meta { count } } } }"
    }
    author_response = requests.post(
        f"{WEAVIATE_URL}/v1/graphql",
        json=author_query,
        headers={"Content-Type": "application/json"}
    )
    
    # 查询 Document 数量
    doc_query = {
        "query": "{ Aggregate { Document { meta { count } } } }"
    }
    doc_response = requests.post(
        f"{WEAVIATE_URL}/v1/graphql",
        json=doc_query,
        headers={"Content-Type": "application/json"}
    )
    
    if author_response.status_code == 200 and doc_response.status_code == 200:
        author_result = author_response.json()
        doc_result = doc_response.json()
        
        author_count = author_result.get("data", {}).get("Aggregate", {}).get("Author", [{}])[0].get("meta", {}).get("count", 0)
        doc_count = doc_result.get("data", {}).get("Aggregate", {}).get("Document", [{}])[0].get("meta", {}).get("count", 0)
        
        print(f"✓ 验证完成：")
        print(f"  - Author: {author_count} 个")
        print(f"  - Document: {doc_count} 个")
        
        return author_count, doc_count
    else:
        print("✗ 验证失败：无法查询数据")
        return 0, 0

def main():
    print("="*80)
    print("Weaviate 数据导入工具")
    print("="*80)
    
    # 检查服务
    if not check_weaviate_service():
        print("✗ Weaviate 服务不可用，请先启动服务")
        print("  运行: docker compose up -d")
        return
    
    print("✓ Weaviate 服务连接成功\n")
    
    # 删除并重新创建 schema
    print("正在设置 Schema...")
    delete_schema("Document")
    delete_schema("Author")
    
    if not create_author_schema():
        return
    if not create_document_schema():
        return
    
    print("\n✓ Schema 设置完成\n")
    
    # 加载数据
    print("正在加载 JSON 数据...")
    try:
        with open("authors.json", "r", encoding="utf-8") as f:
            authors = json.load(f)
        
        with open("documents.json", "r", encoding="utf-8") as f:
            documents = json.load(f)
        
        print(f"✓ 已加载 {len(authors)} 个作者和 {len(documents)} 个文档")
    except FileNotFoundError as e:
        print(f"✗ 数据文件未找到: {e}")
        print("  请确保 authors.json 和 documents.json 文件存在")
        return
    except Exception as e:
        print(f"✗ 加载数据文件时出错: {e}")
        return
    
    # 导入数据
    author_uuid_map = import_authors(authors)
    
    if not author_uuid_map:
        print("✗ 作者导入失败，无法继续导入文档")
        return
    
    import_documents(documents, author_uuid_map)
    
    # 验证导入结果
    author_count, doc_count = verify_import()
    
    # 总结
    print("\n" + "="*80)
    print("导入总结")
    print("="*80)
    print(f"期望导入: {len(authors)} 个作者, {len(documents)} 个文档")
    print(f"实际导入: {author_count} 个作者, {doc_count} 个文档")
    
    if author_count == len(authors) and doc_count == len(documents):
        print("\n✓ 所有数据导入成功！")
    elif doc_count > 0:
        print(f"\n⚠️  部分数据导入成功（文档: {doc_count}/{len(documents)}）")
        print("   可以开始使用混合查询测试")
    else:
        print("\n✗ 数据导入失败，请检查错误信息")
    
    print("="*80)

if __name__ == "__main__":
    main()

