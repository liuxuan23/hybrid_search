# 生成统一测试数据集
# 用于测试 Weaviate 的性能和功能
# 每条记录包含
# {
#   "id": "doc_001",
#   "title": "新一代AI芯片发布",
#   "content": "我们发布了支持大模型推理的NPU芯片...",
#   "category": "Hardware",           // 标量
#   "publish_year": 2024,            // 标量
#   "author_id": "auth_001",         // 图谱关系
#   "image_url": "https://.../chip.jpg"  // 多模态（可选）
# }

# 作者包含
# {
#   "id": "auth_001",
#   "name": "李明",
#   "department": "AI Lab"
# }
import json
import random

categories = ["Hardware", "Software", "Research"]
departments = ["AI Lab", "Cloud", "Hardware"]

docs = []
authors = {}

for i in range(1000):
    auth_id = f"auth_{random.randint(1, 50)}"
    if auth_id not in authors:
        authors[auth_id] = {
            "id": auth_id,
            "name": f"Author {auth_id}",
            "department": random.choice(departments)
        }
    
    docs.append({
        "id": f"doc_{i:04d}",
        "title": f"Document about {random.choice(['AI', 'Chip', 'Cloud'])}",
        "content": f"This is a document discussing advanced topics in {random.choice(['machine learning', 'semiconductor', 'distributed systems'])}.",
        "category": random.choice(categories),
        "publish_year": random.randint(2020, 2024),
        "author_id": auth_id,
        "image_url": f"https://example.com/img_{i}.jpg"
    })

# 保存
with open("documents.json", "w") as f:
    json.dump(docs, f, indent=2)
with open("authors.json", "w") as f:
    json.dump(list(authors.values()), f, indent=2)