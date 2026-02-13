# LightRAG 项目结构分析

> 版本: `1.4.9.12` | API 版本: `0270` | 作者: Zirui Guo (香港大学)  
> 协议: MIT | Python >= 3.10

---

## 一、顶层目录结构

```
LightRAG/
├── lightrag/                      # 核心 Python 包 ⭐（详见第二章）
├── lightrag_webui/                # 前端 WebUI（React + TypeScript + Vite）
├── examples/                      # 各种使用示例脚本
│   └── unofficial-sample/         #   社区贡献的非官方示例
├── tests/                         # 测试套件（pytest）
├── docs/                          # 项目文档
├── assets/                        # 项目图片资源（logo 等）
├── k8s-deploy/                    # Kubernetes 部署配置（Helm + KubeBlocks）
│   ├── databases/                 #   数据库部署（ES/MongoDB/Neo4j/PG/Qdrant/Redis）
│   └── lightrag/                  #   LightRAG Helm Chart
├── reproduce/                     # 论文复现脚本（Step_0 ~ Step_3）
├── .github/                       # GitHub CI/CD 配置
│   ├── workflows/                 #   CI 工作流（测试/发布/Docker 构建/Lint）
│   └── ISSUE_TEMPLATE/            #   Issue 模板
├── docker-compose.yml             # Docker 编排
├── Dockerfile                     # Docker 镜像定义（完整版）
├── Dockerfile.lite                # Docker 镜像定义（精简版）
├── docker-build-push.sh           # Docker 构建推送脚本
├── .dockerignore                  # Docker 忽略文件
├── pyproject.toml                 # Python 包配置（包名: lightrag-hku）
├── setup.py                       # 兼容性安装入口
├── uv.lock                        # uv 锁文件（依赖锁定）
├── config.ini.example             # 配置文件模板
├── env.example                    # 环境变量模板
├── lightrag.service.example       # Systemd 服务配置模板
├── requirements-offline.txt       # 离线部署依赖
├── requirements-offline-llm.txt   # 离线 LLM 依赖
├── requirements-offline-storage.txt # 离线存储依赖
├── MANIFEST.in                    # 打包清单
├── .pre-commit-config.yaml        # Pre-commit 钩子配置
├── AGENTS.md                      # AI Agent 协作说明
├── CLAUDE.md                      # Claude AI 协作说明
├── SECURITY.md                    # 安全策略
├── LICENSE                        # MIT 许可证
└── README.md / README-zh.md       # 项目说明文档（英文/中文）
```

---

## 二、核心包 `lightrag/` 详细结构 ⭐

```
lightrag/
├── __init__.py                    # 包入口，导出 LightRAG 和 QueryParam
├── lightrag.py                    # 🔥 核心引擎类 LightRAG（~4000 行）
├── base.py                        # 抽象基类与核心数据结构定义
├── operate.py                     # 🔥 核心操作逻辑（分块/提取/合并/检索，~5000 行）
├── prompt.py                      # LLM 提示词模板集合
├── utils.py                       # 通用工具函数集（~3300 行）
├── utils_graph.py                 # 图操作工具（节点/边删除、持久化）
├── types.py                       # Pydantic 数据模型（知识图谱节点/边）
├── constants.py                   # 集中式常量配置
├── namespace.py                   # 存储命名空间常量定义
├── exceptions.py                  # 自定义异常类
├── rerank.py                      # 重排序模块
│
├── llm/                           # LLM 适配层（多模型集成）
│   ├── openai.py                  #   OpenAI API
│   ├── azure_openai.py            #   Azure OpenAI
│   ├── anthropic.py               #   Anthropic Claude
│   ├── gemini.py                  #   Google Gemini
│   ├── ollama.py                  #   Ollama 本地模型
│   ├── bedrock.py                 #   AWS Bedrock
│   ├── hf.py                      #   HuggingFace Transformers
│   ├── zhipu.py                   #   智谱 AI (GLM)
│   ├── nvidia_openai.py           #   NVIDIA OpenAI 兼容 API
│   ├── lmdeploy.py                #   LMDeploy
│   ├── lollms.py                  #   LoLLMs
│   ├── jina.py                    #   Jina AI（Embedding）
│   ├── llama_index_impl.py        #   LlamaIndex 集成
│   ├── binding_options.py         #   LLM 绑定选项配置
│   └── deprecated/
│       └── siliconcloud.py        #   [已废弃] SiliconCloud
│
├── kg/                            # 知识图谱存储层（插件式多后端）
│   ├── __init__.py                #   存储注册表与验证逻辑
│   ├── shared_storage.py          #   共享存储管理（锁、命名空间、进程间协调）
│   ├── json_kv_impl.py            #   JSON 文件 KV 存储
│   ├── json_doc_status_impl.py    #   JSON 文件文档状态存储
│   ├── nano_vector_db_impl.py     #   NanoVectorDB 向量存储（轻量级）
│   ├── faiss_impl.py              #   FAISS 向量存储
│   ├── networkx_impl.py           #   NetworkX 图存储（本地/内存）
│   ├── neo4j_impl.py              #   Neo4j 图数据库
│   ├── postgres_impl.py           #   PostgreSQL（KV + 向量 + 图 + 文档状态）
│   ├── milvus_impl.py             #   Milvus 向量数据库
│   ├── qdrant_impl.py             #   Qdrant 向量数据库
│   ├── mongo_impl.py              #   MongoDB（KV + 向量 + 图 + 文档状态）
│   ├── redis_impl.py              #   Redis（KV + 文档状态）
│   ├── memgraph_impl.py           #   Memgraph 图数据库
│   └── deprecated/
│       └── chroma_impl.py         #   [已废弃] ChromaDB
│
├── api/                           # API 服务层（FastAPI）
│   ├── __init__.py                #   API 版本号
│   ├── lightrag_server.py         #   🔥 FastAPI 主服务（~1500 行）
│   ├── config.py                  #   服务配置管理
│   ├── auth.py                    #   认证与授权
│   ├── utils_api.py               #   API 工具函数
│   ├── run_with_gunicorn.py       #   Gunicorn 部署启动
│   ├── gunicorn_config.py         #   Gunicorn 配置
│   ├── .env.aoi.example           #   AOI 环境变量示例
│   ├── .gitignore
│   ├── README.md / README-zh.md   #   API 文档
│   ├── routers/                   #   路由模块
│   │   ├── __init__.py            #     路由注册（导出 4 个路由）
│   │   ├── query_routes.py        #     查询 API 路由
│   │   ├── document_routes.py     #     文档管理 API 路由
│   │   ├── graph_routes.py        #     知识图谱 API 路由
│   │   └── ollama_api.py          #     Ollama 兼容 API 路由
│   ├── static/                    #   静态资源
│   │   └── swagger-ui/            #     Swagger UI 文件
│   └── webui/                     #   前端编译产物
│       ├── index.html             #     WebUI 入口
│       ├── logo.svg               #     Logo
│       └── assets/                #     JS/CSS 静态资源
│
├── tools/                         # 辅助工具集
│   ├── check_initialization.py    #   检查存储初始化状态
│   ├── clean_llm_query_cache.py   #   清理 LLM 查询缓存
│   ├── migrate_llm_cache.py       #   LLM 缓存迁移工具
│   ├── download_cache.py          #   下载缓存
│   ├── prepare_qdrant_legacy_data.py #  Qdrant 遗留数据准备
│   ├── README_*.md                #   工具使用说明
│   └── lightrag_visualizer/       #   知识图谱可视化工具
│       ├── graph_visualizer.py    #     可视化脚本
│       ├── requirements.txt       #     依赖
│       └── assets/                #     字体资源
│
└── evaluation/                    # RAG 质量评估模块
    ├── __init__.py
    ├── eval_rag_quality.py        #   评估脚本（基于 RAGAS 框架）
    ├── sample_dataset.json        #   评估数据集
    ├── sample_documents/          #   示例文档（5 篇 Markdown）
    └── README_EVALUASTION_RAGAS.md #  评估说明
```

---

## 三、核心模块详解

### 3.1 `lightrag.py` — 核心引擎

`LightRAG` 类是整个框架的中枢（使用 `@dataclass` + `@final` 修饰），主要职责：

| 职责 | 说明 |
|------|------|
| **存储管理** | 创建和初始化 4 大类存储（KV/向量/图/文档状态），支持运行时切换后端 |
| **文档管理** | 文档插入（`ainsert`）、删除（`adelete_by_doc_id`）、状态追踪 |
| **查询调度** | 多模式查询（`aquery`），支持 local/global/hybrid/naive/mix/bypass 六种模式 |
| **流程编排** | 编排完整的 RAG 流水线：分块 → 实体提取 → 知识图谱构建 → 向量化 → 检索 → 生成 |
| **并发控制** | 异步并发管理、多进程锁、Gunicorn worker 协调 |

核心配置参数：

```python
@dataclass
class LightRAG:
    working_dir: str = "./rag_storage"           # 工作目录
    kv_storage: str = "JsonKVStorage"            # KV 存储后端
    vector_storage: str = "NanoVectorDBStorage"  # 向量存储后端
    graph_storage: str = "NetworkXStorage"       # 图存储后端
    doc_status_storage: str = "JsonDocStatusStorage"  # 文档状态后端
    workspace: str = ""                          # 数据隔离工作空间
    chunk_token_size: int = 1200                 # 分块 token 大小
    chunk_overlap_token_size: int = 100          # 分块重叠 token
    embedding_func: EmbeddingFunc | None = None  # Embedding 函数
    llm_model_func: Callable | None = None       # LLM 调用函数
    # ... 更多参数
```

### 3.2 `base.py` — 抽象基类

定义了所有存储和数据结构的契约：

| 类 | 说明 |
|----|------|
| `StorageNameSpace` | 存储命名空间基类（所有存储的根基） |
| `BaseVectorStorage` | 向量存储抽象基类（query/upsert/delete/get_by_id 等） |
| `BaseKVStorage` | 键值存储抽象基类（get_by_id/upsert/delete/filter_keys 等） |
| `BaseGraphStorage` | 图存储抽象基类（节点/边的 CRUD + 批量操作 + 子图检索） |
| `DocStatusStorage` | 文档状态存储抽象基类（分页查询/状态统计/按路径查找） |
| `QueryParam` | 查询参数配置（mode/top_k/stream/rerank 等） |
| `QueryResult` | 统一查询结果（支持流式/非流式/引用列表） |
| `DocProcessingStatus` | 文档处理状态数据结构 |
| `DocStatus` | 文档状态枚举（pending/processing/preprocessed/processed/failed） |

### 3.3 `operate.py` — 核心操作逻辑

RAG 流水线的所有关键操作集中于此：

| 函数 | 说明 |
|------|------|
| `chunking_by_token_size()` | 文本分块（支持按字符/按 token 切分，可配置重叠） |
| `extract_entities()` | 利用 LLM 从文本块中提取实体和关系 |
| `merge_nodes_and_edges()` | 知识图谱节点/边合并（相同实体的描述合并、LLM 摘要触发） |
| `kg_query()` | 知识图谱检索（支持 local/global/hybrid 模式） |
| `naive_query()` | 朴素向量检索（纯 chunk 级别） |
| `rebuild_knowledge_from_chunks()` | 从已有 chunk 重建知识图谱 |

### 3.4 `prompt.py` — 提示词模板

包含完整的 LLM 提示词体系：

- `entity_extraction_system_prompt` — 实体和关系提取指令（含 few-shot 示例）
- `entity_extraction_examples` — 提取示例
- `summarize_entity_descriptions` — 实体描述合并摘要
- `entiti_continue_extraction` — 继续提取（gleaning）
- `keywords_extraction` — 关键词提取（high-level + low-level）
- `rag_response` — RAG 查询应答模板
- `naive_rag_response` — 朴素 RAG 应答模板

### 3.5 `utils.py` — 工具函数集

提供贯穿整个项目的基础设施：

| 工具类别 | 关键函数/类 |
|----------|------------|
| **日志** | `logger`, `set_verbose_debug()`, `SafeStreamHandler` |
| **分词器** | `Tokenizer`（协议）、`TiktokenTokenizer`（默认实现） |
| **Embedding** | `EmbeddingFunc`（带模型名/维度/批处理的 dataclass） |
| **哈希** | `compute_mdhash_id()` — MD5 哈希生成文档/chunk ID |
| **缓存** | `CacheData`, `handle_cache()`, `save_to_cache()`, `use_llm_func_with_cache()` |
| **异步** | `always_get_an_event_loop()`, `priority_limit_async_func_call()` |
| **文本处理** | `split_string_by_multi_markers()`, `truncate_list_by_token_size()` |
| **引用** | `generate_reference_list_from_chunks()` — 生成引用列表 |

---

## 四、存储架构

LightRAG 采用**插件式存储架构**，通过 `kg/__init__.py` 中的注册表实现：

### 4.1 四大存储类型

| 存储类型 | 用途 | 可选实现 |
|----------|------|----------|
| **KV Storage** | 存储文档全文、文本 chunk、LLM 缓存、实体/关系详情 | `JsonKVStorage`、`RedisKVStorage`、`PGKVStorage`、`MongoKVStorage` |
| **Vector Storage** | 存储实体/关系/chunk 的向量嵌入，支持语义检索 | `NanoVectorDBStorage`、`MilvusVectorDBStorage`、`PGVectorStorage`、`FaissVectorDBStorage`、`QdrantVectorDBStorage`、`MongoVectorDBStorage` |
| **Graph Storage** | 存储知识图谱（实体为节点、关系为边） | `NetworkXStorage`、`Neo4JStorage`、`PGGraphStorage`、`MongoGraphStorage`、`MemgraphStorage` |
| **Doc Status Storage** | 追踪文档处理状态（pending → processed） | `JsonDocStatusStorage`、`RedisDocStatusStorage`、`PGDocStatusStorage`、`MongoDocStatusStorage` |

### 4.2 命名空间（NameSpace）

每个存储实例通过命名空间隔离数据：

```
KV 存储命名空间:
  ├── full_docs              # 文档原始全文
  ├── text_chunks            # 文本分块
  ├── llm_response_cache     # LLM 响应缓存
  ├── full_entities          # 实体完整信息
  ├── full_relations         # 关系完整信息
  ├── entity_chunks          # 实体-chunk 关联
  └── relation_chunks        # 关系-chunk 关联

向量存储命名空间:
  ├── entities               # 实体向量
  ├── relationships          # 关系向量
  └── chunks                 # chunk 向量

图存储命名空间:
  └── chunk_entity_relation  # 知识图谱

文档状态命名空间:
  └── doc_status             # 文档处理状态
```

### 4.3 存储后端组合建议

| 场景 | KV | 向量 | 图 | 文档状态 |
|------|-----|------|-----|----------|
| **本地轻量开发** | Json | NanoVectorDB | NetworkX | Json |
| **单机生产环境** | Json | FAISS | NetworkX | Json |
| **PostgreSQL 全家桶** | PG | PGVector | PG | PG |
| **MongoDB 全家桶** | Mongo | Mongo | Mongo | Mongo |
| **大规模生产** | Redis | Milvus/Qdrant | Neo4j | Redis |

---

## 五、LLM 集成层

支持 **14 种** LLM/Embedding 提供商：

| 提供商 | 文件 | LLM | Embedding | 说明 |
|--------|------|:---:|:---------:|------|
| OpenAI | `openai.py` | ✅ | ✅ | 默认推荐 |
| Azure OpenAI | `azure_openai.py` | ✅ | ✅ | 企业级 Azure |
| Anthropic | `anthropic.py` | ✅ | ❌ | Claude 系列 |
| Google Gemini | `gemini.py` | ✅ | ✅ | Gemini Pro/Flash |
| Ollama | `ollama.py` | ✅ | ✅ | 本地部署模型 |
| AWS Bedrock | `bedrock.py` | ✅ | ✅ | AWS 云服务 |
| HuggingFace | `hf.py` | ✅ | ✅ | 开源模型 |
| 智谱 AI | `zhipu.py` | ✅ | ✅ | GLM 系列 |
| NVIDIA | `nvidia_openai.py` | ✅ | ✅ | NVIDIA NIM |
| LMDeploy | `lmdeploy.py` | ✅ | ❌ | 本地高效推理 |
| LoLLMs | `lollms.py` | ✅ | ✅ | 多模型网关 |
| Jina AI | `jina.py` | ❌ | ✅ | Embedding 专用 |
| LlamaIndex | `llama_index_impl.py` | ✅ | ✅ | 框架集成 |
| SiliconCloud | `deprecated/` | ⚠️ | ⚠️ | 已废弃 |

---

## 六、API 服务层

基于 **FastAPI** 构建的 REST API 服务，支持 **uvicorn** 和 **gunicorn** 两种部署模式：

### 6.1 路由结构

| 路由模块 | 端点前缀 | 功能 |
|----------|---------|------|
| `document_routes.py` | `/documents` | 文档上传/删除/状态查询/批量管理 |
| `query_routes.py` | `/query` | RAG 查询（支持流式/非流式/多模式） |
| `graph_routes.py` | `/graph` | 知识图谱浏览（节点/边/子图/标签） |
| `ollama_api.py` | `/api` | Ollama 兼容 API（让 LightRAG 伪装为 Ollama 服务） |

### 6.2 内嵌 WebUI

API 服务内嵌了编译好的前端 WebUI（位于 `api/webui/`），提供：
- 知识图谱可视化浏览（基于 Cytoscape.js）
- 文档管理界面
- RAG 查询交互界面
- 多种图表展示（Mermaid 集成）

---

## 七、查询模式

LightRAG 支持 **6 种查询模式**（通过 `QueryParam.mode` 设置）：

| 模式 | 说明 |
|------|------|
| `local` | 局部检索 — 基于实体的上下文相关信息检索 |
| `global` | 全局检索 — 基于关系的全局知识检索 |
| `hybrid` | 混合检索 — 结合 local 和 global 的结果 |
| `naive` | 朴素检索 — 纯向量 chunk 检索，不使用知识图谱 |
| `mix` | 综合检索 — 知识图谱 + 向量检索的深度融合（**默认模式**） |
| `bypass` | 旁路模式 — 跳过检索，直接将查询发送给 LLM |

---

## 八、数据处理流水线

```
                        文档插入流程
                        ═══════════

原始文档 ──→ 文本分块 (chunking_by_token_size)
                │
                ▼
         LLM 实体/关系提取 (extract_entities)
                │
                ▼
         知识图谱节点合并 (merge_nodes_and_edges)
                │
                ├──→ KV 存储 (full_docs, text_chunks, entities, relations)
                ├──→ 向量存储 (entities, relationships, chunks 的嵌入)
                ├──→ 图存储 (知识图谱节点和边)
                └──→ 文档状态更新 (doc_status)


                        查询流程
                        ═══════

用户查询 ──→ 关键词提取 (LLM)
                │
                ├──→ 向量检索 (entities/relationships/chunks VDB)
                ├──→ 图遍历 (知识图谱邻居扩展)
                └──→ 文本 chunk 关联检索
                        │
                        ▼
                上下文组装 + Token 预算控制
                        │
                        ▼
                LLM 生成最终回答 (支持流式)
                        │
                        ▼
                返回结果 (含可选引用列表)
```

---

## 九、其他目录说明

### 9.1 `lightrag_webui/` — 前端项目

| 技术栈 | 说明 |
|--------|------|
| 框架 | React + TypeScript |
| 构建 | Vite + Bun |
| UI | Tailwind CSS + shadcn/ui |
| 图可视化 | Cytoscape.js |
| 图表 | Mermaid |
| 国际化 | 支持 10 种语言（中/英/日/韩/法/德/俄/阿/乌克兰/繁体中文） |

### 9.2 `examples/` — 使用示例

涵盖各种场景的使用示例：

- **基础使用**: `lightrag_openai_demo.py`, `lightrag_ollama_demo.py`
- **云服务**: `lightrag_azure_openai_demo.py`, `lightrag_gemini_demo.py`, `lightrag_vllm_demo.py`
- **存储组合**: `lightrag_gemini_postgres_demo.py`, `lightrag_openai_mongodb_graph_demo.py`
- **高级功能**: `insert_custom_kg.py`（自定义知识图谱）, `rerank_example.py`（重排序）, `graph_visual_with_neo4j.py`（图可视化）
- **多模态**: `modalprocessors_example.py`, `raganything_example.py`

### 9.3 `tests/` — 测试套件

包含 20+ 测试文件，覆盖：

- 分块逻辑、重排序分块、重叠验证
- 各类存储后端（PostgreSQL 索引/迁移/重试、Neo4j 全文索引、Qdrant 迁移）
- 工作空间隔离、维度不匹配检测
- Token 自动续期、统一锁安全性
- JSON 写入优化

### 9.4 `k8s-deploy/` — Kubernetes 部署

基于 **KubeBlocks** 的数据库编排 + **Helm** 的 LightRAG 部署：

- 数据库支持: Elasticsearch、MongoDB、Neo4j、PostgreSQL、Qdrant、Redis
- 提供开发环境和生产环境两套部署脚本

### 9.5 `reproduce/` — 论文复现

论文实验的分步复现脚本（Step 0-3），支持 OpenAI 兼容 API。

---

## 十、关键设计特点

1. **插件式存储架构** — 4 大类存储均通过抽象基类定义接口，支持 15+ 种后端实现自由组合
2. **多模式检索** — 6 种查询模式覆盖从简单向量检索到知识图谱深度融合的全谱段
3. **LLM 无关性** — 适配 14 种 LLM 提供商，支持本地和云端模型
4. **生产就绪** — 提供 Docker/K8s 部署、Gunicorn 多 worker、进程间锁协调、文档状态追踪
5. **全异步设计** — 核心流水线全部基于 Python asyncio 实现
6. **Token 预算控制** — 查询时对实体/关系/chunk 进行统一 token 预算管理
7. **增量更新** — 支持文档增量插入、知识图谱节点合并与描述摘要
8. **内嵌 WebUI** — API 服务自带可视化界面，无需额外前端部署
