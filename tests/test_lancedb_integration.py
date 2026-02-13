#!/usr/bin/env python
"""
LanceDB integration test using Python API (not server).

This script tests LightRAG with all 4 storage types configured to use LanceDB,
using real LLM and embedding functions from environment variables.
"""

import os
import sys
import asyncio
from dotenv import load_dotenv

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lightrag import LightRAG, QueryParam
from lightrag.llm.openai import openai_complete_if_cache, openai_embed
from lightrag.utils import EmbeddingFunc

# Load environment variables
load_dotenv()

# Test configuration
WORKING_DIR = "./test_lancedb_integration"
LANCEDB_URI = os.getenv("LANCEDB_URI", "./test_lancedb_integration_db")


async def main():
    """Main integration test function."""
    print("=" * 70)
    print("LanceDB Integration Test - Python API")
    print("=" * 70)
    print()

    # Check required environment variables
    llm_api_key = os.getenv("LLM_BINDING_API_KEY")
    embedding_api_key = os.getenv("EMBEDDING_BINDING_API_KEY")
    
    if not llm_api_key:
        print("ERROR: LLM_BINDING_API_KEY not set in environment")
        return
    
    if not embedding_api_key:
        print("ERROR: EMBEDDING_BINDING_API_KEY not set in environment")
        return

    print(f"LLM Model: {os.getenv('LLM_MODEL', 'not set')}")
    print(f"LLM Host: {os.getenv('LLM_BINDING_HOST', 'not set')}")
    print(f"Embedding Model: {os.getenv('EMBEDDING_MODEL', 'not set')}")
    print(f"Embedding Host: {os.getenv('EMBEDDING_BINDING_HOST', 'not set')}")
    print(f"LanceDB URI: {LANCEDB_URI}")
    print()

    # Create working directory
    if not os.path.exists(WORKING_DIR):
        os.makedirs(WORKING_DIR)
        print(f"Created working directory: {WORKING_DIR}")

    # Get LLM and embedding configuration from environment
    llm_model = os.getenv("LLM_MODEL")
    if not llm_model:
        print("ERROR: LLM_MODEL not set in environment")
        return
    
    llm_host = os.getenv("LLM_BINDING_HOST")
    if not llm_host:
        print("ERROR: LLM_BINDING_HOST not set in environment")
        return
    
    embedding_model = os.getenv("EMBEDDING_MODEL")
    if not embedding_model:
        print("ERROR: EMBEDDING_MODEL not set in environment")
        return
    
    embedding_host = os.getenv("EMBEDDING_BINDING_HOST")
    if not embedding_host:
        print("ERROR: EMBEDDING_BINDING_HOST not set in environment")
        return
    
    embedding_dim = int(os.getenv("EMBEDDING_DIM", "1024"))

    # Create LLM function wrapper
    async def llm_func(prompt, system_prompt=None, **kwargs):
        return await openai_complete_if_cache(
            model=llm_model,
            prompt=prompt,
            system_prompt=system_prompt,
            base_url=llm_host,
            api_key=llm_api_key,
            **kwargs
        )

    # Create embedding function wrapper
    # NOTE: openai_embed is already an EmbeddingFunc (decorated with embedding_dim=1536),
    # so we must call openai_embed.func to bypass its internal dimension validation.
    # Our outer EmbeddingFunc(embedding_dim=1024) will handle the validation instead.
    async def embedding_func(texts, **kwargs):
        return await openai_embed.func(
            model=embedding_model,
            texts=texts,
            base_url=embedding_host,
            api_key=embedding_api_key,
            embedding_dim=embedding_dim,
            **kwargs
        )

    # Wrap embedding function with EmbeddingFunc
    embedding_func_wrapped = EmbeddingFunc(
        embedding_dim=embedding_dim,
        func=embedding_func,
        model_name=embedding_model,
    )

    # Initialize LightRAG with LanceDB storage
    print("Initializing LightRAG with LanceDB storage...")
    rag = LightRAG(
        working_dir=WORKING_DIR,
        kv_storage="LanceDBKVStorage",
        vector_storage="LanceDBVectorStorage",
        graph_storage="LanceDBGraphStorage",
        doc_status_storage="LanceDBDocStatusStorage",
        embedding_func=embedding_func_wrapped,
        llm_model_func=llm_func,
        vector_db_storage_cls_kwargs={
            "cosine_better_than_threshold": 0.2,
            "lancedb_metric": "cosine",
        },
    )

    # Set LanceDB URI environment variable for this process
    os.environ["LANCEDB_URI"] = LANCEDB_URI
    
    print("Configuration Summary:")
    print(f"  Working Directory: {WORKING_DIR}")
    print(f"  LanceDB URI: {LANCEDB_URI}")
    print(f"  KV Storage: LanceDBKVStorage")
    print(f"  Vector Storage: LanceDBVectorStorage")
    print(f"  Graph Storage: LanceDBGraphStorage")
    print(f"  DocStatus Storage: LanceDBDocStatusStorage")
    print()

    try:
        # Initialize storages
        print("Initializing storages...")
        await rag.initialize_storages()
        print("✓ All storages initialized\n")

        # Test 1: Insert a simple document
        print("=" * 70)
        print("Test 1: Insert Document")
        print("=" * 70)
        test_doc = """
        Artificial Intelligence (AI) is a branch of computer science that aims to create 
        intelligent machines capable of performing tasks that typically require human intelligence.
        Machine Learning is a subset of AI that enables systems to learn and improve from 
        experience without being explicitly programmed. Deep Learning, a further subset of 
        Machine Learning, uses neural networks with multiple layers to model and understand 
        complex patterns in data.
        """
        
        print("Inserting test document...")
        track_id = await rag.ainsert(test_doc)
        print(f"✓ Document inserted with track_id: {track_id}\n")

        # Test 2: Query with naive mode
        print("=" * 70)
        print("Test 2: Query (Naive Mode)")
        print("=" * 70)
        query1 = "What is artificial intelligence?"
        print(f"Query: {query1}")
        response1 = await rag.aquery(query1, param=QueryParam(mode="naive"))
        print(f"Response: {response1}\n")

        # Test 3: Query with local mode
        print("=" * 70)
        print("Test 3: Query (Local Mode)")
        print("=" * 70)
        query2 = "What is the relationship between AI and Machine Learning?"
        print(f"Query: {query2}")
        response2 = await rag.aquery(query2, param=QueryParam(mode="local"))
        print(f"Response: {response2}\n")

        # Test 4: Query with global mode
        print("=" * 70)
        print("Test 4: Query (Global Mode)")
        print("=" * 70)
        query3 = "Explain the concept of deep learning"
        print(f"Query: {query3}")
        response3 = await rag.aquery(query3, param=QueryParam(mode="global"))
        print(f"Response: {response3}\n")

        print("=" * 70)
        print("✓ All integration tests passed!")
        print("=" * 70)

    except Exception as e:
        print(f"\n✗ Error occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Finalize storages
        print("\nFinalizing storages...")
        await rag.finalize_storages()
        print("✓ Storages finalized")


if __name__ == "__main__":
    asyncio.run(main())
