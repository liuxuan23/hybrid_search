#!/usr/bin/env python
"""
General-purpose Vector storage test program.

This program selects the Vector storage type to use based on the LIGHTRAG_VECTOR_STORAGE configuration in .env,
and tests its basic operations.

Supported Vector storage types include:
- NanoVectorDBStorage
- MilvusVectorDBStorage
- PGVectorStorage
- FaissVectorDBStorage
- QdrantVectorDBStorage
- MongoVectorDBStorage
- LanceDBVectorStorage
"""

import asyncio
import os
import sys
import importlib
import numpy as np
import pytest
from dotenv import load_dotenv
from ascii_colors import ASCIIColors

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lightrag.kg import (
    STORAGE_IMPLEMENTATIONS,
    STORAGE_ENV_REQUIREMENTS,
    STORAGES,
    verify_storage_implementation,
)
from lightrag.kg.shared_storage import initialize_share_data
from lightrag.utils import EmbeddingFunc


# Mock embedding function that returns deterministic vectors for testing
async def _mock_embed(texts, **kwargs):
    """Return deterministic embeddings based on text content."""
    embeddings = []
    for text in texts:
        # Create a deterministic embedding based on text hash
        np.random.seed(hash(text) % (2**32))
        embedding = np.random.rand(10).astype(np.float32).tolist()
        embeddings.append(embedding)
    return np.array(embeddings)


mock_embedding_func = EmbeddingFunc(
    embedding_dim=10,
    func=_mock_embed,
)


def check_env_file():
    """Check if the .env file exists and issue a warning if it does not."""
    if not os.path.exists(".env"):
        warning_msg = "Warning: .env file not found in the current directory. This may affect storage configuration loading."
        ASCIIColors.yellow(warning_msg)

        if sys.stdin.isatty():
            response = input("Do you want to continue? (yes/no): ")
            if response.lower() != "yes":
                ASCIIColors.red("Test program cancelled.")
                return False
    return True


async def initialize_vector_storage():
    """Initialize the corresponding Vector storage instance based on environment variables."""
    vector_storage_type = os.getenv("LIGHTRAG_VECTOR_STORAGE", "NanoVectorDBStorage")

    try:
        verify_storage_implementation("VECTOR_STORAGE", vector_storage_type)
    except ValueError as e:
        ASCIIColors.red(f"Error: {str(e)}")
        ASCIIColors.yellow(
            f"Supported Vector storage types: {', '.join(STORAGE_IMPLEMENTATIONS['VECTOR_STORAGE']['implementations'])}"
        )
        return None

    required_env_vars = STORAGE_ENV_REQUIREMENTS.get(vector_storage_type, [])
    missing_env_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_env_vars:
        ASCIIColors.red(
            f"Error: {vector_storage_type} requires the following environment variables, but they are not set: {', '.join(missing_env_vars)}"
        )
        return None

    module_path = STORAGES.get(vector_storage_type)
    if not module_path:
        ASCIIColors.red(f"Error: Module path for {vector_storage_type} not found.")
        return None

    try:
        module = importlib.import_module(module_path, package="lightrag")
        storage_class = getattr(module, vector_storage_type)
    except (ImportError, AttributeError) as e:
        ASCIIColors.red(f"Error: Failed to import {vector_storage_type}: {str(e)}")
        return None

    global_config = {
        "embedding_batch_num": 10,
        "vector_db_storage_cls_kwargs": {
            "cosine_better_than_threshold": 0.5,
            "lancedb_metric": "cosine",
        },
        "working_dir": os.environ.get("WORKING_DIR", "./rag_storage"),
    }

    initialize_share_data()

    try:
        storage = storage_class(
            namespace="test_vector",
            workspace="test_workspace",
            global_config=global_config,
            embedding_func=mock_embedding_func,
            meta_fields={"category", "author"},
        )

        await storage.initialize()
        return storage
    except Exception as e:
        ASCIIColors.red(f"Error: Failed to initialize {vector_storage_type}: {str(e)}")
        return None


@pytest.mark.integration
@pytest.mark.requires_db
async def test_vector_basic(storage):
    """Test basic Vector storage operations: upsert, query, get_by_id, delete."""
    try:
        # 1. Test upsert
        test_data = {
            "doc1": {
                "content": "Python is a programming language",
                "category": "programming",
                "author": "Guido van Rossum",
            },
            "doc2": {
                "content": "Machine learning is a subset of artificial intelligence",
                "category": "AI",
                "author": "Arthur Samuel",
            },
            "doc3": {
                "content": "Deep learning uses neural networks",
                "category": "AI",
                "author": "Geoffrey Hinton",
            },
        }
        print("Inserting test vectors...")
        await storage.upsert(test_data)
        print("✓ upsert test passed")

        # 2. Test query
        print("Testing query...")
        query_text = "artificial intelligence"
        results = await storage.query(query_text, top_k=2)
        assert len(results) > 0, "Query should return at least one result"
        assert "id" in results[0], "Result should have 'id' field"
        assert "distance" in results[0] or "score" in results[0], "Result should have distance/score"
        print(f"✓ query test passed (returned {len(results)} results)")

        # 3. Test get_by_id (note: content is used for embedding only, not stored;
        #    only meta_fields like category and author are persisted)
        print("Testing get_by_id...")
        result = await storage.get_by_id("doc1")
        assert result is not None, "get_by_id should return a result"
        assert result.get("category") == "programming", f"Category should match, got {result.get('category')}"
        assert result.get("author") == "Guido van Rossum", f"Author should match, got {result.get('author')}"
        print("✓ get_by_id test passed")

        # 4. Test get_by_ids
        print("Testing get_by_ids...")
        results = await storage.get_by_ids(["doc1", "doc2", "nonexistent"])
        assert len(results) >= 2, "Should return at least 2 results"
        found_ids = {r.get("id") for r in results if r is not None}
        assert "doc1" in found_ids or any("doc1" in str(r) for r in results if r), "doc1 should be found"
        assert "doc2" in found_ids or any("doc2" in str(r) for r in results if r), "doc2 should be found"
        print("✓ get_by_ids test passed")

        # 5. Test get_vectors_by_ids
        print("Testing get_vectors_by_ids...")
        vectors = await storage.get_vectors_by_ids(["doc1", "doc2"])
        assert len(vectors) > 0, "Should return at least one vector"
        assert isinstance(list(vectors.values())[0], list), "Vector should be a list"
        print("✓ get_vectors_by_ids test passed")

        # 6. Test update (upsert existing id with changed meta_fields)
        print("Testing update...")
        updated_data = {
            "doc1": {
                "content": "Python is a high-level programming language",
                "category": "language",
                "author": "Guido van Rossum Updated",
            }
        }
        await storage.upsert(updated_data)
        updated_result = await storage.get_by_id("doc1")
        assert updated_result is not None, "Updated document should exist"
        assert updated_result.get("category") == "language", f"Category should be updated, got {updated_result.get('category')}"
        assert updated_result.get("author") == "Guido van Rossum Updated", f"Author should be updated, got {updated_result.get('author')}"
        print("✓ update test passed")

        # 7. Test delete
        print("Testing delete...")
        await storage.delete(["doc3"])
        deleted_result = await storage.get_by_id("doc3")
        assert deleted_result is None, "Deleted document should return None"
        print("✓ delete test passed")

        # 8. Test delete remaining doc
        print("Testing delete doc2...")
        await storage.delete(["doc2"])
        entity_result = await storage.get_by_id("doc2")
        assert entity_result is None, "Deleted doc2 should return None"
        print("✓ delete doc2 test passed")

        print("\n✓ All Vector storage basic tests passed!")
        return True

    except Exception as e:
        ASCIIColors.red(f"An error occurred during the test: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Main test execution function."""
    load_dotenv()

    if not check_env_file():
        return

    ASCIIColors.cyan("\n" + "=" * 60)
    ASCIIColors.cyan(" " * 10 + "Vector Storage Test Program")
    ASCIIColors.cyan("=" * 60 + "\n")

    vector_storage_type = os.getenv("LIGHTRAG_VECTOR_STORAGE", "NanoVectorDBStorage")
    ASCIIColors.cyan(f"Currently configured Vector storage type: {vector_storage_type}")
    ASCIIColors.cyan(
        f"Supported Vector storage types: {', '.join(STORAGE_IMPLEMENTATIONS['VECTOR_STORAGE']['implementations'])}"
    )

    storage = await initialize_vector_storage()
    if not storage:
        ASCIIColors.red("Failed to initialize Vector storage. Exiting.")
        return

    try:
        # Clean data before running tests
        ASCIIColors.yellow("\nCleaning data before running tests...")
        await storage.drop()
        ASCIIColors.green("Data cleanup complete\n")

        # Run basic test
        ASCIIColors.cyan("\n=== Starting Basic Vector Test ===")
        result = await test_vector_basic(storage)

        if result:
            ASCIIColors.green("\n✓ All tests completed successfully!")
        else:
            ASCIIColors.red("\n✗ Some tests failed.")

    finally:
        if storage:
            await storage.finalize()
            ASCIIColors.green("\nStorage connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
