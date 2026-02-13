#!/usr/bin/env python
"""
General-purpose KV storage test program.

This program selects the KV storage type to use based on the LIGHTRAG_KV_STORAGE configuration in .env,
and tests its basic operations.

Supported KV storage types include:
- JsonKVStorage
- RedisKVStorage
- PGKVStorage
- MongoKVStorage
- LanceDBKVStorage
"""

import asyncio
import os
import sys
import importlib
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


# Mock embedding function (required by BaseKVStorage but not used for KV operations)
async def _mock_embed(texts, **kwargs):
    return [[0.0] * 10 for _ in texts]


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


async def initialize_kv_storage():
    """Initialize the corresponding KV storage instance based on environment variables."""
    kv_storage_type = os.getenv("LIGHTRAG_KV_STORAGE", "JsonKVStorage")

    try:
        verify_storage_implementation("KV_STORAGE", kv_storage_type)
    except ValueError as e:
        ASCIIColors.red(f"Error: {str(e)}")
        ASCIIColors.yellow(
            f"Supported KV storage types: {', '.join(STORAGE_IMPLEMENTATIONS['KV_STORAGE']['implementations'])}"
        )
        return None

    required_env_vars = STORAGE_ENV_REQUIREMENTS.get(kv_storage_type, [])
    missing_env_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_env_vars:
        ASCIIColors.red(
            f"Error: {kv_storage_type} requires the following environment variables, but they are not set: {', '.join(missing_env_vars)}"
        )
        return None

    module_path = STORAGES.get(kv_storage_type)
    if not module_path:
        ASCIIColors.red(f"Error: Module path for {kv_storage_type} not found.")
        return None

    try:
        module = importlib.import_module(module_path, package="lightrag")
        storage_class = getattr(module, kv_storage_type)
    except (ImportError, AttributeError) as e:
        ASCIIColors.red(f"Error: Failed to import {kv_storage_type}: {str(e)}")
        return None

    global_config = {
        "working_dir": os.environ.get("WORKING_DIR", "./rag_storage"),
    }

    initialize_share_data()

    try:
        storage = storage_class(
            namespace="test_kv",
            workspace="test_workspace",
            global_config=global_config,
            embedding_func=mock_embedding_func,
        )

        await storage.initialize()
        return storage
    except Exception as e:
        ASCIIColors.red(f"Error: Failed to initialize {kv_storage_type}: {str(e)}")
        return None


@pytest.mark.integration
@pytest.mark.requires_db
async def test_kv_basic(storage):
    """Test basic KV storage operations: upsert, get_by_id, get_by_ids, filter_keys, delete."""
    try:
        # 1. Test upsert
        test_data = {
            "key1": {"name": "Alice", "age": 30, "city": "New York"},
            "key2": {"name": "Bob", "age": 25, "city": "San Francisco"},
            "key3": {"name": "Charlie", "age": 35, "city": "Boston"},
        }
        print("Inserting test data...")
        await storage.upsert(test_data)

        # 2. Test get_by_id
        print("Testing get_by_id...")
        result = await storage.get_by_id("key1")
        assert result is not None, "get_by_id should return a result"
        assert result["name"] == "Alice", f"Expected 'Alice', got {result.get('name')}"
        assert result["age"] == 30, f"Expected 30, got {result.get('age')}"
        print("✓ get_by_id test passed")

        # 3. Test get_by_ids
        print("Testing get_by_ids...")
        results = await storage.get_by_ids(["key1", "key2", "key3", "nonexistent"])
        assert len(results) == 4, f"Expected 4 results, got {len(results)}"
        assert results[0] is not None and results[0]["name"] == "Alice"
        assert results[1] is not None and results[1]["name"] == "Bob"
        assert results[2] is not None and results[2]["name"] == "Charlie"
        assert results[3] is None, "Nonexistent key should return None"
        print("✓ get_by_ids test passed")

        # 4. Test filter_keys
        print("Testing filter_keys...")
        missing_keys = await storage.filter_keys({"key1", "key2", "key4", "key5"})
        assert "key4" in missing_keys, "key4 should be missing"
        assert "key5" in missing_keys, "key5 should be missing"
        assert "key1" not in missing_keys, "key1 should exist"
        assert "key2" not in missing_keys, "key2 should exist"
        print("✓ filter_keys test passed")

        # 5. Test update (upsert existing key)
        print("Testing update...")
        updated_data = {"key1": {"name": "Alice Updated", "age": 31, "city": "Los Angeles"}}
        await storage.upsert(updated_data)
        updated_result = await storage.get_by_id("key1")
        assert updated_result["name"] == "Alice Updated", "Update should change name"
        assert updated_result["age"] == 31, "Update should change age"
        assert updated_result["city"] == "Los Angeles", "Update should change city"
        print("✓ update test passed")

        # 6. Test delete
        print("Testing delete...")
        await storage.delete(["key3"])
        deleted_result = await storage.get_by_id("key3")
        assert deleted_result is None, "Deleted key should return None"
        print("✓ delete test passed")

        # 7. Test is_empty
        print("Testing is_empty...")
        is_empty = await storage.is_empty()
        assert not is_empty, "Storage should not be empty"
        print("✓ is_empty test passed")

        print("\n✓ All KV storage basic tests passed!")
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
    ASCIIColors.cyan(" " * 10 + "KV Storage Test Program")
    ASCIIColors.cyan("=" * 60 + "\n")

    kv_storage_type = os.getenv("LIGHTRAG_KV_STORAGE", "JsonKVStorage")
    ASCIIColors.cyan(f"Currently configured KV storage type: {kv_storage_type}")
    ASCIIColors.cyan(
        f"Supported KV storage types: {', '.join(STORAGE_IMPLEMENTATIONS['KV_STORAGE']['implementations'])}"
    )

    storage = await initialize_kv_storage()
    if not storage:
        ASCIIColors.red("Failed to initialize KV storage. Exiting.")
        return

    try:
        # Clean data before running tests
        ASCIIColors.yellow("\nCleaning data before running tests...")
        await storage.drop()
        ASCIIColors.green("Data cleanup complete\n")

        # Run basic test
        ASCIIColors.cyan("\n=== Starting Basic KV Test ===")
        result = await test_kv_basic(storage)

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
