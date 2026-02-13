#!/usr/bin/env python
"""
General-purpose DocStatus storage test program.

This program selects the DocStatus storage type to use based on the LIGHTRAG_DOC_STATUS_STORAGE configuration in .env,
and tests its basic operations.

Supported DocStatus storage types include:
- JsonDocStatusStorage
- RedisDocStatusStorage
- PGDocStatusStorage
- MongoDocStatusStorage
- LanceDBDocStatusStorage
"""

import asyncio
import os
import sys
import importlib
from datetime import datetime
import pytest
from dotenv import load_dotenv
from ascii_colors import ASCIIColors

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lightrag.base import DocStatus, DocProcessingStatus
from lightrag.kg import (
    STORAGE_IMPLEMENTATIONS,
    STORAGE_ENV_REQUIREMENTS,
    STORAGES,
    verify_storage_implementation,
)
from lightrag.kg.shared_storage import initialize_share_data
from lightrag.utils import EmbeddingFunc


# Mock embedding function (required by BaseKVStorage but not used for DocStatus operations)
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


async def initialize_doc_status_storage():
    """Initialize the corresponding DocStatus storage instance based on environment variables."""
    doc_status_storage_type = os.getenv("LIGHTRAG_DOC_STATUS_STORAGE", "JsonDocStatusStorage")

    try:
        verify_storage_implementation("DOC_STATUS_STORAGE", doc_status_storage_type)
    except ValueError as e:
        ASCIIColors.red(f"Error: {str(e)}")
        ASCIIColors.yellow(
            f"Supported DocStatus storage types: {', '.join(STORAGE_IMPLEMENTATIONS['DOC_STATUS_STORAGE']['implementations'])}"
        )
        return None

    required_env_vars = STORAGE_ENV_REQUIREMENTS.get(doc_status_storage_type, [])
    missing_env_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_env_vars:
        ASCIIColors.red(
            f"Error: {doc_status_storage_type} requires the following environment variables, but they are not set: {', '.join(missing_env_vars)}"
        )
        return None

    module_path = STORAGES.get(doc_status_storage_type)
    if not module_path:
        ASCIIColors.red(f"Error: Module path for {doc_status_storage_type} not found.")
        return None

    try:
        module = importlib.import_module(module_path, package="lightrag")
        storage_class = getattr(module, doc_status_storage_type)
    except (ImportError, AttributeError) as e:
        ASCIIColors.red(f"Error: Failed to import {doc_status_storage_type}: {str(e)}")
        return None

    global_config = {
        "working_dir": os.environ.get("WORKING_DIR", "./rag_storage"),
    }

    initialize_share_data()

    try:
        storage = storage_class(
            namespace="test_doc_status",
            workspace="test_workspace",
            global_config=global_config,
            embedding_func=mock_embedding_func,
        )

        await storage.initialize()
        return storage
    except Exception as e:
        ASCIIColors.red(f"Error: Failed to initialize {doc_status_storage_type}: {str(e)}")
        return None


def create_doc_status(doc_id: str, status: DocStatus, track_id: str = None) -> DocProcessingStatus:
    """Helper function to create a DocProcessingStatus instance."""
    now = datetime.now().isoformat()
    return DocProcessingStatus(
        content_summary=f"Summary for {doc_id}",
        content_length=1000,
        file_path=f"/path/to/{doc_id}.txt",
        status=status,
        created_at=now,
        updated_at=now,
        track_id=track_id or f"track_{doc_id}",
        chunks_count=5,
        chunks_list=[f"chunk_{doc_id}_1", f"chunk_{doc_id}_2"],
    )


@pytest.mark.integration
@pytest.mark.requires_db
async def test_doc_status_basic(storage):
    """Test basic DocStatus storage operations: upsert, get_by_id, get_docs_by_status, get_status_counts."""
    try:
        # 1. Test upsert
        doc1 = create_doc_status("doc1", DocStatus.PENDING, "track1")
        doc2 = create_doc_status("doc2", DocStatus.PROCESSING, "track1")
        doc3 = create_doc_status("doc3", DocStatus.PROCESSED, "track2")
        doc4 = create_doc_status("doc4", DocStatus.FAILED, "track2")

        test_data = {
            "doc1": {
                "content_summary": doc1.content_summary,
                "content_length": doc1.content_length,
                "file_path": doc1.file_path,
                "status": doc1.status.value,
                "created_at": doc1.created_at,
                "updated_at": doc1.updated_at,
                "track_id": doc1.track_id,
                "chunks_count": doc1.chunks_count,
                "chunks_list": doc1.chunks_list,
            },
            "doc2": {
                "content_summary": doc2.content_summary,
                "content_length": doc2.content_length,
                "file_path": doc2.file_path,
                "status": doc2.status.value,
                "created_at": doc2.created_at,
                "updated_at": doc2.updated_at,
                "track_id": doc2.track_id,
                "chunks_count": doc2.chunks_count,
                "chunks_list": doc2.chunks_list,
            },
            "doc3": {
                "content_summary": doc3.content_summary,
                "content_length": doc3.content_length,
                "file_path": doc3.file_path,
                "status": doc3.status.value,
                "created_at": doc3.created_at,
                "updated_at": doc3.updated_at,
                "track_id": doc3.track_id,
                "chunks_count": doc3.chunks_count,
                "chunks_list": doc3.chunks_list,
            },
            "doc4": {
                "content_summary": doc4.content_summary,
                "content_length": doc4.content_length,
                "file_path": doc4.file_path,
                "status": doc4.status.value,
                "created_at": doc4.created_at,
                "updated_at": doc4.updated_at,
                "track_id": doc4.track_id,
                "chunks_count": doc4.chunks_count,
                "chunks_list": doc4.chunks_list,
            },
        }

        print("Inserting test documents...")
        await storage.upsert(test_data)
        print("✓ upsert test passed")

        # 2. Test get_by_id
        print("Testing get_by_id...")
        result = await storage.get_by_id("doc1")
        assert result is not None, "get_by_id should return a result"
        assert result.get("status") == DocStatus.PENDING.value, "Status should match"
        print("✓ get_by_id test passed")

        # 3. Test get_docs_by_status
        print("Testing get_docs_by_status...")
        pending_docs = await storage.get_docs_by_status(DocStatus.PENDING)
        assert len(pending_docs) >= 1, "Should have at least one pending document"
        assert "doc1" in pending_docs, "doc1 should be in pending status"

        processed_docs = await storage.get_docs_by_status(DocStatus.PROCESSED)
        assert len(processed_docs) >= 1, "Should have at least one processed document"
        assert "doc3" in processed_docs, "doc3 should be in processed status"
        print("✓ get_docs_by_status test passed")

        # 4. Test get_docs_by_track_id
        print("Testing get_docs_by_track_id...")
        track1_docs = await storage.get_docs_by_track_id("track1")
        assert len(track1_docs) >= 2, "Should have at least 2 documents with track1"
        assert "doc1" in track1_docs, "doc1 should be in track1"
        assert "doc2" in track1_docs, "doc2 should be in track1"
        print("✓ get_docs_by_track_id test passed")

        # 5. Test get_status_counts
        print("Testing get_status_counts...")
        status_counts = await storage.get_status_counts()
        assert isinstance(status_counts, dict), "Status counts should be a dict"
        assert status_counts.get(DocStatus.PENDING.value, 0) >= 1, "Should have at least one pending"
        assert status_counts.get(DocStatus.PROCESSED.value, 0) >= 1, "Should have at least one processed"
        print("✓ get_status_counts test passed")

        # 6. Test get_all_status_counts
        print("Testing get_all_status_counts...")
        all_counts = await storage.get_all_status_counts()
        assert isinstance(all_counts, dict), "All status counts should be a dict"
        assert sum(all_counts.values()) >= 4, "Should have at least 4 documents total"
        print("✓ get_all_status_counts test passed")

        # 7. Test get_docs_paginated (note: page_size has a minimum of 10 per the API)
        print("Testing get_docs_paginated...")
        page1, total = await storage.get_docs_paginated(page=1, page_size=10)
        assert len(page1) <= 10, "Page size should be respected"
        assert total >= 4, "Total should be at least 4"
        print(f"✓ get_docs_paginated test passed (page1: {len(page1)} items, total: {total})")

        # 8. Test get_docs_paginated with status filter
        print("Testing get_docs_paginated with status filter...")
        pending_page, pending_total = await storage.get_docs_paginated(
            status_filter=DocStatus.PENDING, page=1, page_size=10
        )
        assert pending_total >= 1, "Should have at least one pending document"
        print("✓ get_docs_paginated with status filter test passed")

        # 9. Test update status
        print("Testing status update...")
        updated_data = {
            "doc1": {
                "content_summary": doc1.content_summary,
                "content_length": doc1.content_length,
                "file_path": doc1.file_path,
                "status": DocStatus.PROCESSED.value,  # Update status
                "created_at": doc1.created_at,
                "updated_at": datetime.now().isoformat(),
                "track_id": doc1.track_id,
                "chunks_count": doc1.chunks_count,
                "chunks_list": doc1.chunks_list,
            }
        }
        await storage.upsert(updated_data)
        updated_result = await storage.get_by_id("doc1")
        assert updated_result.get("status") == DocStatus.PROCESSED.value, "Status should be updated"
        print("✓ status update test passed")

        print("\n✓ All DocStatus storage basic tests passed!")
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
    ASCIIColors.cyan(" " * 10 + "DocStatus Storage Test Program")
    ASCIIColors.cyan("=" * 60 + "\n")

    doc_status_storage_type = os.getenv("LIGHTRAG_DOC_STATUS_STORAGE", "JsonDocStatusStorage")
    ASCIIColors.cyan(f"Currently configured DocStatus storage type: {doc_status_storage_type}")
    ASCIIColors.cyan(
        f"Supported DocStatus storage types: {', '.join(STORAGE_IMPLEMENTATIONS['DOC_STATUS_STORAGE']['implementations'])}"
    )

    storage = await initialize_doc_status_storage()
    if not storage:
        ASCIIColors.red("Failed to initialize DocStatus storage. Exiting.")
        return

    try:
        # Clean data before running tests
        ASCIIColors.yellow("\nCleaning data before running tests...")
        await storage.drop()
        ASCIIColors.green("Data cleanup complete\n")

        # Run basic test
        ASCIIColors.cyan("\n=== Starting Basic DocStatus Test ===")
        result = await test_doc_status_basic(storage)

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
