"""
LanceDB unified storage implementation for LightRAG.

This module provides four storage backends using LanceDB:
- LanceDBKVStorage: Key-value storage using JSON blob columns
- LanceDBVectorStorage: Vector storage with native LanceDB vector search
- LanceDBGraphStorage: Graph storage using dual-table (nodes + edges) approach
- LanceDBDocStatusStorage: Document status storage with fixed schema

Environment Variables:
    LANCEDB_URI: Path to LanceDB database (default: "./lancedb")
    LANCEDB_WORKSPACE: Override workspace name for all LanceDB storage instances
"""

import json
import os
import re
import time
import asyncio
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Union, final

import numpy as np

from ..base import (
    BaseGraphStorage,
    BaseKVStorage,
    BaseVectorStorage,
    DocProcessingStatus,
    DocStatus,
    DocStatusStorage,
)
from ..utils import logger, compute_mdhash_id
from ..types import KnowledgeGraph, KnowledgeGraphNode, KnowledgeGraphEdge
from ..constants import GRAPH_FIELD_SEP
from ..kg.shared_storage import get_data_init_lock

import pipmaster as pm

if not pm.is_installed("lancedb"):
    pm.install("lancedb")

if not pm.is_installed("pyarrow"):
    pm.install("pyarrow")

import lancedb  # type: ignore
import pyarrow as pa  # type: ignore


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _escape_sql_string(s: str) -> str:
    """Escape single quotes for LanceDB SQL-like where clauses."""
    return s.replace("'", "''")


def _build_where_eq(field_name: str, value: str) -> str:
    """Build a WHERE clause for equality: field = 'value'."""
    return f"`{field_name}` = '{_escape_sql_string(value)}'"


def _build_where_in(field_name: str, values: list[str]) -> str:
    """Build a WHERE clause for IN: field IN ('v1', 'v2', ...)."""
    if not values:
        return "1 = 0"  # Always false
    escaped = [f"'{_escape_sql_string(v)}'" for v in values]
    return f"`{field_name}` IN ({', '.join(escaped)})"


async def get_or_create_table(
    db: lancedb.AsyncConnection, table_name: str, schema: pa.Schema
):
    """Open an existing LanceDB table or create a new empty one."""
    table_names = await db.table_names()
    if table_name in table_names:
        return await db.open_table(table_name)
    else:
        tbl = await db.create_table(table_name, schema=schema)
        logger.info(f"Created LanceDB table: {table_name}")
        return tbl


def _compute_effective_workspace(workspace: str, env_var: str = "LANCEDB_WORKSPACE"):
    """Compute effective workspace considering environment variable override."""
    lancedb_workspace = os.environ.get(env_var)
    if lancedb_workspace and lancedb_workspace.strip():
        effective_workspace = lancedb_workspace.strip()
        logger.info(
            f"Using {env_var} environment variable: '{effective_workspace}' "
            f"(overriding '{workspace}')"
        )
    else:
        effective_workspace = workspace
        if effective_workspace:
            logger.debug(f"Using passed workspace parameter: '{effective_workspace}'")
    return effective_workspace


def _build_final_namespace(effective_workspace: str, namespace: str) -> str:
    """Build final namespace with workspace prefix for data isolation."""
    if effective_workspace:
        return f"{effective_workspace}_{namespace}"
    return namespace


# ---------------------------------------------------------------------------
# Connection Manager
# ---------------------------------------------------------------------------


class ClientManager:
    """Singleton LanceDB connection manager with reference counting."""

    _instances: dict[str, Any] = {"db": None, "ref_count": 0}
    _lock = asyncio.Lock()

    @classmethod
    async def get_client(cls) -> lancedb.AsyncConnection:
        async with cls._lock:
            if cls._instances["db"] is None:
                uri = os.environ.get("LANCEDB_URI", "./lancedb")
                db = await lancedb.connect_async(uri)
                cls._instances["db"] = db
                cls._instances["ref_count"] = 0
                logger.info(f"Connected to LanceDB at: {uri}")
            cls._instances["ref_count"] += 1
            return cls._instances["db"]

    @classmethod
    async def release_client(cls, db):
        async with cls._lock:
            if db is not None and db is cls._instances["db"]:
                cls._instances["ref_count"] -= 1
                if cls._instances["ref_count"] == 0:
                    cls._instances["db"] = None
                    logger.debug("Released LanceDB connection (ref_count=0)")


# ---------------------------------------------------------------------------
# KV Schema
# ---------------------------------------------------------------------------

KV_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.utf8(), nullable=False),
        pa.field("data", pa.large_utf8()),
        pa.field("create_time", pa.int64()),
        pa.field("update_time", pa.int64()),
    ]
)


# ---------------------------------------------------------------------------
# LanceDBKVStorage
# ---------------------------------------------------------------------------


@final
@dataclass
class LanceDBKVStorage(BaseKVStorage):
    db: Any = field(default=None)
    _table: Any = field(default=None)

    def __init__(self, namespace, global_config, embedding_func, workspace=None):
        super().__init__(
            namespace=namespace,
            workspace=workspace or "",
            global_config=global_config,
            embedding_func=embedding_func,
        )
        self.__post_init__()

    def __post_init__(self):
        effective_workspace = _compute_effective_workspace(self.workspace)
        self.final_namespace = _build_final_namespace(
            effective_workspace, self.namespace
        )
        self.workspace = effective_workspace or ""
        self._table_name = self.final_namespace
        logger.debug(f"[{self.workspace}] LanceDB KV table name: '{self._table_name}'")

    async def initialize(self):
        async with get_data_init_lock():
            if self.db is None:
                self.db = await ClientManager.get_client()
            self._table = await get_or_create_table(
                self.db, self._table_name, KV_SCHEMA
            )
            logger.debug(
                f"[{self.workspace}] Use LanceDB as KV {self._table_name}"
            )

    async def finalize(self):
        if self.db is not None:
            await ClientManager.release_client(self.db)
            self.db = None
            self._table = None

    async def get_by_id(self, id: str) -> dict[str, Any] | None:
        try:
            results = (
                await self._table.query()
                .where(_build_where_eq("_id", id))
                .to_list()
            )
            if results:
                row = results[0]
                doc = json.loads(row["data"]) if row.get("data") else {}
                doc["_id"] = row["_id"]
                doc.setdefault("create_time", row.get("create_time", 0))
                doc.setdefault("update_time", row.get("update_time", 0))
                return doc
            return None
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_by_id: {e}")
            return None

    async def get_by_ids(self, ids: list[str]) -> list[dict[str, Any]]:
        if not ids:
            return []
        try:
            results = (
                await self._table.query()
                .where(_build_where_in("_id", ids))
                .to_list()
            )
            doc_map: dict[str, dict[str, Any]] = {}
            for row in results:
                doc = json.loads(row["data"]) if row.get("data") else {}
                doc["_id"] = row["_id"]
                doc.setdefault("create_time", row.get("create_time", 0))
                doc.setdefault("update_time", row.get("update_time", 0))
                doc_map[row["_id"]] = doc

            return [doc_map.get(id_val) for id_val in ids]
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_by_ids: {e}")
            return [None] * len(ids)

    async def filter_keys(self, keys: set[str]) -> set[str]:
        if not keys:
            return set()
        try:
            keys_list = list(keys)
            results = (
                await self._table.query()
                .where(_build_where_in("_id", keys_list))
                .select(["_id"])
                .to_list()
            )
            existing_ids = {row["_id"] for row in results}
            return keys - existing_ids
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in filter_keys: {e}")
            return keys

    async def upsert(self, data: dict[str, dict[str, Any]]) -> None:
        logger.debug(f"[{self.workspace}] Inserting {len(data)} to {self.namespace}")
        if not data:
            return

        current_time = int(time.time())

        # Fetch existing create_times to preserve them on updates
        ids_list = list(data.keys())
        existing_create_times: dict[str, int] = {}
        try:
            existing = (
                await self._table.query()
                .where(_build_where_in("_id", ids_list))
                .select(["_id", "create_time"])
                .to_list()
            )
            for row in existing:
                existing_create_times[row["_id"]] = row.get("create_time", current_time)
        except Exception:
            pass  # Table might be empty or query fails

        records = []
        for k, v in data.items():
            # For text_chunks namespace, ensure llm_cache_list field exists
            if self.namespace.endswith("text_chunks"):
                if "llm_cache_list" not in v:
                    v["llm_cache_list"] = []

            records.append(
                {
                    "_id": k,
                    "data": json.dumps(v, ensure_ascii=False, default=str),
                    "create_time": existing_create_times.get(k, current_time),
                    "update_time": current_time,
                }
            )

        try:
            (
                await self._table.merge_insert("_id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute(records)
            )
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in KV upsert: {e}")
            raise

    async def index_done_callback(self) -> None:
        # LanceDB handles persistence automatically
        pass

    async def is_empty(self) -> bool:
        try:
            count = await self._table.count_rows()
            return count == 0
        except Exception as e:
            logger.error(f"[{self.workspace}] Error checking if storage is empty: {e}")
            return True

    async def delete(self, ids: list[str]) -> None:
        if not ids:
            return
        if isinstance(ids, set):
            ids = list(ids)
        try:
            await self._table.delete(_build_where_in("_id", ids))
            logger.info(
                f"[{self.workspace}] Deleted {len(ids)} documents from {self.namespace}"
            )
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error deleting documents from {self.namespace}: {e}"
            )

    async def drop(self) -> dict[str, str]:
        try:
            count = await self._table.count_rows()
            if count > 0:
                await self._table.delete("1 = 1")
            logger.info(
                f"[{self.workspace}] Dropped {count} documents from KV {self._table_name}"
            )
            return {"status": "success", "message": f"{count} documents dropped"}
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error dropping KV {self._table_name}: {e}"
            )
            return {"status": "error", "message": str(e)}


# ---------------------------------------------------------------------------
# LanceDBVectorStorage
# ---------------------------------------------------------------------------


@final
@dataclass
class LanceDBVectorStorage(BaseVectorStorage):
    db: Any = field(default=None)
    _table: Any = field(default=None)
    _max_batch_size: int = field(default=10, init=False)

    def __init__(
        self, namespace, global_config, embedding_func, workspace=None, meta_fields=None
    ):
        super().__init__(
            namespace=namespace,
            workspace=workspace or "",
            global_config=global_config,
            embedding_func=embedding_func,
            meta_fields=meta_fields or set(),
        )
        self.__post_init__()

    def __post_init__(self):
        self._validate_embedding_func()

        effective_workspace = _compute_effective_workspace(self.workspace)
        self.final_namespace = _build_final_namespace(
            effective_workspace, self.namespace
        )
        self.workspace = effective_workspace or ""

        kwargs = self.global_config.get("vector_db_storage_cls_kwargs", {})
        cosine_threshold = kwargs.get("cosine_better_than_threshold")
        if cosine_threshold is None:
            raise ValueError(
                "cosine_better_than_threshold must be specified in "
                "vector_db_storage_cls_kwargs"
            )
        self.cosine_better_than_threshold = cosine_threshold
        self._metric = kwargs.get("lancedb_metric", "cosine")
        self._table_name = self.final_namespace
        self._max_batch_size = self.global_config.get("embedding_batch_num", 10)

    def _build_vector_schema(self) -> pa.Schema:
        """Build PyArrow schema for vector table dynamically based on meta_fields."""
        fields = [
            pa.field("_id", pa.utf8(), nullable=False),
            pa.field(
                "vector",
                pa.list_(pa.float32(), self.embedding_func.embedding_dim),
            ),
        ]
        # Add meta fields as utf8 columns
        for mf in sorted(self.meta_fields):
            fields.append(pa.field(mf, pa.utf8(), nullable=True))
        fields.append(pa.field("created_at", pa.int64()))
        return pa.schema(fields)

    async def initialize(self):
        async with get_data_init_lock():
            if self.db is None:
                self.db = await ClientManager.get_client()

            schema = self._build_vector_schema()
            self._table = await get_or_create_table(
                self.db, self._table_name, schema
            )
            logger.debug(
                f"[{self.workspace}] Use LanceDB as VDB {self._table_name}"
            )

    async def finalize(self):
        if self.db is not None:
            await ClientManager.release_client(self.db)
            self.db = None
            self._table = None

    async def query(
        self, query: str, top_k: int, query_embedding: list[float] = None
    ) -> list[dict[str, Any]]:
        if query_embedding is not None:
            if hasattr(query_embedding, "tolist"):
                query_vector = query_embedding.tolist()
            else:
                query_vector = list(query_embedding)
        else:
            embedding = await self.embedding_func([query], _priority=5)
            query_vector = embedding[0].tolist()

        try:
            search_builder = await self._table.search(query_vector)
            results = await search_builder.distance_type(self._metric).limit(top_k).to_list()
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in vector query: {e}")
            return []

        # LanceDB returns _distance for cosine metric (distance = 1 - similarity)
        # Filter by cosine threshold and format results
        formatted = []
        for doc in results:
            distance = doc.get("_distance", 1.0)
            score = 1.0 - distance  # Convert distance to cosine similarity
            if score >= self.cosine_better_than_threshold:
                result = {
                    k: v
                    for k, v in doc.items()
                    if k not in ("vector", "_distance", "_rowid")
                }
                result["id"] = doc["_id"]
                result["distance"] = score
                result["created_at"] = doc.get("created_at")
                formatted.append(result)
        return formatted

    async def upsert(self, data: dict[str, dict[str, Any]]) -> None:
        logger.debug(f"[{self.workspace}] Inserting {len(data)} to {self.namespace}")
        if not data:
            return

        current_time = int(time.time())

        list_data = [
            {
                "_id": k,
                "created_at": current_time,
                **{mf: str(v.get(mf, "")) for mf in self.meta_fields},
            }
            for k, v in data.items()
        ]
        contents = [v["content"] for v in data.values()]
        batches = [
            contents[i : i + self._max_batch_size]
            for i in range(0, len(contents), self._max_batch_size)
        ]

        embedding_tasks = [self.embedding_func(batch) for batch in batches]
        embeddings_list = await asyncio.gather(*embedding_tasks)
        embeddings = np.concatenate(embeddings_list)

        for i, d in enumerate(list_data):
            d["vector"] = np.array(embeddings[i], dtype=np.float32).tolist()

        try:
            (
                await self._table.merge_insert("_id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute(list_data)
            )
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in vector upsert: {e}")
            raise

        return list_data

    async def index_done_callback(self) -> None:
        pass

    async def delete(self, ids: list[str]) -> None:
        if not ids:
            return
        if isinstance(ids, set):
            ids = list(ids)
        try:
            await self._table.delete(_build_where_in("_id", ids))
            logger.debug(
                f"[{self.workspace}] Deleted {len(ids)} vectors from {self.namespace}"
            )
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error deleting vectors from {self.namespace}: {e}"
            )

    async def delete_entity(self, entity_name: str) -> None:
        try:
            entity_id = compute_mdhash_id(entity_name, prefix="ent-")
            logger.debug(
                f"[{self.workspace}] Deleting entity {entity_name} with ID {entity_id}"
            )
            await self._table.delete(_build_where_eq("_id", entity_id))
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error deleting entity {entity_name}: {e}"
            )

    async def delete_entity_relation(self, entity_name: str) -> None:
        try:
            # Find relations where entity appears as source or target
            src_clause = _build_where_eq("src_id", entity_name)
            tgt_clause = _build_where_eq("tgt_id", entity_name)
            where = f"({src_clause}) OR ({tgt_clause})"

            results = (
                await self._table.query()
                .where(where)
                .select(["_id"])
                .to_list()
            )
            if not results:
                logger.debug(
                    f"[{self.workspace}] No relations found for entity {entity_name}"
                )
                return

            relation_ids = [r["_id"] for r in results]
            logger.debug(
                f"[{self.workspace}] Found {len(relation_ids)} relations for entity {entity_name}"
            )
            await self._table.delete(_build_where_in("_id", relation_ids))
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error deleting relations for {entity_name}: {e}"
            )

    async def get_by_id(self, id: str) -> dict[str, Any] | None:
        try:
            results = (
                await self._table.query()
                .where(_build_where_eq("_id", id))
                .to_list()
            )
            if results:
                result_dict = dict(results[0])
                # Remove internal fields
                result_dict.pop("_rowid", None)
                if "_id" in result_dict and "id" not in result_dict:
                    result_dict["id"] = result_dict["_id"]
                return result_dict
            return None
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error retrieving vector data for ID {id}: {e}"
            )
            return None

    async def get_by_ids(self, ids: list[str]) -> list[dict[str, Any]]:
        if not ids:
            return []
        try:
            results = (
                await self._table.query()
                .where(_build_where_in("_id", ids))
                .to_list()
            )
            formatted_map: dict[str, dict[str, Any]] = {}
            for row in results:
                result_dict = dict(row)
                result_dict.pop("_rowid", None)
                if "_id" in result_dict and "id" not in result_dict:
                    result_dict["id"] = result_dict["_id"]
                formatted_map[result_dict["_id"]] = result_dict

            return [formatted_map.get(id_val) for id_val in ids]
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error retrieving vector data for IDs: {e}"
            )
            return []

    async def get_vectors_by_ids(self, ids: list[str]) -> dict[str, list[float]]:
        if not ids:
            return {}
        try:
            results = (
                await self._table.query()
                .where(_build_where_in("_id", ids))
                .select(["_id", "vector"])
                .to_list()
            )
            vectors_dict = {}
            for row in results:
                if row and "vector" in row and "_id" in row:
                    vec = row["vector"]
                    if hasattr(vec, "tolist"):
                        vec = vec.tolist()
                    vectors_dict[row["_id"]] = vec
            return vectors_dict
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error retrieving vectors by IDs: {e}"
            )
            return {}

    async def drop(self) -> dict[str, str]:
        try:
            count = await self._table.count_rows()
            if count > 0:
                await self._table.delete("1 = 1")
            logger.info(
                f"[{self.workspace}] Dropped {count} vectors from {self._table_name}"
            )
            return {
                "status": "success",
                "message": f"{count} documents dropped",
            }
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error dropping vector storage {self._table_name}: {e}"
            )
            return {"status": "error", "message": str(e)}


# ---------------------------------------------------------------------------
# Graph Storage Schemas
# ---------------------------------------------------------------------------

GRAPH_NODE_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.utf8(), nullable=False),
        pa.field("entity_id", pa.utf8()),  # Node ID (same as _id, for compatibility)
        pa.field("entity_type", pa.utf8()),
        pa.field("description", pa.utf8()),
        pa.field("keywords", pa.utf8()),  # Keywords field
        pa.field("source_id", pa.utf8()),
        pa.field("file_path", pa.utf8()),
        pa.field("source_ids", pa.utf8()),  # JSON-encoded list
        pa.field("created_at", pa.int64()),
    ]
)

GRAPH_EDGE_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.utf8(), nullable=False),  # Deterministic composite key
        pa.field("source_node_id", pa.utf8()),
        pa.field("target_node_id", pa.utf8()),
        pa.field("relationship", pa.utf8()),
        pa.field("weight", pa.float64()),
        pa.field("keywords", pa.utf8()),
        pa.field("description", pa.utf8()),
        pa.field("source_id", pa.utf8()),
        pa.field("file_path", pa.utf8()),
        pa.field("source_ids", pa.utf8()),  # JSON-encoded list
        pa.field("created_at", pa.int64()),
    ]
)

# Separator used to build deterministic edge keys
_EDGE_KEY_SEP = "||"


def _make_edge_id(src: str, tgt: str) -> str:
    """Create a deterministic edge ID regardless of direction (undirected)."""
    a, b = sorted([src, tgt])
    return f"{a}{_EDGE_KEY_SEP}{b}"


# ---------------------------------------------------------------------------
# LanceDBGraphStorage
# ---------------------------------------------------------------------------


@final
@dataclass
class LanceDBGraphStorage(BaseGraphStorage):
    db: Any = field(default=None)
    _node_table: Any = field(default=None)
    _edge_table: Any = field(default=None)

    def __init__(self, namespace, global_config, embedding_func, workspace=None):
        super().__init__(
            namespace=namespace,
            workspace=workspace or "",
            global_config=global_config,
            embedding_func=embedding_func,
        )
        effective_workspace = _compute_effective_workspace(self.workspace)
        self.final_namespace = _build_final_namespace(
            effective_workspace, self.namespace
        )
        self.workspace = effective_workspace or ""
        self._node_table_name = self.final_namespace
        self._edge_table_name = f"{self.final_namespace}_edges"
        logger.debug(
            f"[{self.workspace}] LanceDB Graph tables: "
            f"nodes='{self._node_table_name}', edges='{self._edge_table_name}'"
        )

    async def initialize(self):
        async with get_data_init_lock():
            if self.db is None:
                self.db = await ClientManager.get_client()

            self._node_table = await get_or_create_table(
                self.db, self._node_table_name, GRAPH_NODE_SCHEMA
            )
            self._edge_table = await get_or_create_table(
                self.db, self._edge_table_name, GRAPH_EDGE_SCHEMA
            )
            logger.debug(
                f"[{self.workspace}] Use LanceDB as KG {self._node_table_name}"
            )

    async def finalize(self):
        if self.db is not None:
            await ClientManager.release_client(self.db)
            self.db = None
            self._node_table = None
            self._edge_table = None

    # ------------------------------------------------------------------
    # Basic Queries
    # ------------------------------------------------------------------

    async def has_node(self, node_id: str) -> bool:
        try:
            results = (
                await self._node_table.query()
                .where(_build_where_eq("_id", node_id))
                .select(["_id"])
                .to_list()
            )
            return len(results) > 0
        except Exception:
            return False

    async def has_edge(self, source_node_id: str, target_node_id: str) -> bool:
        edge_id = _make_edge_id(source_node_id, target_node_id)
        try:
            results = (
                await self._edge_table.query()
                .where(_build_where_eq("_id", edge_id))
                .select(["_id"])
                .to_list()
            )
            return len(results) > 0
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Degrees
    # ------------------------------------------------------------------

    async def node_degree(self, node_id: str) -> int:
        try:
            src_clause = _build_where_eq("source_node_id", node_id)
            tgt_clause = _build_where_eq("target_node_id", node_id)
            where = f"({src_clause}) OR ({tgt_clause})"
            results = (
                await self._edge_table.query()
                .where(where)
                .select(["_id"])
                .to_list()
            )
            return len(results)
        except Exception:
            return 0

    async def edge_degree(self, src_id: str, tgt_id: str) -> int:
        src_degree = await self.node_degree(src_id)
        tgt_degree = await self.node_degree(tgt_id)
        return src_degree + tgt_degree

    # ------------------------------------------------------------------
    # Getters
    # ------------------------------------------------------------------

    async def get_node(self, node_id: str) -> dict[str, str] | None:
        try:
            results = (
                await self._node_table.query()
                .where(_build_where_eq("_id", node_id))
                .to_list()
            )
            if results:
                node = dict(results[0])
                node.pop("_rowid", None)
                return node
            return None
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_node: {e}")
            return None

    async def get_edge(
        self, source_node_id: str, target_node_id: str
    ) -> dict[str, str] | None:
        edge_id = _make_edge_id(source_node_id, target_node_id)
        try:
            results = (
                await self._edge_table.query()
                .where(_build_where_eq("_id", edge_id))
                .to_list()
            )
            if results:
                edge = dict(results[0])
                edge.pop("_rowid", None)
                return edge
            return None
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_edge: {e}")
            return None

    async def get_node_edges(
        self, source_node_id: str
    ) -> list[tuple[str, str]] | None:
        try:
            src_clause = _build_where_eq("source_node_id", source_node_id)
            tgt_clause = _build_where_eq("target_node_id", source_node_id)
            where = f"({src_clause}) OR ({tgt_clause})"
            results = (
                await self._edge_table.query()
                .where(where)
                .select(["source_node_id", "target_node_id"])
                .to_list()
            )
            return [
                (e["source_node_id"], e["target_node_id"]) for e in results
            ]
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_node_edges: {e}")
            return []

    async def get_nodes_batch(self, node_ids: list[str]) -> dict[str, dict]:
        if not node_ids:
            return {}
        try:
            results = (
                await self._node_table.query()
                .where(_build_where_in("_id", node_ids))
                .to_list()
            )
            result_dict = {}
            for row in results:
                node = dict(row)
                node.pop("_rowid", None)
                result_dict[node["_id"]] = node
            return result_dict
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_nodes_batch: {e}")
            return {}

    async def node_degrees_batch(self, node_ids: list[str]) -> dict[str, int]:
        if not node_ids:
            return {}
        try:
            src_clause = _build_where_in("source_node_id", node_ids)
            tgt_clause = _build_where_in("target_node_id", node_ids)
            where = f"({src_clause}) OR ({tgt_clause})"
            results = (
                await self._edge_table.query()
                .where(where)
                .select(["source_node_id", "target_node_id"])
                .to_list()
            )
            node_ids_set = set(node_ids)
            counter: Counter = Counter()
            for edge in results:
                src = edge["source_node_id"]
                tgt = edge["target_node_id"]
                if src in node_ids_set:
                    counter[src] += 1
                if tgt in node_ids_set:
                    counter[tgt] += 1
            return dict(counter)
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in node_degrees_batch: {e}")
            return {}

    async def get_nodes_edges_batch(
        self, node_ids: list[str]
    ) -> dict[str, list[tuple[str, str]]]:
        if not node_ids:
            return {}
        result = {nid: [] for nid in node_ids}
        try:
            node_ids_set = set(node_ids)
            src_clause = _build_where_in("source_node_id", node_ids)
            tgt_clause = _build_where_in("target_node_id", node_ids)
            where = f"({src_clause}) OR ({tgt_clause})"
            edges = (
                await self._edge_table.query()
                .where(where)
                .select(["source_node_id", "target_node_id"])
                .to_list()
            )
            for edge in edges:
                src = edge["source_node_id"]
                tgt = edge["target_node_id"]
                pair = (src, tgt)
                if src in node_ids_set:
                    result[src].append(pair)
                if tgt in node_ids_set:
                    result[tgt].append(pair)
            return result
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error in get_nodes_edges_batch: {e}"
            )
            return result

    # ------------------------------------------------------------------
    # Upserts
    # ------------------------------------------------------------------

    async def upsert_node(self, node_id: str, node_data: dict[str, str]) -> None:
        current_time = int(time.time())

        source_ids_list = []
        if node_data.get("source_id", ""):
            source_ids_list = node_data["source_id"].split(GRAPH_FIELD_SEP)

        record = {
            "_id": node_id,
            "entity_id": str(node_data.get("entity_id", node_id)),
            "entity_type": str(node_data.get("entity_type", "")),
            "description": str(node_data.get("description", "")),
            "keywords": str(node_data.get("keywords", "")),
            "source_id": str(node_data.get("source_id", "")),
            "file_path": str(node_data.get("file_path", "")),
            "source_ids": json.dumps(source_ids_list),
            "created_at": current_time,
        }

        try:
            (
                await self._node_table.merge_insert("_id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute([record])
            )
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error upserting node {node_id}: {e}"
            )
            raise

    async def upsert_edge(
        self, source_node_id: str, target_node_id: str, edge_data: dict[str, str]
    ) -> None:
        # Ensure source node exists — only create a placeholder when missing,
        # never overwrite an existing node's data with empty fields.
        if not await self.has_node(source_node_id):
            await self.upsert_node(source_node_id, {"entity_id": source_node_id})

        current_time = int(time.time())
        edge_id = _make_edge_id(source_node_id, target_node_id)

        source_ids_list = []
        if edge_data.get("source_id", ""):
            source_ids_list = edge_data["source_id"].split(GRAPH_FIELD_SEP)

        weight_val = edge_data.get("weight", 1.0)
        try:
            weight_val = float(weight_val)
        except (ValueError, TypeError):
            weight_val = 1.0

        record = {
            "_id": edge_id,
            "source_node_id": source_node_id,
            "target_node_id": target_node_id,
            "relationship": str(edge_data.get("relationship", "")),
            "weight": weight_val,
            "keywords": str(edge_data.get("keywords", "")),
            "description": str(edge_data.get("description", "")),
            "source_id": str(edge_data.get("source_id", "")),
            "file_path": str(edge_data.get("file_path", "")),
            "source_ids": json.dumps(source_ids_list),
            "created_at": current_time,
        }

        try:
            (
                await self._edge_table.merge_insert("_id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute([record])
            )
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error upserting edge {source_node_id}->{target_node_id}: {e}"
            )
            raise

    # ------------------------------------------------------------------
    # Deletion
    # ------------------------------------------------------------------

    async def delete_node(self, node_id: str) -> None:
        try:
            # Remove all edges referencing this node
            src_clause = _build_where_eq("source_node_id", node_id)
            tgt_clause = _build_where_eq("target_node_id", node_id)
            await self._edge_table.delete(f"({src_clause}) OR ({tgt_clause})")
            # Remove the node
            await self._node_table.delete(_build_where_eq("_id", node_id))
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error deleting node {node_id}: {e}"
            )

    async def remove_nodes(self, nodes: list[str]) -> None:
        logger.info(f"[{self.workspace}] Deleting {len(nodes)} nodes")
        if not nodes:
            return
        try:
            # Remove edges referencing any of these nodes
            src_clause = _build_where_in("source_node_id", nodes)
            tgt_clause = _build_where_in("target_node_id", nodes)
            await self._edge_table.delete(f"({src_clause}) OR ({tgt_clause})")
            # Remove nodes
            await self._node_table.delete(_build_where_in("_id", nodes))
            logger.debug(f"[{self.workspace}] Successfully deleted nodes: {nodes}")
        except Exception as e:
            logger.error(f"[{self.workspace}] Error removing nodes: {e}")

    async def remove_edges(self, edges: list[tuple[str, str]]) -> None:
        logger.info(f"[{self.workspace}] Deleting {len(edges)} edges")
        if not edges:
            return
        try:
            edge_ids = [_make_edge_id(src, tgt) for src, tgt in edges]
            await self._edge_table.delete(_build_where_in("_id", edge_ids))
            logger.debug(f"[{self.workspace}] Successfully deleted {len(edges)} edges")
        except Exception as e:
            logger.error(f"[{self.workspace}] Error removing edges: {e}")

    # ------------------------------------------------------------------
    # Query / Knowledge Graph
    # ------------------------------------------------------------------

    async def get_all_labels(self) -> list[str]:
        try:
            results = (
                await self._node_table.query()
                .select(["_id"])
                .to_list()
            )
            labels = sorted([r["_id"] for r in results])
            return labels
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_all_labels: {e}")
            return []

    def _construct_graph_node(
        self, node_id: str, node_data: dict
    ) -> KnowledgeGraphNode:
        return KnowledgeGraphNode(
            id=node_id,
            labels=[node_id],
            properties={
                k: v
                for k, v in node_data.items()
                if k not in ("_id", "source_ids", "_rowid")
            },
        )

    def _construct_graph_edge(
        self, edge_id: str, edge: dict
    ) -> KnowledgeGraphEdge:
        return KnowledgeGraphEdge(
            id=edge_id,
            type=edge.get("relationship", ""),
            source=edge["source_node_id"],
            target=edge["target_node_id"],
            properties={
                k: v
                for k, v in edge.items()
                if k
                not in (
                    "_id",
                    "source_node_id",
                    "target_node_id",
                    "relationship",
                    "source_ids",
                    "_rowid",
                )
            },
        )

    async def _bidirectional_bfs_nodes(
        self,
        node_labels: list[str],
        seen_nodes: set[str],
        result: KnowledgeGraph,
        depth: int,
        max_depth: int,
        max_nodes: int,
    ) -> KnowledgeGraph:
        """Perform BFS traversal from given nodes, collecting nodes up to max_depth."""
        if depth > max_depth or len(result.nodes) > max_nodes:
            return result

        # Fetch node data for the current layer
        if node_labels:
            nodes_data = (
                await self._node_table.query()
                .where(_build_where_in("_id", node_labels))
                .to_list()
            )
            for node in nodes_data:
                node_id = node["_id"]
                if node_id not in seen_nodes:
                    seen_nodes.add(node_id)
                    result.nodes.append(
                        self._construct_graph_node(node_id, dict(node))
                    )
                    if len(result.nodes) > max_nodes:
                        return result

        # Find neighbors via edges
        src_clause = _build_where_in("source_node_id", node_labels)
        tgt_clause = _build_where_in("target_node_id", node_labels)
        where = f"({src_clause}) OR ({tgt_clause})"
        edges = (
            await self._edge_table.query()
            .where(where)
            .select(["source_node_id", "target_node_id"])
            .to_list()
        )

        neighbor_nodes = []
        for edge in edges:
            if edge["source_node_id"] not in seen_nodes:
                neighbor_nodes.append(edge["source_node_id"])
            if edge["target_node_id"] not in seen_nodes:
                neighbor_nodes.append(edge["target_node_id"])

        if neighbor_nodes:
            result = await self._bidirectional_bfs_nodes(
                neighbor_nodes, seen_nodes, result, depth + 1, max_depth, max_nodes
            )

        return result

    async def get_knowledge_graph_all_by_degree(
        self, max_depth: int, max_nodes: int
    ) -> KnowledgeGraph:
        """Get knowledge graph for all nodes, prioritized by degree."""
        result = KnowledgeGraph()
        seen_edges = set()

        try:
            total_node_count = await self._node_table.count_rows()
            result.is_truncated = total_node_count > max_nodes

            if result.is_truncated:
                # Get all edges to compute degrees
                all_edges = (
                    await self._edge_table.query()
                    .select(["source_node_id", "target_node_id"])
                    .to_list()
                )
                degree_counter: Counter = Counter()
                for edge in all_edges:
                    degree_counter[edge["source_node_id"]] += 1
                    degree_counter[edge["target_node_id"]] += 1

                # Pick top max_nodes nodes by degree
                top_nodes = [
                    nid for nid, _ in degree_counter.most_common(max_nodes)
                ]
                node_ids_set = set(top_nodes)

                # Fetch those nodes
                nodes = (
                    await self._node_table.query()
                    .where(_build_where_in("_id", top_nodes))
                    .to_list()
                )
                for node in nodes:
                    result.nodes.append(
                        self._construct_graph_node(node["_id"], dict(node))
                    )

                # Fetch edges between these nodes
                edge_results = await self._fetch_edges_between_nodes(top_nodes)
            else:
                # Fetch all nodes
                nodes = await self._node_table.query().to_list()
                for node in nodes:
                    result.nodes.append(
                        self._construct_graph_node(node["_id"], dict(node))
                    )
                # Fetch all edges
                edge_results = await self._edge_table.query().to_list()

            for edge in edge_results:
                edge_id = f"{edge['source_node_id']}-{edge['target_node_id']}"
                if edge_id not in seen_edges:
                    seen_edges.add(edge_id)
                    result.edges.append(
                        self._construct_graph_edge(edge_id, dict(edge))
                    )

        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error in get_knowledge_graph_all_by_degree: {e}"
            )

        return result

    async def _fetch_edges_between_nodes(self, node_ids: list[str]) -> list[dict]:
        """Fetch edges where both source and target are in node_ids."""
        if not node_ids:
            return []
        try:
            # Get edges where source is in node_ids
            src_clause = _build_where_in("source_node_id", node_ids)
            tgt_clause = _build_where_in("target_node_id", node_ids)
            where = f"({src_clause}) AND ({tgt_clause})"
            return await self._edge_table.query().where(where).to_list()
        except Exception:
            return []

    async def get_knowledge_graph(
        self,
        node_label: str,
        max_depth: int = 3,
        max_nodes: int = None,
    ) -> KnowledgeGraph:
        if max_nodes is None:
            max_nodes = self.global_config.get("max_graph_nodes", 1000)
        else:
            max_nodes = min(
                max_nodes, self.global_config.get("max_graph_nodes", 1000)
            )

        result = KnowledgeGraph()
        start = time.perf_counter()

        try:
            if node_label == "*":
                result = await self.get_knowledge_graph_all_by_degree(
                    max_depth, max_nodes
                )
            else:
                # Bidirectional BFS
                seen_nodes: set[str] = set()
                seen_edges: set[str] = set()
                result = await self._bidirectional_bfs_nodes(
                    [node_label], seen_nodes, result, 0, max_depth, max_nodes
                )

                # Fetch edges among all discovered nodes
                all_node_ids = list(seen_nodes)
                if all_node_ids:
                    edges = await self._fetch_edges_between_nodes(all_node_ids)
                    for edge in edges:
                        edge_id = (
                            f"{edge['source_node_id']}-{edge['target_node_id']}"
                        )
                        if edge_id not in seen_edges:
                            seen_edges.add(edge_id)
                            result.edges.append(
                                self._construct_graph_edge(edge_id, dict(edge))
                            )

            duration = time.perf_counter() - start
            logger.info(
                f"[{self.workspace}] Subgraph query in {duration:.4f}s | "
                f"Nodes: {len(result.nodes)} | Edges: {len(result.edges)} | "
                f"Truncated: {result.is_truncated}"
            )
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_knowledge_graph: {e}")

        return result

    async def get_all_nodes(self) -> list[dict]:
        try:
            results = await self._node_table.query().to_list()
            nodes = []
            for node in results:
                node_dict = dict(node)
                node_dict.pop("_rowid", None)
                node_dict["id"] = node_dict.get("_id")
                nodes.append(node_dict)
            return nodes
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_all_nodes: {e}")
            return []

    async def get_all_edges(self) -> list[dict]:
        try:
            results = await self._edge_table.query().to_list()
            edges = []
            for edge in results:
                edge_dict = dict(edge)
                edge_dict.pop("_rowid", None)
                edge_dict["source"] = edge_dict.get("source_node_id")
                edge_dict["target"] = edge_dict.get("target_node_id")
                edges.append(edge_dict)
            return edges
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_all_edges: {e}")
            return []

    async def get_popular_labels(self, limit: int = 300) -> list[str]:
        try:
            # Get all edges and compute node degrees in application layer
            all_edges = (
                await self._edge_table.query()
                .select(["source_node_id", "target_node_id"])
                .to_list()
            )
            degree_counter: Counter = Counter()
            for edge in all_edges:
                degree_counter[edge["source_node_id"]] += 1
                degree_counter[edge["target_node_id"]] += 1

            labels = [nid for nid, _ in degree_counter.most_common(limit)]
            logger.debug(
                f"[{self.workspace}] Retrieved {len(labels)} popular labels (limit: {limit})"
            )
            return labels
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error getting popular labels: {e}"
            )
            return []

    async def search_labels(self, query: str, limit: int = 50) -> list[str]:
        query_strip = query.strip()
        if not query_strip:
            return []

        try:
            node_count = await self._node_table.count_rows()
            if node_count == 0:
                return []
        except Exception:
            return []

        try:
            # Use LIKE for case-insensitive substring matching
            escaped_query = _escape_sql_string(query_strip)
            # LanceDB supports LIKE with % wildcards
            where = f"`_id` LIKE '%{escaped_query}%'"
            results = (
                await self._node_table.query()
                .where(where)
                .select(["_id"])
                .to_list()
            )

            labels = [r["_id"] for r in results]

            # Sort: exact match first, then starts-with, then contains
            def sort_key(label):
                label_lower = label.lower()
                query_lower = query_strip.lower()
                if label_lower == query_lower:
                    return (0, label_lower)
                elif label_lower.startswith(query_lower):
                    return (1, label_lower)
                else:
                    return (2, label_lower)

            labels.sort(key=sort_key)
            labels = labels[:limit]

            logger.debug(
                f"[{self.workspace}] search_labels returned {len(labels)} results"
            )
            return labels
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in search_labels: {e}")
            return []

    async def index_done_callback(self) -> None:
        pass

    async def drop(self) -> dict[str, str]:
        try:
            node_count = await self._node_table.count_rows()
            edge_count = await self._edge_table.count_rows()
            if node_count > 0:
                await self._node_table.delete("1 = 1")
            if edge_count > 0:
                await self._edge_table.delete("1 = 1")
            logger.info(
                f"[{self.workspace}] Dropped {node_count} nodes and "
                f"{edge_count} edges from graph {self._node_table_name}"
            )
            return {
                "status": "success",
                "message": f"{node_count} nodes and {edge_count} edges dropped",
            }
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error dropping graph {self._node_table_name}: {e}"
            )
            return {"status": "error", "message": str(e)}


# ---------------------------------------------------------------------------
# DocStatus Schema
# ---------------------------------------------------------------------------

DOC_STATUS_SCHEMA = pa.schema(
    [
        pa.field("_id", pa.utf8(), nullable=False),
        pa.field("content_summary", pa.utf8()),
        pa.field("content_length", pa.int64()),
        pa.field("file_path", pa.utf8()),
        pa.field("status", pa.utf8()),
        pa.field("created_at", pa.utf8()),
        pa.field("updated_at", pa.utf8()),
        pa.field("track_id", pa.utf8()),
        pa.field("chunks_count", pa.int64()),
        pa.field("chunks_list", pa.utf8()),  # JSON-encoded list
        pa.field("error_msg", pa.utf8()),
        pa.field("metadata", pa.utf8()),  # JSON-encoded dict
        pa.field("multimodal_processed", pa.utf8()),  # "true"/"false"/""
    ]
)


# ---------------------------------------------------------------------------
# LanceDBDocStatusStorage
# ---------------------------------------------------------------------------


@final
@dataclass
class LanceDBDocStatusStorage(DocStatusStorage):
    db: Any = field(default=None)
    _table: Any = field(default=None)

    def __init__(self, namespace, global_config, embedding_func, workspace=None):
        super().__init__(
            namespace=namespace,
            workspace=workspace or "",
            global_config=global_config,
            embedding_func=embedding_func,
        )
        self.__post_init__()

    def __post_init__(self):
        effective_workspace = _compute_effective_workspace(self.workspace)
        self.final_namespace = _build_final_namespace(
            effective_workspace, self.namespace
        )
        self.workspace = effective_workspace or ""
        self._table_name = self.final_namespace
        logger.debug(
            f"[{self.workspace}] LanceDB DocStatus table: '{self._table_name}'"
        )

    async def initialize(self):
        async with get_data_init_lock():
            if self.db is None:
                self.db = await ClientManager.get_client()
            self._table = await get_or_create_table(
                self.db, self._table_name, DOC_STATUS_SCHEMA
            )
            logger.debug(
                f"[{self.workspace}] Use LanceDB as DocStatus {self._table_name}"
            )

    async def finalize(self):
        if self.db is not None:
            await ClientManager.release_client(self.db)
            self.db = None
            self._table = None

    def _prepare_doc_status_data(self, doc: dict[str, Any]) -> dict[str, Any]:
        """Normalize a LanceDB row to DocProcessingStatus-compatible dict."""
        data = dict(doc)
        data.pop("_id", None)
        data.pop("_rowid", None)
        # Remove deprecated content field
        data.pop("content", None)

        if "file_path" not in data or not data["file_path"]:
            data["file_path"] = "no-file-path"

        # Parse JSON-encoded fields
        if isinstance(data.get("chunks_list"), str):
            try:
                data["chunks_list"] = json.loads(data["chunks_list"])
            except (json.JSONDecodeError, TypeError):
                data["chunks_list"] = []
        if data.get("chunks_list") is None:
            data["chunks_list"] = []

        if isinstance(data.get("metadata"), str):
            try:
                data["metadata"] = json.loads(data["metadata"])
            except (json.JSONDecodeError, TypeError):
                data["metadata"] = {}
        if data.get("metadata") is None:
            data["metadata"] = {}

        if data.get("error_msg") is None:
            data["error_msg"] = None

        # Handle multimodal_processed
        mp = data.get("multimodal_processed")
        if mp == "true":
            data["multimodal_processed"] = True
        elif mp == "false":
            data["multimodal_processed"] = False
        else:
            data["multimodal_processed"] = None

        # Backward compatibility: migrate legacy 'error' field
        if "error" in data:
            if not data.get("error_msg"):
                data["error_msg"] = data.pop("error")
            else:
                data.pop("error", None)

        # Ensure chunks_count is int or None
        if data.get("chunks_count") is not None:
            try:
                data["chunks_count"] = int(data["chunks_count"])
            except (ValueError, TypeError):
                data["chunks_count"] = None

        # Ensure content_length is int
        if data.get("content_length") is not None:
            try:
                data["content_length"] = int(data["content_length"])
            except (ValueError, TypeError):
                data["content_length"] = 0

        return data

    def _doc_to_record(self, doc_id: str, v: dict[str, Any]) -> dict[str, Any]:
        """Convert a doc status dict to a LanceDB record."""
        chunks_list = v.get("chunks_list", [])
        if isinstance(chunks_list, list):
            chunks_list_str = json.dumps(chunks_list)
        else:
            chunks_list_str = str(chunks_list)

        metadata = v.get("metadata", {})
        if isinstance(metadata, dict):
            metadata_str = json.dumps(metadata, ensure_ascii=False, default=str)
        else:
            metadata_str = str(metadata)

        mp = v.get("multimodal_processed")
        if mp is True:
            mp_str = "true"
        elif mp is False:
            mp_str = "false"
        else:
            mp_str = ""

        chunks_count = v.get("chunks_count")
        if chunks_count is not None:
            try:
                chunks_count = int(chunks_count)
            except (ValueError, TypeError):
                chunks_count = 0
        else:
            chunks_count = 0

        content_length = v.get("content_length", 0)
        try:
            content_length = int(content_length)
        except (ValueError, TypeError):
            content_length = 0

        return {
            "_id": doc_id,
            "content_summary": str(v.get("content_summary", "")),
            "content_length": content_length,
            "file_path": str(v.get("file_path", "")),
            "status": v["status"].value if hasattr(v.get("status"), "value") else str(v.get("status", "")),
            "created_at": str(v.get("created_at", "")),
            "updated_at": str(v.get("updated_at", "")),
            "track_id": str(v.get("track_id", "") or ""),
            "chunks_count": chunks_count,
            "chunks_list": chunks_list_str,
            "error_msg": str(v.get("error_msg", "") or ""),
            "metadata": metadata_str,
            "multimodal_processed": mp_str,
        }

    async def get_by_id(self, id: str) -> dict[str, Any] | None:
        try:
            results = (
                await self._table.query()
                .where(_build_where_eq("_id", id))
                .to_list()
            )
            if results:
                row = dict(results[0])
                row.pop("_rowid", None)
                return row
            return None
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in DocStatus get_by_id: {e}")
            return None

    async def get_by_ids(self, ids: list[str]) -> list[dict[str, Any]]:
        if not ids:
            return []
        try:
            results = (
                await self._table.query()
                .where(_build_where_in("_id", ids))
                .to_list()
            )
            doc_map: dict[str, dict[str, Any]] = {}
            for row in results:
                d = dict(row)
                d.pop("_rowid", None)
                doc_map[d["_id"]] = d

            return [doc_map.get(id_val) for id_val in ids]
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in DocStatus get_by_ids: {e}")
            return [None] * len(ids)

    async def filter_keys(self, keys: set[str]) -> set[str]:
        if not keys:
            return set()
        try:
            results = (
                await self._table.query()
                .where(_build_where_in("_id", list(keys)))
                .select(["_id"])
                .to_list()
            )
            existing_ids = {row["_id"] for row in results}
            return keys - existing_ids
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in DocStatus filter_keys: {e}")
            return keys

    async def upsert(self, data: dict[str, dict[str, Any]]) -> None:
        logger.debug(f"[{self.workspace}] Inserting {len(data)} to {self.namespace}")
        if not data:
            return

        records = []
        for k, v in data.items():
            # Ensure chunks_list field exists
            if "chunks_list" not in v:
                v["chunks_list"] = []
            records.append(self._doc_to_record(k, v))

        try:
            (
                await self._table.merge_insert("_id")
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute(records)
            )
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in DocStatus upsert: {e}")
            raise

    async def get_status_counts(self) -> dict[str, int]:
        try:
            results = (
                await self._table.query()
                .select(["status"])
                .to_list()
            )
            counts: dict[str, int] = {}
            for row in results:
                status = row.get("status", "unknown")
                counts[status] = counts.get(status, 0) + 1
            return counts
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_status_counts: {e}")
            return {}

    async def get_docs_by_status(
        self, status: DocStatus
    ) -> dict[str, DocProcessingStatus]:
        try:
            results = (
                await self._table.query()
                .where(_build_where_eq("status", status.value))
                .to_list()
            )
            processed = {}
            for row in results:
                try:
                    doc_id = row["_id"]
                    data = self._prepare_doc_status_data(dict(row))
                    processed[doc_id] = DocProcessingStatus(**data)
                except (KeyError, TypeError) as e:
                    logger.error(
                        f"[{self.workspace}] Missing field for document {row.get('_id')}: {e}"
                    )
            return processed
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_docs_by_status: {e}")
            return {}

    async def get_docs_by_track_id(
        self, track_id: str
    ) -> dict[str, DocProcessingStatus]:
        try:
            results = (
                await self._table.query()
                .where(_build_where_eq("track_id", track_id))
                .to_list()
            )
            processed = {}
            for row in results:
                try:
                    doc_id = row["_id"]
                    data = self._prepare_doc_status_data(dict(row))
                    processed[doc_id] = DocProcessingStatus(**data)
                except (KeyError, TypeError) as e:
                    logger.error(
                        f"[{self.workspace}] Missing field for document {row.get('_id')}: {e}"
                    )
            return processed
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_docs_by_track_id: {e}")
            return {}

    async def index_done_callback(self) -> None:
        pass

    async def is_empty(self) -> bool:
        try:
            count = await self._table.count_rows()
            return count == 0
        except Exception:
            return True

    async def delete(self, ids: list[str]) -> None:
        if not ids:
            return
        try:
            await self._table.delete(_build_where_in("_id", ids))
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in DocStatus delete: {e}")

    async def drop(self) -> dict[str, str]:
        try:
            count = await self._table.count_rows()
            if count > 0:
                await self._table.delete("1 = 1")
            logger.info(
                f"[{self.workspace}] Dropped {count} documents from DocStatus {self._table_name}"
            )
            return {"status": "success", "message": f"{count} documents dropped"}
        except Exception as e:
            logger.error(
                f"[{self.workspace}] Error dropping DocStatus {self._table_name}: {e}"
            )
            return {"status": "error", "message": str(e)}

    async def get_docs_paginated(
        self,
        status_filter: DocStatus | None = None,
        page: int = 1,
        page_size: int = 50,
        sort_field: str = "updated_at",
        sort_direction: str = "desc",
    ) -> tuple[list[tuple[str, DocProcessingStatus]], int]:
        # Validate parameters
        if page < 1:
            page = 1
        if page_size < 10:
            page_size = 10
        elif page_size > 200:
            page_size = 200

        valid_sort_fields = {"created_at", "updated_at", "_id", "file_path"}
        if sort_field not in valid_sort_fields:
            sort_field = "updated_at"

        if sort_direction.lower() not in ("asc", "desc"):
            sort_direction = "desc"

        try:
            # Build query
            if status_filter is not None:
                where = _build_where_eq("status", status_filter.value)
                all_results = (
                    await self._table.query()
                    .where(where)
                    .to_list()
                )
            else:
                all_results = await self._table.query().to_list()

            total_count = len(all_results)

            # Sort in application layer
            reverse = sort_direction.lower() == "desc"
            all_results.sort(
                key=lambda x: str(x.get(sort_field, "")),
                reverse=reverse,
            )

            # Paginate
            skip = (page - 1) * page_size
            page_results = all_results[skip : skip + page_size]

            # Convert to (doc_id, DocProcessingStatus) tuples
            documents = []
            for row in page_results:
                try:
                    doc_id = row["_id"]
                    data = self._prepare_doc_status_data(dict(row))
                    doc_status = DocProcessingStatus(**data)
                    documents.append((doc_id, doc_status))
                except (KeyError, TypeError) as e:
                    logger.error(
                        f"[{self.workspace}] Missing field for document {row.get('_id')}: {e}"
                    )

            return documents, total_count
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_docs_paginated: {e}")
            return [], 0

    async def get_all_status_counts(self) -> dict[str, int]:
        try:
            results = (
                await self._table.query()
                .select(["status"])
                .to_list()
            )
            counts: dict[str, int] = {}
            total = 0
            for row in results:
                status = row.get("status", "unknown")
                counts[status] = counts.get(status, 0) + 1
                total += 1
            counts["all"] = total
            return counts
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_all_status_counts: {e}")
            return {}

    async def get_doc_by_file_path(self, file_path: str) -> dict[str, Any] | None:
        try:
            results = (
                await self._table.query()
                .where(_build_where_eq("file_path", file_path))
                .to_list()
            )
            if results:
                row = dict(results[0])
                row.pop("_rowid", None)
                return row
            return None
        except Exception as e:
            logger.error(f"[{self.workspace}] Error in get_doc_by_file_path: {e}")
            return None
