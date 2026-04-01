from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Iterable

from experiments.cross_db_graph.adapters.base import GraphAdapter


class LanceGraphAdapter(GraphAdapter):
    engine_name = "lance_graph"
    NODE_LABEL = "nodes"
    EDGE_TYPE = "edges"

    def __init__(self, db_path: str, materialize: bool = False):
        self.db_path = Path(db_path)
        self.materialize = materialize
        self.connected = False
        self._query_cls = None
        self._dir_namespace_cls = None
        self._graph_config = None
        self._namespace = None

    def connect(self):
        self._ensure_python_paths()

        try:
            from lance_graph import CypherQuery, DirNamespace, GraphConfig
        except ImportError as exc:  # pragma: no cover - depends on local environment
            raise RuntimeError(
                "Unable to import lance_graph. Install the Python package or build the local bindings first."
            ) from exc

        self._query_cls = CypherQuery
        self._dir_namespace_cls = DirNamespace
        self._graph_config = (
            GraphConfig.builder()
            .with_node_label(self.NODE_LABEL, "node_id")
            .with_relationship(self.EDGE_TYPE, "src_id", "dst_id")
            .build()
        )
        self._namespace = DirNamespace(str(self.db_path))
        self.connected = True

    def close(self):
        self._query_cls = None
        self._dir_namespace_cls = None
        self._graph_config = None
        self._namespace = None
        self.connected = False

    def query_neighbors(self, seed: str, direction: str = "out"):
        self._ensure_connected()
        cypher = self._build_neighbor_query(seed=seed, direction=direction or "out")
        return self._execute_query(cypher)

    def query_k_hop(self, seed: str, k: int, direction: str = "out"):
        self._ensure_connected()
        cypher = self._build_k_hop_query(seed=seed, k=k, direction=direction or "out")
        return self._execute_query(cypher)

    def query_batch_neighbors(self, seeds, direction: str = "out"):
        self._ensure_connected()
        cypher = self._build_batch_neighbor_query(seeds=seeds, direction=direction or "out")
        result = self._execute_query(cypher)
        result["mode"] = "batch"
        return result

    def _execute_query(self, cypher: str):
        start = time.perf_counter()
        query = self._query_cls(cypher).with_config(self._graph_config)
        table = query.execute_with_namespace(self._namespace)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        rows = table.to_pylist() if self.materialize else []
        return {
            "rows": rows,
            "count": int(table.num_rows),
            "time_ms": elapsed_ms,
            "mode": "materialized" if self.materialize else "count_only",
            "io_stats": {},
        }

    def _build_neighbor_query(self, seed: str, direction: str) -> str:
        if direction == "out":
            return (
                f"MATCH (a:{self.NODE_LABEL})-[:{self.EDGE_TYPE}]->(b:{self.NODE_LABEL}) "
                f"WHERE a.node_id = {self._quote(seed)} "
                f"RETURN {self._neighbor_return('a', 'b')}"
            )
        if direction == "in":
            return (
                f"MATCH (a:{self.NODE_LABEL})-[:{self.EDGE_TYPE}]->(b:{self.NODE_LABEL}) "
                f"WHERE b.node_id = {self._quote(seed)} "
                f"RETURN {self._neighbor_return('b', 'a')}"
            )
        if direction == "both":
            return (
                f"MATCH (a:{self.NODE_LABEL})-[:{self.EDGE_TYPE}]-(b:{self.NODE_LABEL}) "
                f"WHERE a.node_id = {self._quote(seed)} "
                f"RETURN {self._neighbor_return('a', 'b')}"
            )
        raise ValueError(f"Unsupported direction: {direction}")

    def _build_batch_neighbor_query(self, seeds: Iterable[str], direction: str) -> str:
        quoted = ", ".join(self._quote(seed) for seed in seeds)
        if not quoted:
            raise ValueError("Batch neighbor query requires at least one seed")

        if direction == "out":
            return (
                f"MATCH (a:{self.NODE_LABEL})-[:{self.EDGE_TYPE}]->(b:{self.NODE_LABEL}) "
                f"WHERE a.node_id IN [{quoted}] "
                f"RETURN DISTINCT {self._neighbor_return('a', 'b')}"
            )
        if direction == "in":
            return (
                f"MATCH (a:{self.NODE_LABEL})-[:{self.EDGE_TYPE}]->(b:{self.NODE_LABEL}) "
                f"WHERE b.node_id IN [{quoted}] "
                f"RETURN DISTINCT {self._neighbor_return('b', 'a')}"
            )
        if direction == "both":
            return (
                f"MATCH (a:{self.NODE_LABEL})-[:{self.EDGE_TYPE}]-(b:{self.NODE_LABEL}) "
                f"WHERE a.node_id IN [{quoted}] "
                f"RETURN DISTINCT {self._neighbor_return('a', 'b')}"
            )
        raise ValueError(f"Unsupported direction: {direction}")

    def _build_k_hop_query(self, seed: str, k: int, direction: str) -> str:
        if k < 1:
            raise ValueError("k must be >= 1")

        if direction == "out":
            return (
                f"MATCH (a:{self.NODE_LABEL})-[:{self.EDGE_TYPE}*1..{k}]->(b:{self.NODE_LABEL}) "
                f"WHERE a.node_id = {self._quote(seed)} "
                f"RETURN DISTINCT {self._neighbor_return('a', 'b')}"
            )
        if direction == "in":
            return (
                f"MATCH (a:{self.NODE_LABEL})<-[:{self.EDGE_TYPE}*1..{k}]-(b:{self.NODE_LABEL}) "
                f"WHERE a.node_id = {self._quote(seed)} "
                f"RETURN DISTINCT {self._neighbor_return('a', 'b')}"
            )
        if direction == "both":
            return (
                f"MATCH (a:{self.NODE_LABEL})-[:{self.EDGE_TYPE}*1..{k}]-(b:{self.NODE_LABEL}) "
                f"WHERE a.node_id = {self._quote(seed)} "
                f"RETURN DISTINCT {self._neighbor_return('a', 'b')}"
            )
        raise ValueError(f"Unsupported direction: {direction}")

    def _neighbor_return(self, source_alias: str, target_alias: str) -> str:
        if self.materialize:
            return (
                f"{source_alias}.node_id AS seed_id, "
                f"{target_alias}.node_id AS neighbor_id, "
                f"{target_alias}.node_type AS neighbor_type"
            )
        return f"{target_alias}.node_id AS neighbor_id"

    def _ensure_connected(self):
        if not self.connected or self._namespace is None or self._graph_config is None:
            raise RuntimeError("LanceGraphAdapter is not connected")

    @staticmethod
    def _quote(value: str) -> str:
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"

    @staticmethod
    def _ensure_python_paths():
        repo_python_root = Path("/home/lx/workplace/lance-graph/python")
        source_python_root = repo_python_root / "python"
        for path in (source_python_root, repo_python_root):
            path_str = str(path)
            if path.exists() and path_str not in sys.path:
                sys.path.insert(0, path_str)