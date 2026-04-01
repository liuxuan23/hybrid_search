from pathlib import Path

from experiments.cross_db_graph.adapters.base import GraphAdapter
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import (
    LanceDBGraphAdjacency,
)


class LanceDBGraphAdapter(GraphAdapter):
    engine_name = "lancedb"

    def __init__(self, db_path: str, materialize: bool = True, direction: str = "out"):
        self.db_path = str(Path(db_path))
        self.materialize = materialize
        self.direction = direction
        self.graph = None
        self.connected = False

    def connect(self):
        self.graph = LanceDBGraphAdjacency(db_path=self.db_path).load()
        self.connected = True

    def close(self):
        self.graph = None
        self.connected = False

    def query_neighbors(self, seed: str, direction: str = "out"):
        self._ensure_connected()
        direction = direction or self.direction

        if direction == "out":
            result = self.graph.query_out_neighbors_index(seed, materialize=self.materialize)
        elif direction == "in":
            result = self.graph.query_in_neighbors_index(seed, materialize=self.materialize)
        elif direction == "both":
            result = self.graph.query_neighbors_index(seed, materialize=self.materialize)
        else:
            raise ValueError(f"Unsupported direction: {direction}")

        return self._normalize_result(result)

    def query_k_hop(self, seed: str, k: int, direction: str = "out"):
        self._ensure_connected()
        result = self.graph.query_k_hop_index(
            seed,
            k=k,
            materialize=self.materialize,
            direction=direction or self.direction,
        )
        return self._normalize_result(result)

    def query_batch_neighbors(self, seeds, direction: str = "out"):
        self._ensure_connected()
        result = self.graph.query_batch_neighbors_index(
            seeds,
            direction=direction or self.direction,
            materialize=self.materialize,
        )
        normalized = self._normalize_result(result)
        normalized["mode"] = "batch"
        return normalized

    def _ensure_connected(self):
        if not self.connected or self.graph is None:
            raise RuntimeError("LanceDBGraphAdapter is not connected")

    @staticmethod
    def _normalize_result(result):
        return {
            "rows": result.get("rows", []),
            "count": int(result.get("count", 0)),
            "time_ms": float(result.get("time_ms", 0.0)),
            "mode": result.get("mode", "unknown"),
            "io_stats": result.get("io_stats", {}),
        }
