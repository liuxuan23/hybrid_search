import time

import psycopg

from experiments.cross_db_graph.adapters.base import GraphAdapter


class PostgresGraphAdapter(GraphAdapter):
    engine_name = "postgres"

    def __init__(self, dsn: str):
        self.dsn = dsn
        self.conn = None

    def connect(self):
        self.conn = psycopg.connect(self.dsn)
        return self

    def close(self):
        if self.conn is not None:
            self.conn.close()
        self.conn = None

    def query_neighbors(self, seed: str, direction: str = "out"):
        edge_column = "src_id" if direction == "out" else "dst_id"
        target_column = "dst_id" if direction == "out" else "src_id"
        sql = f"SELECT {target_column} FROM graph_edges WHERE {edge_column} = %s"

        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(sql, (seed,))
            rows = cur.fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {"time_ms": elapsed_ms, "count": len(rows)}

    def query_k_hop(self, seed: str, k: int, direction: str = "out"):
        edge_from = "src_id" if direction == "out" else "dst_id"
        edge_to = "dst_id" if direction == "out" else "src_id"
        sql = f"""
        WITH RECURSIVE hop_walk(node_id, depth, path) AS (
            SELECT %s::text AS node_id, 0 AS depth, ARRAY[%s::text] AS path
            UNION ALL
            SELECT e.{edge_to} AS node_id,
                   hw.depth + 1 AS depth,
                   hw.path || e.{edge_to}
            FROM hop_walk hw
            JOIN graph_edges e ON e.{edge_from} = hw.node_id
            WHERE hw.depth < %s
              AND NOT e.{edge_to} = ANY(hw.path)
        )
        SELECT COUNT(DISTINCT node_id)
        FROM hop_walk
        WHERE depth > 0
        """

        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(sql, (seed, seed, k))
            count = cur.fetchone()[0]
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {"time_ms": elapsed_ms, "count": int(count)}

    def query_batch_neighbors(self, seeds, direction: str = "out"):
        edge_column = "src_id" if direction == "out" else "dst_id"
        target_column = "dst_id" if direction == "out" else "src_id"
        sql = f"SELECT {target_column} FROM graph_edges WHERE {edge_column} = ANY(%s)"

        start = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(sql, (list(seeds),))
            rows = cur.fetchall()
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {"time_ms": elapsed_ms, "count": len(rows)}
