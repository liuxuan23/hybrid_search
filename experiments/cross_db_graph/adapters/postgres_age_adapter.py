import json
import time

import psycopg
from psycopg import sql

from experiments.cross_db_graph.adapters.base import GraphAdapter


class PostgresAGEGraphAdapter(GraphAdapter):
    engine_name = "postgres_age"

    def __init__(
        self,
        dsn: str,
        graph_name: str,
        vertex_label: str = "Node",
        edge_label: str = "EDGE",
        materialize: bool = False,
    ):
        self.dsn = dsn
        self.graph_name = graph_name
        self.vertex_label = vertex_label
        self.edge_label = edge_label
        self.materialize = materialize
        self.conn = None

    def connect(self):
        self.conn = psycopg.connect(self.dsn)
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'age'")
            if cur.fetchone() is None:
                raise RuntimeError(
                    "Apache AGE extension is not installed in this PostgreSQL instance. "
                    "Please install AGE and run CREATE EXTENSION age first."
                )
            cur.execute("LOAD 'age'")
            cur.execute('SET search_path = ag_catalog, "$user", public')
        self.conn.commit()
        return self

    def close(self):
        if self.conn is not None:
            self.conn.close()
        self.conn = None

    def _execute_cypher(self, query: str, params: dict):
        query_literal = sql.SQL("$age${}$age$").format(sql.SQL(query))
        stmt = sql.SQL("SELECT * FROM cypher({}, {}, %s::agtype) AS (result agtype)").format(
            sql.Literal(self.graph_name),
            query_literal,
        )
        with self.conn.cursor() as cur:
            cur.execute(stmt, (json.dumps(params, ensure_ascii=False),))
            return cur.fetchall()

    def _resolve_seed_graphid(self, seed: str):
        query = sql.SQL(
            "SELECT id FROM {}.{} WHERE properties @> %s::agtype LIMIT 1"
        ).format(
            sql.Identifier(self.graph_name),
            sql.Identifier(self.vertex_label),
        )
        with self.conn.cursor() as cur:
            cur.execute(query, (json.dumps({"node_id": seed}, ensure_ascii=False),))
            row = cur.fetchone()
        return row[0] if row else None

    def query_neighbors(self, seed: str, direction: str = "out"):
        relation_pattern = (
            f"-[e:{self.edge_label}]->" if direction == "out" else f"<-[e:{self.edge_label}]-"
        )
        if self.materialize:
            query = f"""
            MATCH (s:{self.vertex_label} {{node_id: $seed}}){relation_pattern}(n:{self.vertex_label})
            RETURN n
            """
        else:
            query = f"""
            MATCH (s:{self.vertex_label} {{node_id: $seed}}){relation_pattern}(n:{self.vertex_label})
            RETURN n.node_id
            """

        start = time.perf_counter()
        rows = self._execute_cypher(query, {"seed": seed})
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {"time_ms": elapsed_ms, "count": len(rows)}

    def query_k_hop(self, seed: str, k: int, direction: str = "out"):
        seed_graphid = self._resolve_seed_graphid(seed)
        if seed_graphid is None:
            return {"time_ms": 0.0, "count": 0}

        edge_from = "start_id" if direction == "out" else "end_id"
        edge_to = "end_id" if direction == "out" else "start_id"

        if self.materialize:
            query = sql.SQL(
                """
                WITH RECURSIVE hop_walk(node_id, depth, path) AS (
                    SELECT %s::graphid AS node_id, 0 AS depth, ARRAY[%s::graphid] AS path
                    UNION ALL
                    SELECT e.{edge_to} AS node_id,
                           hw.depth + 1 AS depth,
                           hw.path || e.{edge_to}
                    FROM hop_walk hw
                    JOIN {graph}.{edge_table} e ON e.{edge_from} = hw.node_id
                    WHERE hw.depth < %s
                      AND NOT e.{edge_to} = ANY(hw.path)
                )
                SELECT n.*
                FROM {graph}.{vertex_table} n
                JOIN (
                    SELECT DISTINCT node_id
                    FROM hop_walk
                    WHERE depth > 0
                ) hops ON hops.node_id = n.id
                """
            ).format(
                graph=sql.Identifier(self.graph_name),
                edge_table=sql.Identifier(self.edge_label),
                vertex_table=sql.Identifier(self.vertex_label),
                edge_from=sql.SQL(edge_from),
                edge_to=sql.SQL(edge_to),
            )
        else:
            query = sql.SQL(
                """
                WITH RECURSIVE hop_walk(node_id, depth, path) AS (
                    SELECT %s::graphid AS node_id, 0 AS depth, ARRAY[%s::graphid] AS path
                    UNION ALL
                    SELECT e.{edge_to} AS node_id,
                           hw.depth + 1 AS depth,
                           hw.path || e.{edge_to}
                    FROM hop_walk hw
                    JOIN {graph}.{edge_table} e ON e.{edge_from} = hw.node_id
                    WHERE hw.depth < %s
                      AND NOT e.{edge_to} = ANY(hw.path)
                )
                SELECT COUNT(DISTINCT node_id)
                FROM hop_walk
                WHERE depth > 0
                """
            ).format(
                graph=sql.Identifier(self.graph_name),
                edge_table=sql.Identifier(self.edge_label),
                edge_from=sql.SQL(edge_from),
                edge_to=sql.SQL(edge_to),
            )

        start = time.perf_counter()
        with self.conn.cursor() as cur:
            if self.materialize:
                cur.execute(query, (seed_graphid, seed_graphid, int(k)))
                rows = cur.fetchall()
                count = len(rows)
            else:
                cur.execute(query, (seed_graphid, seed_graphid, int(k)))
                count = cur.fetchone()[0]
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {"time_ms": elapsed_ms, "count": int(count)}

    def query_batch_neighbors(self, seeds, direction: str = "out"):
        relation_pattern = (
            f"-[e:{self.edge_label}]->" if direction == "out" else f"<-[e:{self.edge_label}]-"
        )
        if self.materialize:
            query = f"""
            UNWIND $seeds AS sid
            MATCH (s:{self.vertex_label} {{node_id: sid}}){relation_pattern}(n:{self.vertex_label})
            RETURN n
            """
        else:
            query = f"""
            UNWIND $seeds AS sid
            MATCH (s:{self.vertex_label} {{node_id: sid}}){relation_pattern}(n:{self.vertex_label})
            RETURN n.node_id
            """

        start = time.perf_counter()
        rows = self._execute_cypher(query, {"seeds": list(seeds)})
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {"time_ms": elapsed_ms, "count": len(rows)}
