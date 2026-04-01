import time

from arango import ArangoClient

from experiments.cross_db_graph.adapters.base import GraphAdapter

VERTEX_COLLECTION = "graph_nodes"
EDGE_COLLECTION = "graph_edges"


class ArangoDBGraphAdapter(GraphAdapter):
    engine_name = "arangodb"

    def __init__(self, url: str, db_name: str, username: str, password: str, materialize: bool = False):
        self.url = url
        self.db_name = db_name
        self.username = username
        self.password = password
        self.materialize = materialize
        self.client = None
        self.db = None

    def connect(self):
        self.client = ArangoClient(hosts=self.url)
        self.db = self.client.db(self.db_name, username=self.username, password=self.password)
        self.db.collections()
        return self

    def close(self):
        self.db = None
        self.client = None

    def query_neighbors(self, seed: str, direction: str = "out"):
        edge_field = "src_id" if direction == "out" else "dst_id"
        target_field = "dst_id" if direction == "out" else "src_id"
        if self.materialize:
            query = f"""
            FOR e IN {EDGE_COLLECTION}
                FILTER e.{edge_field} == @seed
                FOR v IN {VERTEX_COLLECTION}
                    FILTER v.node_id == e.{target_field}
                    RETURN v
            """
        else:
            query = f"""
            FOR e IN {EDGE_COLLECTION}
                FILTER e.{edge_field} == @seed
                RETURN e.{target_field}
            """
        start = time.perf_counter()
        cursor = self.db.aql.execute(query, bind_vars={"seed": seed})
        rows = list(cursor)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {"time_ms": elapsed_ms, "count": len(rows)}

    def query_k_hop(self, seed: str, k: int, direction: str = "out"):
        traversal_direction = "OUTBOUND" if direction == "out" else "INBOUND"
        start_vertex = f"{VERTEX_COLLECTION}/{seed}"
        if self.materialize:
            query = f"""
            FOR v, e, p IN 1..@k {traversal_direction} @start_vertex {EDGE_COLLECTION}
                OPTIONS {{ uniqueVertices: 'path' }}
                COLLECT node_id = v.node_id INTO groups
                RETURN FIRST(groups[*].v)
            """
        else:
            query = f"""
            FOR v, e, p IN 1..@k {traversal_direction} @start_vertex {EDGE_COLLECTION}
                OPTIONS {{ uniqueVertices: 'path' }}
                COLLECT node_id = v.node_id
                RETURN node_id
            """
        start = time.perf_counter()
        cursor = self.db.aql.execute(query, bind_vars={"k": k, "start_vertex": start_vertex})
        rows = list(cursor)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {"time_ms": elapsed_ms, "count": len(rows)}

    def query_batch_neighbors(self, seeds, direction: str = "out"):
        edge_field = "src_id" if direction == "out" else "dst_id"
        target_field = "dst_id" if direction == "out" else "src_id"
        if self.materialize:
            query = f"""
            FOR e IN {EDGE_COLLECTION}
                FILTER e.{edge_field} IN @seeds
                FOR v IN {VERTEX_COLLECTION}
                    FILTER v.node_id == e.{target_field}
                    RETURN v
            """
        else:
            query = f"""
            FOR e IN {EDGE_COLLECTION}
                FILTER e.{edge_field} IN @seeds
                RETURN e.{target_field}
            """
        start = time.perf_counter()
        cursor = self.db.aql.execute(query, bind_vars={"seeds": list(seeds)})
        rows = list(cursor)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return {"time_ms": elapsed_ms, "count": len(rows)}
