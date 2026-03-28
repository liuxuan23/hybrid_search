from experiments.cross_db_graph.adapters.base import GraphAdapter


class ArangoDBGraphAdapter(GraphAdapter):
    engine_name = "arangodb"

    def __init__(self, url: str, db_name: str, username: str, password: str):
        self.url = url
        self.db_name = db_name
        self.username = username
        self.password = password
        self.db = None

    def connect(self):
        raise NotImplementedError("TODO: connect to ArangoDB")

    def close(self):
        self.db = None

    def query_neighbors(self, seed: str, direction: str = "out"):
        raise NotImplementedError("TODO: implement AQL 1-hop query")

    def query_k_hop(self, seed: str, k: int, direction: str = "out"):
        raise NotImplementedError("TODO: implement AQL k-hop traversal")

    def query_batch_neighbors(self, seeds, direction: str = "out"):
        raise NotImplementedError("TODO: implement batch AQL neighbor query")
