import argparse
import json
from pathlib import Path

from arango import ArangoClient

from experiments.cross_db_graph import config
from experiments.lancedb_graph.data_prep.build_graph_tables import build_graph_dataframes_from_tsv

VERTEX_COLLECTION = "graph_nodes"
EDGE_COLLECTION = "graph_edges"
GRAPH_NAME = "graph_bench_graph"


def _ensure_database(client: ArangoClient, db_name: str, username: str, password: str):
    sys_db = client.db("_system", username=username, password=password)
    if not sys_db.has_database(db_name):
        sys_db.create_database(db_name)
    return client.db(db_name, username=username, password=password)


def ensure_schema(db):
    if not db.has_collection(VERTEX_COLLECTION):
        db.create_collection(VERTEX_COLLECTION)

    if not db.has_collection(EDGE_COLLECTION):
        db.create_collection(EDGE_COLLECTION, edge=True)

    if not db.has_graph(GRAPH_NAME):
        graph = db.create_graph(GRAPH_NAME)
        graph.create_edge_definition(
            edge_collection=EDGE_COLLECTION,
            from_vertex_collections=[VERTEX_COLLECTION],
            to_vertex_collections=[VERTEX_COLLECTION],
        )

    vertex_collection = db.collection(VERTEX_COLLECTION)
    edge_collection = db.collection(EDGE_COLLECTION)

    vertex_collection.add_hash_index(fields=["node_id"], unique=True)
    vertex_collection.add_hash_index(fields=["node_type"])
    vertex_collection.add_hash_index(fields=["community_id"])
    vertex_collection.add_persistent_index(fields=["degree_out"])

    edge_collection.add_hash_index(fields=["src_id"])
    edge_collection.add_hash_index(fields=["dst_id"])


def truncate_collections(db):
    db.collection(EDGE_COLLECTION).truncate()
    db.collection(VERTEX_COLLECTION).truncate()


def _to_vertex_docs(nodes_df):
    docs = []
    for row in nodes_df.itertuples(index=False):
        docs.append(
            {
                "_key": str(row.node_id),
                "node_id": str(row.node_id),
                "node_type": row.node_type,
                "degree_out": int(row.degree_out),
                "degree_in": int(row.degree_in),
                "community_id": str(row.community_id),
                "attrs_json": json.loads(row.attrs_json),
            }
        )
    return docs


def _to_edge_docs(edges_df):
    docs = []
    for row in edges_df.itertuples(index=False):
        docs.append(
            {
                "_key": str(row.edge_id),
                "_from": f"{VERTEX_COLLECTION}/{row.src_id}",
                "_to": f"{VERTEX_COLLECTION}/{row.dst_id}",
                "edge_id": str(row.edge_id),
                "src_id": str(row.src_id),
                "dst_id": str(row.dst_id),
                "edge_type": row.edge_type,
                "src_type": row.src_type,
                "dst_type": row.dst_type,
                "attrs_json": json.loads(row.attrs_json),
            }
        )
    return docs


def _import_in_batches(collection, docs, batch_size: int = 5000):
    for start in range(0, len(docs), batch_size):
        chunk = docs[start : start + batch_size]
        if chunk:
            collection.import_bulk(chunk, overwrite=True)


def import_tsv_to_arangodb(tsv_path: Path, url: str, db_name: str, username: str, password: str):
    nodes_df, edges_df = build_graph_dataframes_from_tsv(str(tsv_path))

    client = ArangoClient(hosts=url)
    db = _ensure_database(client, db_name, username, password)
    ensure_schema(db)
    truncate_collections(db)

    vertex_docs = _to_vertex_docs(nodes_df)
    edge_docs = _to_edge_docs(edges_df)

    _import_in_batches(db.collection(VERTEX_COLLECTION), vertex_docs)
    _import_in_batches(db.collection(EDGE_COLLECTION), edge_docs)


def main():
    parser = argparse.ArgumentParser(description="Import TSV graph data into ArangoDB")
    parser.add_argument("tsv_path", help="Path to triples TSV file")
    parser.add_argument("--url", default=config.ARANGODB_URL, help="ArangoDB URL")
    parser.add_argument("--db", default=config.ARANGODB_DB, help="ArangoDB database name")
    parser.add_argument("--username", default=config.ARANGODB_USERNAME, help="ArangoDB username")
    parser.add_argument("--password", default=config.ARANGODB_PASSWORD, help="ArangoDB password")
    args = parser.parse_args()

    import_tsv_to_arangodb(Path(args.tsv_path), args.url, args.db, args.username, args.password)
    print(f"Imported graph data from {args.tsv_path} into ArangoDB database {args.db}")


if __name__ == "__main__":
    main()
