import argparse
from pathlib import Path

import psycopg

from experiments.cross_db_graph import config
from experiments.lancedb_graph.data_prep.build_graph_tables import build_graph_dataframes_from_tsv


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS graph_edges")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS graph_nodes (
                node_id TEXT PRIMARY KEY,
                node_type TEXT,
                degree_out INTEGER,
                degree_in INTEGER,
                community_id TEXT,
                attrs_json JSONB
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS graph_edges (
                edge_id TEXT PRIMARY KEY,
                src_id TEXT NOT NULL,
                dst_id TEXT NOT NULL,
                edge_type TEXT,
                src_type TEXT,
                dst_type TEXT,
                attrs_json JSONB
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_graph_edges_src ON graph_edges (src_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_graph_edges_dst ON graph_edges (dst_id)")
    conn.commit()


def truncate_tables(conn):
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE graph_edges")
        cur.execute("TRUNCATE TABLE graph_nodes")
    conn.commit()


def import_tsv_to_postgres(tsv_path: Path, dsn: str):
    nodes_df, edges_df = build_graph_dataframes_from_tsv(str(tsv_path))

    with psycopg.connect(dsn) as conn:
        ensure_schema(conn)
        truncate_tables(conn)

        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO graph_nodes (node_id, node_type, degree_out, degree_in, community_id, attrs_json)
                VALUES (%s, %s, %s, %s, %s, %s::jsonb)
                """,
                [
                    (
                        row.node_id,
                        row.node_type,
                        int(row.degree_out),
                        int(row.degree_in),
                        row.community_id,
                        row.attrs_json,
                    )
                    for row in nodes_df.itertuples(index=False)
                ],
            )
            cur.executemany(
                """
                INSERT INTO graph_edges (edge_id, src_id, dst_id, edge_type, src_type, dst_type, attrs_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                [
                    (
                        row.edge_id,
                        row.src_id,
                        row.dst_id,
                        row.edge_type,
                        row.src_type,
                        row.dst_type,
                        row.attrs_json,
                    )
                    for row in edges_df.itertuples(index=False)
                ],
            )
        conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Import TSV graph data into PostgreSQL")
    parser.add_argument("tsv_path", help="Path to triples TSV file")
    parser.add_argument("--dsn", default=config.POSTGRES_DSN, help="PostgreSQL DSN")
    args = parser.parse_args()

    import_tsv_to_postgres(Path(args.tsv_path), args.dsn)
    print(f"Imported graph data from {args.tsv_path} into PostgreSQL")


if __name__ == "__main__":
    main()
