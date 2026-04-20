import argparse
import csv
import os
import tempfile
from pathlib import Path

import psycopg

from experiments.cross_db_graph import config
from experiments.lancedb_graph.data_prep.build_graph_tables import (
    build_node_records_from_tsv,
    iter_edge_records_from_tsv,
)


def ensure_age_available(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS age")
        cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'age'")
        if cur.fetchone() is None:
            raise RuntimeError(
                "Apache AGE extension is not installed. Install AGE and run CREATE EXTENSION age before import."
            )
        cur.execute("LOAD 'age'")
        cur.execute('SET search_path = ag_catalog, "$user", public')


def recreate_graph(conn, graph_name: str, vertex_label: str, edge_label: str):
    with conn.cursor() as cur:
        cur.execute("SELECT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s)", (graph_name,))
        exists = cur.fetchone()[0]
        if exists:
            cur.execute("SELECT drop_graph(%s, true)", (graph_name,))
        cur.execute("SELECT create_graph(%s)", (graph_name,))
        cur.execute("SELECT create_vlabel(%s, %s)", (graph_name, vertex_label))
        cur.execute("SELECT create_elabel(%s, %s)", (graph_name, edge_label))
    conn.commit()


def write_age_nodes_csv_and_mapping(tsv_path: Path, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(output_dir, 0o755)
    nodes_csv = output_dir / "age_nodes.csv"

    node_records = build_node_records_from_tsv(str(tsv_path))
    node_id_to_internal_id: dict[str, int] = {}

    with nodes_csv.open("w", newline="", encoding="utf-8") as nf:
        writer = csv.writer(nf)
        writer.writerow(["id", "node_id", "node_type", "degree_out", "degree_in", "community_id", "attrs_json"])
        for idx, row in enumerate(node_records, start=1):
            node_id = str(row.node_id)
            node_id_to_internal_id[node_id] = idx
            writer.writerow(
                [
                    idx,
                    node_id,
                    row.node_type,
                    int(row.degree_out),
                    int(row.degree_in),
                    str(row.community_id),
                    row.attrs_json,
                ]
            )
    os.chmod(nodes_csv, 0o644)
    return nodes_csv, node_id_to_internal_id, len(node_records)


def load_edges_in_chunks(
    conn,
    graph_name: str,
    edge_label: str,
    tsv_path: Path,
    node_id_to_internal_id: dict[str, int],
    output_dir: Path,
    vertex_label: str,
    edge_chunk_size: int,
) -> int:
    edge_count = 0
    chunk_rows: list[list] = []
    chunk_index = 0

    def flush_chunk(rows: list[list], idx: int):
        if not rows:
            return
        chunk_file = output_dir / f"age_edges_chunk_{idx:06d}.csv"
        with chunk_file.open("w", newline="", encoding="utf-8") as ef:
            writer = csv.writer(ef)
            writer.writerow(
                [
                    "start_id",
                    "start_vertex_type",
                    "end_id",
                    "end_vertex_type",
                    "edge_id",
                    "src_id",
                    "dst_id",
                    "edge_type",
                    "src_type",
                    "dst_type",
                    "attrs_json",
                ]
            )
            writer.writerows(rows)
        os.chmod(chunk_file, 0o644)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ag_catalog.load_edges_from_file(%s, %s, %s, false)",
                (graph_name, edge_label, str(chunk_file)),
            )
        conn.commit()
        chunk_file.unlink(missing_ok=True)

    for edge in iter_edge_records_from_tsv(str(tsv_path)):
        src_id = str(edge.src_id)
        dst_id = str(edge.dst_id)
        start_id = node_id_to_internal_id.get(src_id)
        end_id = node_id_to_internal_id.get(dst_id)
        if start_id is None or end_id is None:
            raise RuntimeError(f"Missing node mapping for edge {edge.edge_id}: {src_id} -> {dst_id}")

        chunk_rows.append(
            [
                start_id,
                vertex_label,
                end_id,
                vertex_label,
                str(edge.edge_id),
                src_id,
                dst_id,
                edge.edge_type,
                edge.src_type,
                edge.dst_type,
                edge.attrs_json,
            ]
        )
        edge_count += 1

        if len(chunk_rows) >= edge_chunk_size:
            chunk_index += 1
            flush_chunk(chunk_rows, chunk_index)
            chunk_rows = []

    if chunk_rows:
        chunk_index += 1
        flush_chunk(chunk_rows, chunk_index)

    return edge_count


def import_tsv_to_postgres_age(
    tsv_path: Path,
    dsn: str,
    graph_name: str,
    vertex_label: str = "Node",
    edge_label: str = "EDGE",
    edge_chunk_size: int = 100_000,
):
    with tempfile.TemporaryDirectory(prefix="age_bulk_import_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        nodes_csv, node_id_to_internal_id, node_count = write_age_nodes_csv_and_mapping(
            tsv_path=tsv_path,
            output_dir=tmp_path,
        )

        with psycopg.connect(dsn) as conn:
            ensure_age_available(conn)
            recreate_graph(conn, graph_name, vertex_label, edge_label)

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ag_catalog.load_labels_from_file(%s, %s, %s, true, false)",
                    (graph_name, vertex_label, str(nodes_csv)),
                )
            conn.commit()

            edge_count = load_edges_in_chunks(
                conn=conn,
                graph_name=graph_name,
                edge_label=edge_label,
                tsv_path=tsv_path,
                node_id_to_internal_id=node_id_to_internal_id,
                output_dir=tmp_path,
                vertex_label=vertex_label,
                edge_chunk_size=edge_chunk_size,
            )

            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{vertex_label.lower()}_id_unique "
                    f"ON {graph_name}.\"{vertex_label}\" (id)"
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{vertex_label.lower()}_node_id_agtype "
                    f"ON {graph_name}.\"{vertex_label}\" ((properties -> '\"node_id\"'::agtype))"
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{vertex_label.lower()}_properties_gin "
                    f"ON {graph_name}.\"{vertex_label}\" USING gin (properties)"
                )
                cur.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{edge_label.lower()}_id_unique "
                    f"ON {graph_name}.\"{edge_label}\" (id)"
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{edge_label.lower()}_src_id_agtype "
                    f"ON {graph_name}.\"{edge_label}\" ((properties -> '\"src_id\"'::agtype))"
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{edge_label.lower()}_dst_id_agtype "
                    f"ON {graph_name}.\"{edge_label}\" ((properties -> '\"dst_id\"'::agtype))"
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{edge_label.lower()}_start_id "
                    f"ON {graph_name}.\"{edge_label}\" (start_id)"
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{edge_label.lower()}_end_id "
                    f"ON {graph_name}.\"{edge_label}\" (end_id)"
                )
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{edge_label.lower()}_properties_gin "
                    f"ON {graph_name}.\"{edge_label}\" USING gin (properties)"
                )
            conn.commit()

        return node_count, edge_count


def main():
    parser = argparse.ArgumentParser(description="Import TSV graph data into PostgreSQL Apache AGE graph")
    parser.add_argument("tsv_path", help="Path to triples TSV file")
    parser.add_argument("--dsn", default=config.POSTGRES_DSN, help="PostgreSQL DSN")
    parser.add_argument("--graph-name", default=config.POSTGRES_AGE_GRAPH, help="AGE graph name")
    parser.add_argument("--vertex-label", default=config.POSTGRES_AGE_VERTEX_LABEL, help="AGE vertex label")
    parser.add_argument("--edge-label", default=config.POSTGRES_AGE_EDGE_LABEL, help="AGE edge label")
    parser.add_argument(
        "--edge-chunk-size",
        type=int,
        default=100000,
        help="Number of edges per CSV chunk for AGE load_edges_from_file",
    )
    args = parser.parse_args()

    node_count, edge_count = import_tsv_to_postgres_age(
        Path(args.tsv_path),
        dsn=args.dsn,
        graph_name=args.graph_name,
        vertex_label=args.vertex_label,
        edge_label=args.edge_label,
        edge_chunk_size=args.edge_chunk_size,
    )
    print(
        f"Imported graph data from {args.tsv_path} into PostgreSQL AGE graph {args.graph_name} "
        f"(nodes={node_count}, edges={edge_count})"
    )


if __name__ == "__main__":
    main()
