import argparse
import json
from dataclasses import asdict

from experiments.cross_db_graph.adapters.arangodb_adapter import ArangoDBGraphAdapter
from experiments.cross_db_graph.adapters.lance_graph_adapter import LanceGraphAdapter
from experiments.cross_db_graph.adapters.lancedb_adapter import LanceDBGraphAdapter
from experiments.cross_db_graph.adapters.postgres_age_adapter import PostgresAGEGraphAdapter
from experiments.cross_db_graph.adapters.postgres_adapter import PostgresGraphAdapter
from experiments.cross_db_graph.runner import run_workload
from experiments.cross_db_graph.workloads import KHopQuery, NeighborQuery
from experiments.cross_db_graph import config


def parse_args():
    parser = argparse.ArgumentParser(description="Run 1-hop, 2-hop, and 3-hop queries for a single seed")
    parser.add_argument(
        "--engine",
        choices=["lancedb", "lance_graph", "postgres", "postgres_age", "arangodb"],
        required=True,
        help="Backend engine to query",
    )
    parser.add_argument("--seed", required=True, help="Seed node id, e.g. type0:node_123")
    parser.add_argument(
        "--direction",
        choices=["out", "in"],
        default="out",
        help="Traversal direction for all queries",
    )
    parser.add_argument(
        "--materialize",
        choices=["true", "false"],
        default=None,
        help="Optional materialization override passed to the selected adapter",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the three query results as JSON instead of a simple table",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional storage path override for Lance-based engines (`lancedb` or `lance_graph`)",
    )
    parser.add_argument(
        "--query-spec",
        choices=["all", "neighbor", "k_hop_2", "k_hop_3"],
        default="all",
        help="Optional single-query mode for cold-start orchestration",
    )
    return parser.parse_args()


def build_single_seed_adapter(engine: str, materialize: bool | None, db_path: str | None):
    if engine == "lancedb":
        return LanceDBGraphAdapter(
            db_path=db_path or str(config.LANCEDB_DB_PATH),
            materialize=True if materialize is None else materialize,
        )
    if engine == "lance_graph":
        return LanceGraphAdapter(
            db_path=db_path or str(config.LANCE_GRAPH_DB_PATH),
            materialize=False if materialize is None else materialize,
        )
    if engine == "postgres":
        return PostgresGraphAdapter(dsn=config.POSTGRES_DSN, materialize=False if materialize is None else materialize)
    if engine == "postgres_age":
        return PostgresAGEGraphAdapter(
            dsn=config.POSTGRES_DSN,
            graph_name=config.POSTGRES_AGE_GRAPH,
            vertex_label=config.POSTGRES_AGE_VERTEX_LABEL,
            edge_label=config.POSTGRES_AGE_EDGE_LABEL,
            materialize=False if materialize is None else materialize,
        )
    if engine == "arangodb":
        return ArangoDBGraphAdapter(
            url=config.ARANGODB_URL,
            db_name=config.ARANGODB_DB,
            username=config.ARANGODB_USERNAME,
            password=config.ARANGODB_PASSWORD,
            materialize=False if materialize is None else materialize,
        )
    raise ValueError(f"Unsupported engine: {engine}")


def main():
    args = parse_args()
    materialize = None if args.materialize is None else args.materialize == "true"
    if args.query_spec == "neighbor":
        workloads = [NeighborQuery(seed=args.seed, direction=args.direction)]
    elif args.query_spec == "k_hop_2":
        workloads = [KHopQuery(seed=args.seed, k=2, direction=args.direction)]
    elif args.query_spec == "k_hop_3":
        workloads = [KHopQuery(seed=args.seed, k=3, direction=args.direction)]
    else:
        workloads = [
            NeighborQuery(seed=args.seed, direction=args.direction),
            KHopQuery(seed=args.seed, k=2, direction=args.direction),
            KHopQuery(seed=args.seed, k=3, direction=args.direction),
        ]

    adapter = build_single_seed_adapter(args.engine, materialize=materialize, db_path=args.db_path)
    adapter.connect()
    try:
        results = [run_workload(adapter, workload) for workload in workloads]
    finally:
        adapter.close()

    if args.json:
        print(json.dumps([asdict(result) for result in results], ensure_ascii=False, indent=2))
        return

    print(f"engine={args.engine} seed={args.seed} direction={args.direction}")
    print("query_type\tk\ttime_ms\tresult_count")
    for result in results:
        print(f"{result.query_type}\t{result.k}\t{result.time_ms:.3f}\t{result.result_count}")


if __name__ == "__main__":
    main()
