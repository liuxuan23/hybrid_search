import argparse
import csv
import json
from dataclasses import asdict
from datetime import datetime

from experiments.cross_db_graph import config
from experiments.cross_db_graph.adapters.arangodb_adapter import ArangoDBGraphAdapter
from experiments.cross_db_graph.adapters.lancedb_adapter import LanceDBGraphAdapter
from experiments.cross_db_graph.adapters.postgres_adapter import PostgresGraphAdapter
from experiments.cross_db_graph.result_schema import BenchmarkResult
from experiments.cross_db_graph.scripts.analyze_results import analyze_results
from experiments.cross_db_graph.workloads import BatchNeighborQuery, KHopQuery, NeighborQuery, build_default_workloads


def load_seeds():
    if not config.SEEDS_FILE.exists():
        return [], []
    with open(config.SEEDS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    single_seeds = data.get("low_degree", []) + data.get("medium_degree", []) + data.get("high_degree", [])
    batch_seeds = data.get("batch_seed_set", [])
    return single_seeds, batch_seeds


def build_lancedb_adapter(materialize: bool = True):
    return LanceDBGraphAdapter(db_path=str(config.LANCEDB_DB_PATH), materialize=materialize)


def build_postgres_adapter(materialize: bool = False):
    return PostgresGraphAdapter(dsn=config.POSTGRES_DSN, materialize=materialize)


def build_arangodb_adapter(materialize: bool = False):
    return ArangoDBGraphAdapter(
        url=config.ARANGODB_URL,
        db_name=config.ARANGODB_DB,
        username=config.ARANGODB_USERNAME,
        password=config.ARANGODB_PASSWORD,
        materialize=materialize,
    )


def build_adapter(engine: str, materialize: bool | None = None):
    if engine == "lancedb":
        return build_lancedb_adapter(materialize=True if materialize is None else materialize)
    if engine == "postgres":
        return build_postgres_adapter(materialize=False if materialize is None else materialize)
    if engine == "arangodb":
        return build_arangodb_adapter(materialize=False if materialize is None else materialize)
    raise ValueError(f"Unsupported engine: {engine}")


def run_workload(adapter, workload):
    if isinstance(workload, NeighborQuery):
        result = adapter.query_neighbors(workload.seed, direction=workload.direction)
        return BenchmarkResult(
            engine=adapter.engine_name,
            query_type=workload.query_type,
            seed=workload.seed,
            time_ms=result["time_ms"],
            result_count=result["count"],
        )

    if isinstance(workload, KHopQuery):
        result = adapter.query_k_hop(workload.seed, k=workload.k, direction=workload.direction)
        return BenchmarkResult(
            engine=adapter.engine_name,
            query_type=workload.query_type,
            seed=workload.seed,
            k=workload.k,
            time_ms=result["time_ms"],
            result_count=result["count"],
        )

    if isinstance(workload, BatchNeighborQuery):
        result = adapter.query_batch_neighbors(workload.seeds, direction=workload.direction)
        return BenchmarkResult(
            engine=adapter.engine_name,
            query_type=workload.query_type,
            seed=",".join(workload.seeds),
            batch_size=len(workload.seeds),
            time_ms=result["time_ms"],
            result_count=result["count"],
        )

    raise TypeError(f"Unsupported workload type: {type(workload)!r}")


def execute_benchmark(adapter, workloads):
    results = []

    for _ in range(config.WARMUP_RUNS):
        for workload in workloads:
            run_workload(adapter, workload)

    for _ in range(config.MEASURE_RUNS):
        for workload in workloads:
            try:
                results.append(run_workload(adapter, workload))
            except Exception as exc:
                results.append(
                    BenchmarkResult(
                        engine=adapter.engine_name,
                        query_type=getattr(workload, "query_type", "unknown"),
                        seed=getattr(workload, "seed", ""),
                        k=getattr(workload, "k", 0),
                        batch_size=len(getattr(workload, "seeds", [])) if hasattr(workload, "seeds") else 0,
                        success=False,
                        error_message=str(exc),
                    )
                )
    return results


def write_results(results):
    config.RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = config.RESULTS_DIR / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "raw_results.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(results[0]).keys()) if results else list(asdict(BenchmarkResult(engine="", query_type="")).keys()))
        writer.writeheader()
        for result in results:
            writer.writerow(result.to_dict())

    analyze_results(output_dir)

    return output_dir


def parse_args():
    parser = argparse.ArgumentParser(description="Run cross-db graph benchmark")
    parser.add_argument(
        "--engine",
        choices=["lancedb", "postgres", "arangodb"],
        default="lancedb",
        help="Which backend engine to benchmark",
    )
    parser.add_argument(
        "--materialize",
        choices=["true", "false"],
        default=None,
        help="Override query materialization mode for the selected engine",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    single_seeds, batch_seeds = load_seeds()
    workloads = build_default_workloads(single_seeds, batch_seeds)
    print(f"Loaded {len(workloads)} workloads from {config.SEEDS_FILE}")
    materialize = None if args.materialize is None else args.materialize == "true"
    adapter = build_adapter(args.engine, materialize=materialize)
    adapter.connect()
    try:
        results = execute_benchmark(adapter, workloads)
        output_dir = write_results(results)
    finally:
        adapter.close()

    print(f"Completed {adapter.engine_name} benchmark run. Results written to {output_dir}")


if __name__ == "__main__":
    main()
