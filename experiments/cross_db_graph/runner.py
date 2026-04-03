import argparse
import csv
import json
from collections import OrderedDict
from dataclasses import asdict
from datetime import datetime

from experiments.cross_db_graph import config
from experiments.cross_db_graph.result_schema import BenchmarkResult
from experiments.cross_db_graph.scripts.analyze_results import analyze_results
from experiments.cross_db_graph.workloads import BatchNeighborQuery, KHopQuery, NeighborQuery, build_default_workloads


def load_seeds():
    # 从统一的 seeds.json 中读取单点查询和批量查询使用的种子集合。
    if not config.SEEDS_FILE.exists():
        return [], []
    with open(config.SEEDS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    single_seeds = data.get("low_degree", []) + data.get("medium_degree", []) + data.get("high_degree", [])
    batch_seeds = data.get("batch_seed_set", [])
    return single_seeds, batch_seeds


def build_lancedb_adapter(materialize: bool = True):
    from experiments.cross_db_graph.adapters.lancedb_adapter import LanceDBGraphAdapter

    return LanceDBGraphAdapter(db_path=str(config.LANCEDB_DB_PATH), materialize=materialize)


def build_lance_graph_adapter(materialize: bool = False):
    from experiments.cross_db_graph.adapters.lance_graph_adapter import LanceGraphAdapter

    return LanceGraphAdapter(db_path=str(config.LANCE_GRAPH_DB_PATH), materialize=materialize)


def build_postgres_adapter(materialize: bool = False):
    from experiments.cross_db_graph.adapters.postgres_adapter import PostgresGraphAdapter

    return PostgresGraphAdapter(dsn=config.POSTGRES_DSN, materialize=materialize)


def build_arangodb_adapter(materialize: bool = False):
    from experiments.cross_db_graph.adapters.arangodb_adapter import ArangoDBGraphAdapter

    return ArangoDBGraphAdapter(
        url=config.ARANGODB_URL,
        db_name=config.ARANGODB_DB,
        username=config.ARANGODB_USERNAME,
        password=config.ARANGODB_PASSWORD,
        materialize=materialize,
    )


def build_adapter(engine: str, materialize: bool | None = None):
    # 根据 engine 名称构造对应后端适配器，并应用可选的 materialize 覆盖参数。
    if engine == "lancedb":
        return build_lancedb_adapter(materialize=True if materialize is None else materialize)
    if engine == "lance_graph":
        return build_lance_graph_adapter(materialize=False if materialize is None else materialize)
    if engine == "postgres":
        return build_postgres_adapter(materialize=False if materialize is None else materialize)
    if engine == "arangodb":
        return build_arangodb_adapter(materialize=False if materialize is None else materialize)
    raise ValueError(f"Unsupported engine: {engine}")


def run_workload(adapter, workload):
    # 将统一的 workload 对象分发到各后端适配器的具体查询实现，并归一化为 BenchmarkResult。
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
    # warm 模式：复用同一个连接，先做预热，再进行多轮正式测量。
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


def execute_coldish_benchmark(engine: str, workloads, materialize: bool | None = None):
    # coldish 模式：保持原始 workload 顺序，并且每个 workload 单独创建连接。
    results = []

    for workload in workloads:
        adapter = build_adapter(engine, materialize=materialize)
        adapter.connect()
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
        finally:
            adapter.close()

    return results


def group_workloads_for_connection_isolation(workloads):
    # 将 workload 按查询类别分组，避免 1-hop/2-hop/3-hop 之间互相预热。
    grouped = OrderedDict()
    for workload in workloads:
        if isinstance(workload, NeighborQuery):
            key = (workload.query_type, workload.direction)
        elif isinstance(workload, KHopQuery):
            key = (workload.query_type, workload.k, workload.direction)
        elif isinstance(workload, BatchNeighborQuery):
            key = (workload.query_type, workload.direction, len(workload.seeds))
        else:
            key = (getattr(workload, "query_type", "unknown"),)
        grouped.setdefault(key, []).append(workload)
    return list(grouped.values())


def execute_group_coldish_benchmark(engine: str, workloads, materialize: bool | None = None):
    # group-coldish 模式：每个查询类别使用一个新连接，组内复用，组间断开重连。
    results = []

    for workload_group in group_workloads_for_connection_isolation(workloads):
        adapter = build_adapter(engine, materialize=materialize)
        adapter.connect()
        try:
            for workload in workload_group:
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
        finally:
            adapter.close()

    return results


def write_results(results):
    # 每次运行生成独立结果目录，落盘原始 CSV，并派生 summary.md。
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
        choices=["lancedb", "lance_graph", "postgres", "arangodb"],
        default="lancedb",
        help="Which backend engine to benchmark",
    )
    parser.add_argument(
        "--materialize",
        choices=["true", "false"],
        default=None,
        help="Override query materialization mode for the selected engine",
    )
    parser.add_argument(
        "--mode",
        choices=["warm", "group-coldish", "coldish"],
        default="warm",
        help="Benchmark execution mode: reuse one connection for warm mode, reconnect per query group for group-coldish mode, or reconnect per workload for coldish mode",
    )
    return parser.parse_args()


def main():
    # 主流程：加载 seeds -> 生成 workloads -> 按 mode 执行 -> 写出结果。
    args = parse_args()
    single_seeds, batch_seeds = load_seeds()
    workloads = build_default_workloads(single_seeds, batch_seeds)
    print(f"Loaded {len(workloads)} workloads from {config.SEEDS_FILE}")
    materialize = None if args.materialize is None else args.materialize == "true"
    if args.mode == "group-coldish":
        results = execute_group_coldish_benchmark(args.engine, workloads, materialize=materialize)
        output_dir = write_results(results)
        print(f"Completed {args.engine} benchmark run in group-coldish mode. Results written to {output_dir}")
        return

    if args.mode == "coldish":
        results = execute_coldish_benchmark(args.engine, workloads, materialize=materialize)
        output_dir = write_results(results)
        print(f"Completed {args.engine} benchmark run in coldish mode. Results written to {output_dir}")
        return

    adapter = build_adapter(args.engine, materialize=materialize)
    adapter.connect()
    try:
        results = execute_benchmark(adapter, workloads)
        output_dir = write_results(results)
    finally:
        adapter.close()

    print(f"Completed {adapter.engine_name} benchmark run in warm mode. Results written to {output_dir}")


if __name__ == "__main__":
    main()
