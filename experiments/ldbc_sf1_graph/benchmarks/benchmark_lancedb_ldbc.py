from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path

from experiments.ldbc_sf1_graph import config
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark LanceDB graph queries on LDBC SF1")
    parser.add_argument("--db-path", type=Path, default=config.LDBC_LANCEDB_DIR)
    parser.add_argument("--seeds-path", type=Path, default=config.LDBC_SEEDS_PATH)
    parser.add_argument("--output-dir", type=Path, default=config.RESULTS_DIR)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--k-hop", type=int, default=2)
    parser.add_argument(
        "--single-group",
        choices=["low_degree", "mid_degree", "high_degree", "random", "combined"],
        default="combined",
    )
    parser.add_argument(
        "--batch-group",
        choices=["low_degree", "mid_degree", "high_degree", "mixed"],
        default="mixed",
    )
    parser.add_argument("--batch-group-index", type=int, default=0)
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Do not persist benchmark results to disk; print JSON only",
    )
    return parser.parse_args()


def load_seed_payload(seeds_path: Path) -> dict:
    if not seeds_path.exists():
        raise FileNotFoundError(f"Seeds file not found: {seeds_path}")
    return json.loads(seeds_path.read_text(encoding="utf-8"))


def pick_single_seeds(payload: dict, group: str) -> list[str]:
    seeds = payload.get("single_seeds", {}).get(group, [])
    if not seeds:
        raise ValueError(f"No single seeds available for group: {group}")
    return [str(seed) for seed in seeds]


def pick_batch_seed_group(payload: dict, group: str, index: int) -> list[str]:
    groups = payload.get("batch_seeds", {}).get(group, [])
    if not groups:
        raise ValueError(f"No batch seed groups available for group: {group}")
    normalized_index = max(0, min(int(index), len(groups) - 1))
    return [str(seed) for seed in groups[normalized_index]]


def benchmark_single(graph: LanceDBGraphAdjacency, node_ids: list[str], repeat: int, direction: str) -> dict:
    if direction == "out":
        query_fn = lambda node_id: graph.query_out_neighbors_index(node_id, materialize=False)
    elif direction == "both":
        query_fn = lambda node_id: graph.query_neighbors_index(node_id, materialize=False)
    else:
        raise ValueError(f"Unsupported single-hop direction: {direction}")
    return _benchmark_many(node_ids, repeat, query_fn)


def benchmark_batch(graph: LanceDBGraphAdjacency, batch_node_ids: list[str], repeat: int, direction: str) -> dict:
    return _benchmark_many(
        [batch_node_ids],
        repeat,
        lambda node_ids: graph.query_batch_neighbors_index(node_ids, direction=direction, materialize=False),
    )


def benchmark_k_hop(graph: LanceDBGraphAdjacency, node_ids: list[str], repeat: int, k: int, direction: str) -> dict:
    return _benchmark_many(
        node_ids,
        repeat,
        lambda node_id: graph.query_k_hop_index(node_id, k=k, materialize=False, direction=direction),
    )


def _benchmark_many(inputs, repeat: int, query_fn) -> dict:
    latency_values = []
    count_values = []
    read_bytes_values = []

    for _ in range(max(1, int(repeat))):
        for value in inputs:
            result = query_fn(value)
            latency_values.append(float(result.get("time_ms", 0.0)))
            count_values.append(int(result.get("count", 0)))
            read_bytes_values.append(int(result.get("io_stats", {}).get("read_bytes", 0)))

    total_time_ms = sum(latency_values)
    query_count = len(latency_values)
    return {
        "queries": query_count,
        "total_time_ms": total_time_ms,
        "avg_time_ms": statistics.fmean(latency_values) if latency_values else 0.0,
        "p50_time_ms": _percentile(latency_values, 50),
        "p95_time_ms": _percentile(latency_values, 95),
        "avg_count": statistics.fmean(count_values) if count_values else 0.0,
        "throughput_qps": (query_count / (total_time_ms / 1000.0)) if total_time_ms > 0 else 0.0,
        "avg_read_bytes": statistics.fmean(read_bytes_values) if read_bytes_values else 0.0,
        "total_read_bytes": sum(read_bytes_values),
    }


def _percentile(values, percentile: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, round((percentile / 100) * (len(ordered) - 1))))
    return float(ordered[index])


def build_result_filename(single_group: str, batch_group: str, k_hop: int, repeat: int, timestamp: str) -> str:
    return (
        f"ldbc_lancedb_benchmark_"
        f"single-{single_group}_batch-{batch_group}_k{k_hop}_r{repeat}_{timestamp}.json"
    )


def save_results(output_dir: Path, results: dict, single_group: str, batch_group: str, k_hop: int, repeat: int) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = output_dir / build_result_filename(single_group, batch_group, k_hop, repeat, timestamp)
    output_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def main() -> None:
    args = parse_args()
    payload = load_seed_payload(args.seeds_path)
    single_seeds = pick_single_seeds(payload, args.single_group)
    batch_seed_group = pick_batch_seed_group(payload, args.batch_group, args.batch_group_index)

    graph = LanceDBGraphAdjacency(db_path=str(args.db_path)).load()

    results = {
        "config": {
            "db_path": str(args.db_path),
            "seeds_path": str(args.seeds_path),
            "output_dir": str(args.output_dir),
            "repeat": args.repeat,
            "k_hop": args.k_hop,
            "single_group": args.single_group,
            "batch_group": args.batch_group,
            "batch_group_index": args.batch_group_index,
            "single_seed_count": len(single_seeds),
            "batch_seed_count": len(batch_seed_group),
        },
        "benchmarks": {
            "single_out": benchmark_single(graph, single_seeds, args.repeat, direction="out"),
            "single_both": benchmark_single(graph, single_seeds, args.repeat, direction="both"),
            "batch_both": benchmark_batch(graph, batch_seed_group, args.repeat, direction="both"),
            "k_hop_out": benchmark_k_hop(graph, single_seeds, args.repeat, k=args.k_hop, direction="out"),
            "k_hop_both": benchmark_k_hop(graph, single_seeds, args.repeat, k=args.k_hop, direction="both"),
        },
    }

    if not args.no_save:
        output_path = save_results(
            args.output_dir,
            results,
            single_group=args.single_group,
            batch_group=args.batch_group,
            k_hop=args.k_hop,
            repeat=args.repeat,
        )
        results["result_path"] = str(output_path)

    print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
