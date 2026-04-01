import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean, median
from typing import Dict, List


def load_results(csv_path: Path) -> List[dict]:
    rows = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["time_ms"] = float(row["time_ms"]) if row.get("time_ms") else 0.0
            row["result_count"] = int(row["result_count"]) if row.get("result_count") else 0
            row["k"] = int(row["k"]) if row.get("k") else 0
            row["batch_size"] = int(row["batch_size"]) if row.get("batch_size") else 0
            row["success"] = row.get("success") == "True"
            rows.append(row)
    return rows


def summarize_rows(rows: List[dict]) -> Dict[str, object]:
    by_type = defaultdict(list)
    khop_by_k = defaultdict(list)
    by_batch_size = defaultdict(list)
    seeds_by_type = defaultdict(set)
    error_rows = []

    for row in rows:
        by_type[row["query_type"]].append(row)
        if row["query_type"] == "k_hop":
            khop_by_k[row["k"]].append(row)
        if row["query_type"] == "batch_neighbor" and row["batch_size"]:
            by_batch_size[row["batch_size"]].append(row)
        if row.get("seed"):
            seeds_by_type[row["query_type"]].add(row["seed"])
        if not row["success"]:
            error_rows.append(row)

    tested_parameters = {
        "engine": rows[0].get("engine", "") if rows else "",
        "query_types": sorted(by_type.keys()),
        "k_values": sorted(khop_by_k.keys()),
        "batch_sizes": sorted(by_batch_size.keys()),
        "seed_counts_by_query_type": {
            query_type: len(seeds)
            for query_type, seeds in sorted(seeds_by_type.items())
        },
    }

    summary = {
        "total_runs": len(rows),
        "success_runs": sum(1 for row in rows if row["success"]),
        "tested_parameters": tested_parameters,
        "run_profile": {
            "unique_seeds": len({row["seed"] for row in rows if row.get("seed")}),
            "query_type_count": len(by_type),
        },
        "by_type": {},
        "khop_by_k": {},
        "batch_by_size": {},
        "slowest_rows": sorted(rows, key=lambda x: x["time_ms"], reverse=True)[:10],
        "error_rows": error_rows[:10],
    }

    for query_type, items in sorted(by_type.items()):
        times = [item["time_ms"] for item in items]
        counts = [item["result_count"] for item in items]
        summary["by_type"][query_type] = {
            "runs": len(items),
            "unique_seeds": len(seeds_by_type[query_type]),
            "mean_time_ms": mean(times),
            "median_time_ms": median(times),
            "min_time_ms": min(times),
            "max_time_ms": max(times),
            "mean_result_count": mean(counts),
            "min_result_count": min(counts),
            "max_result_count": max(counts),
        }

    for k, items in sorted(khop_by_k.items()):
        times = [item["time_ms"] for item in items]
        counts = [item["result_count"] for item in items]
        summary["khop_by_k"][k] = {
            "runs": len(items),
            "mean_time_ms": mean(times),
            "median_time_ms": median(times),
            "min_time_ms": min(times),
            "max_time_ms": max(times),
            "mean_result_count": mean(counts),
            "min_result_count": min(counts),
            "max_result_count": max(counts),
        }

    for batch_size, items in sorted(by_batch_size.items()):
        times = [item["time_ms"] for item in items]
        counts = [item["result_count"] for item in items]
        summary["batch_by_size"][batch_size] = {
            "runs": len(items),
            "mean_time_ms": mean(times),
            "median_time_ms": median(times),
            "min_time_ms": min(times),
            "max_time_ms": max(times),
            "mean_result_count": mean(counts),
            "min_result_count": min(counts),
            "max_result_count": max(counts),
        }

    return summary


def _format_float(value: float) -> str:
    return f"{value:.3f}"


def render_summary(summary: Dict[str, object], csv_name: str) -> str:
    total_runs = summary["total_runs"]
    success_runs = summary["success_runs"]
    success_rate = (success_runs / total_runs * 100.0) if total_runs else 0.0
    tested_parameters = summary["tested_parameters"]

    lines = [
        "# Cross-DB Graph Benchmark Summary",
        "",
        "## This Run",
        "",
        f"- engine: `{tested_parameters.get('engine', '')}`",
        f"- query types executed: {', '.join(f'`{q}`' for q in tested_parameters.get('query_types', [])) or 'none'}",
        f"- k values tested: {', '.join(str(k) for k in tested_parameters.get('k_values', [])) or 'none'}",
        f"- batch sizes tested: {', '.join(str(b) for b in tested_parameters.get('batch_sizes', [])) or 'none'}",
        f"- unique seeds in this run: {summary['run_profile']['unique_seeds']}",
        f"- raw results: `{csv_name}`",
        "",
        "## Tested Workloads and Parameters",
        "",
        "| query_type | runs | unique_seeds | parameterization |",
        "| --- | ---: | ---: | --- |",
    ]

    for query_type, stats in summary["by_type"].items():
        parameterization = []
        if query_type == "k_hop":
            k_values = tested_parameters.get("k_values", [])
            parameterization.append("k in [" + ", ".join(str(k) for k in k_values) + "]")
        if query_type == "batch_neighbor":
            batch_sizes = tested_parameters.get("batch_sizes", [])
            parameterization.append(
                "batch_size in [" + ", ".join(str(b) for b in batch_sizes) + "]"
            )
        if not parameterization:
            parameterization.append("default parameters")
        lines.append(
            f"| {query_type} | {stats['runs']} | {stats['unique_seeds']} | {'; '.join(parameterization)} |"
        )

    lines.extend(
        [
            "",
            "## Overall",
            "",
            f"- total runs: {total_runs}",
            f"- success runs: {success_runs}",
            f"- success rate: {_format_float(success_rate)}%",
            "",
            "## Seed Coverage",
            "",
            "| query_type | unique_seed_count |",
            "| --- | ---: |",
        ]
    )

    for query_type, seed_count in tested_parameters["seed_counts_by_query_type"].items():
        lines.append(f"| {query_type} | {seed_count} |")

    lines.extend(
        [
            "",
            "## By Query Type",
            "",
            "| query_type | runs | unique_seeds | mean_time_ms | median_time_ms | min_time_ms | max_time_ms | mean_result_count | min_result_count | max_result_count |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for query_type, stats in summary["by_type"].items():
        lines.append(
            "| {query_type} | {runs} | {unique_seeds} | {mean_time} | {median_time} | {min_time} | {max_time} | {mean_count} | {min_count} | {max_count} |".format(
                query_type=query_type,
                runs=stats["runs"],
                unique_seeds=stats["unique_seeds"],
                mean_time=_format_float(stats["mean_time_ms"]),
                median_time=_format_float(stats["median_time_ms"]),
                min_time=_format_float(stats["min_time_ms"]),
                max_time=_format_float(stats["max_time_ms"]),
                mean_count=_format_float(stats["mean_result_count"]),
                min_count=stats["min_result_count"],
                max_count=stats["max_result_count"],
            )
        )

    lines.extend(
        [
            "",
            "## K-Hop Breakdown",
            "",
            "| k | runs | mean_time_ms | median_time_ms | min_time_ms | max_time_ms | mean_result_count | min_result_count | max_result_count |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for k, stats in summary["khop_by_k"].items():
        lines.append(
            "| {k} | {runs} | {mean_time} | {median_time} | {min_time} | {max_time} | {mean_count} | {min_count} | {max_count} |".format(
                k=k,
                runs=stats["runs"],
                mean_time=_format_float(stats["mean_time_ms"]),
                median_time=_format_float(stats["median_time_ms"]),
                min_time=_format_float(stats["min_time_ms"]),
                max_time=_format_float(stats["max_time_ms"]),
                mean_count=_format_float(stats["mean_result_count"]),
                min_count=stats["min_result_count"],
                max_count=stats["max_result_count"],
            )
        )

    if summary["batch_by_size"]:
        lines.extend(
            [
                "",
                "## Batch Size Breakdown",
                "",
                "| batch_size | runs | mean_time_ms | median_time_ms | min_time_ms | max_time_ms | mean_result_count | min_result_count | max_result_count |",
                "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )

        for batch_size, stats in summary["batch_by_size"].items():
            lines.append(
                "| {batch_size} | {runs} | {mean_time} | {median_time} | {min_time} | {max_time} | {mean_count} | {min_count} | {max_count} |".format(
                    batch_size=batch_size,
                    runs=stats["runs"],
                    mean_time=_format_float(stats["mean_time_ms"]),
                    median_time=_format_float(stats["median_time_ms"]),
                    min_time=_format_float(stats["min_time_ms"]),
                    max_time=_format_float(stats["max_time_ms"]),
                    mean_count=_format_float(stats["mean_result_count"]),
                    min_count=stats["min_result_count"],
                    max_count=stats["max_result_count"],
                )
            )

    lines.extend(
        [
            "",
            "## Top 10 Slowest Queries",
            "",
            "| query_type | seed | k | batch_size | time_ms | result_count | success |",
            "| --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
    )

    for row in summary["slowest_rows"]:
        seed = row["seed"]
        if len(seed) > 80:
            seed = seed[:77] + "..."
        lines.append(
            f"| {row['query_type']} | {seed} | {row['k']} | {row['batch_size']} | {_format_float(row['time_ms'])} | {row['result_count']} | {row['success']} |"
        )

    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- `neighbor` reflects the 1-hop baseline cost.",
            "- `k_hop` shows how latency and result size grow with traversal depth.",
            "- `batch_neighbor` indicates whether the current batch path is efficiently optimized.",
        ]
    )

    if summary["error_rows"]:
        lines.extend(
            [
                "",
                "## Sample Errors",
                "",
                "| query_type | seed | k | batch_size | error_message |",
                "| --- | --- | ---: | ---: | --- |",
            ]
        )
        for row in summary["error_rows"]:
            error_message = (row.get("error_message") or "").replace("\n", " ")
            if len(error_message) > 120:
                error_message = error_message[:117] + "..."
            lines.append(
                f"| {row['query_type']} | {row['seed']} | {row['k']} | {row['batch_size']} | {error_message} |"
            )

    return "\n".join(lines) + "\n"


def analyze_results(output_dir: Path) -> Path:
    csv_path = output_dir / "raw_results.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"raw results not found: {csv_path}")

    rows = load_results(csv_path)
    summary = summarize_rows(rows)
    summary_path = output_dir / "summary.md"
    summary_text = render_summary(summary, csv_path.name)
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary_text)
    return summary_path


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Analyze cross-db graph benchmark results")
    parser.add_argument("output_dir", help="Result directory containing raw_results.csv")
    args = parser.parse_args()

    summary_path = analyze_results(Path(args.output_dir))
    print(f"Wrote enhanced summary: {summary_path}")


if __name__ == "__main__":
    main()
