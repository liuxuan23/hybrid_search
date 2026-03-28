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

    for row in rows:
        by_type[row["query_type"]].append(row)
        if row["query_type"] == "k_hop":
            khop_by_k[row["k"]].append(row)

    summary = {
        "total_runs": len(rows),
        "success_runs": sum(1 for row in rows if row["success"]),
        "by_type": {},
        "khop_by_k": {},
        "slowest_rows": sorted(rows, key=lambda x: x["time_ms"], reverse=True)[:10],
    }

    for query_type, items in sorted(by_type.items()):
        times = [item["time_ms"] for item in items]
        counts = [item["result_count"] for item in items]
        summary["by_type"][query_type] = {
            "runs": len(items),
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

    return summary


def _format_float(value: float) -> str:
    return f"{value:.3f}"


def render_summary(summary: Dict[str, object], csv_name: str) -> str:
    total_runs = summary["total_runs"]
    success_runs = summary["success_runs"]
    success_rate = (success_runs / total_runs * 100.0) if total_runs else 0.0

    lines = [
        "# Cross-DB Graph Benchmark Summary",
        "",
        "## Overall",
        "",
        f"- total runs: {total_runs}",
        f"- success runs: {success_runs}",
        f"- success rate: {_format_float(success_rate)}%",
        f"- raw results: `{csv_name}`",
        "",
        "## By Query Type",
        "",
        "| query_type | runs | mean_time_ms | median_time_ms | min_time_ms | max_time_ms | mean_result_count | min_result_count | max_result_count |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for query_type, stats in summary["by_type"].items():
        lines.append(
            "| {query_type} | {runs} | {mean_time} | {median_time} | {min_time} | {max_time} | {mean_count} | {min_count} | {max_count} |".format(
                query_type=query_type,
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
