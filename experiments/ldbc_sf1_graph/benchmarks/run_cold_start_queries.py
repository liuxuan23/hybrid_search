from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import socket
import statistics
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from experiments.ldbc_sf1_graph import config


QUERY_SPECS = [
    {"query_spec": "neighbor"},
    {"query_spec": "k_hop_2"},
    {"query_spec": "k_hop_3"},
]


@dataclass
class SeedRecord:
    seed: str
    group: str
    metadata: dict[str, Any]


@dataclass
class PrepActionRecord:
    engine: str
    restarted_service: bool
    service_name: str | None
    dropped_page_cache: bool
    prep_time_ms: float


@dataclass
class SingleRunRecord:
    run_index: int
    engine: str
    seed: str
    group: str
    query_label: str
    direction: str
    materialize: bool
    prep: dict[str, Any]
    invoke_time_ms: float | None
    success: bool
    returncode: int | None
    query_result: dict[str, Any] | None
    error: str | None
    stdout_path: str | None
    stderr_path: str | None
    started_at: str
    finished_at: str


class ColdStartError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run repeated cold-start LDBC SF1 single-seed graph queries")
    parser.add_argument("--engine", default="lancedb", choices=["lancedb"])
    parser.add_argument("--seeds-file", type=Path, default=config.LDBC_SEEDS_PATH)
    parser.add_argument("--seed-group", default="high_degree")
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--sample-strategy", choices=["head", "random"], default="head")
    parser.add_argument("--random-seed", type=int, default=config.DEFAULT_RANDOM_SEED)
    parser.add_argument("--direction", choices=["out", "in", "both"], default="out")
    parser.add_argument("--materialize", choices=["true", "false"], default="true")
    parser.add_argument("--db-path", type=Path, default=config.LDBC_LANCEDB_DIR)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--restart-wait-seconds", type=float, default=0.0)
    parser.add_argument("--post-drop-sleep-seconds", type=float, default=0.25)
    parser.add_argument(
        "--all-degree-groups",
        action="store_true",
        help="Run `low_degree`, `mid_degree`, and `high_degree` together and summarize by group",
    )
    return parser.parse_args()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sanitize_seed(seed: str) -> str:
    return seed.replace(":", "__").replace("/", "_")


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    rank = (len(ordered) - 1) * p
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def compact_query_result(result: dict[str, Any]) -> dict[str, Any]:
    io_stats = result.get("io_stats")
    compact: dict[str, Any] = {
        "count": result.get("count"),
        "time_ms": result.get("time_ms"),
    }
    if isinstance(io_stats, dict):
        compact["io_stats"] = {
            "read_bytes": io_stats.get("read_bytes", 0),
        }
    return compact


def load_seed_candidates(seeds_file: Path) -> dict[str, list[SeedRecord]]:
    data = json.loads(seeds_file.read_text(encoding="utf-8"))
    single_seeds = data.get("single_seeds")
    if not isinstance(single_seeds, dict):
        raise ColdStartError(f"Unsupported seeds.json format in {seeds_file}: expected 'single_seeds' object")

    groups: dict[str, list[SeedRecord]] = {}
    for group_name, values in single_seeds.items():
        if not isinstance(values, list):
            raise ColdStartError(f"Unsupported seed group format for '{group_name}': expected list")
        groups[group_name] = [SeedRecord(seed=str(value), group=group_name, metadata={}) for value in values]
    return groups


def select_from_candidates(candidates: list[SeedRecord], sample_size: int, sample_strategy: str, rng: random.Random) -> list[SeedRecord]:
    if not candidates:
        raise ColdStartError("Candidate seed set is empty")
    if sample_size <= 0:
        raise ColdStartError("--sample-size must be > 0")
    sample_size = min(sample_size, len(candidates))
    if sample_strategy == "head":
        return candidates[:sample_size]
    if sample_strategy == "random":
        return rng.sample(candidates, sample_size)
    raise ColdStartError(f"Unsupported sample strategy: {sample_strategy}")


def select_seeds(
    groups: dict[str, list[SeedRecord]],
    seed_group: str,
    sample_size: int,
    sample_strategy: str,
    rng: random.Random,
    all_degree_groups: bool,
) -> list[SeedRecord]:
    if all_degree_groups:
        requested_groups = ["low_degree", "mid_degree", "high_degree"]
        selected: list[SeedRecord] = []
        for group_name in requested_groups:
            if group_name not in groups:
                raise ColdStartError(
                    f"Missing required degree group '{group_name}'. Available groups: {', '.join(sorted(groups))}"
                )
            selected.extend(select_from_candidates(groups[group_name], sample_size, sample_strategy, rng))
        return selected

    if seed_group not in groups:
        raise ColdStartError(f"Seed group '{seed_group}' not found. Available groups: {', '.join(sorted(groups))}")
    return select_from_candidates(groups[seed_group], sample_size, sample_strategy, rng)


def make_output_dir(engine: str, output_dir: Path | None) -> Path:
    if output_dir:
        path = output_dir
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = config.RESULTS_DIR / f"cold_{engine}_{timestamp}"
    path.mkdir(parents=True, exist_ok=True)
    (path / "per_run").mkdir(exist_ok=True)
    (path / "raw").mkdir(exist_ok=True)
    return path


def drop_linux_page_caches() -> None:
    subprocess.run(["sync"], check=True)
    with open("/proc/sys/vm/drop_caches", "w", encoding="utf-8") as f:
        f.write("3\n")


def prepare_engine_for_cold_query(restart_wait_seconds: float, post_drop_sleep_seconds: float) -> PrepActionRecord:
    started = time.perf_counter()
    if restart_wait_seconds > 0:
        time.sleep(restart_wait_seconds)
    drop_linux_page_caches()
    if post_drop_sleep_seconds > 0:
        time.sleep(post_drop_sleep_seconds)
    return PrepActionRecord(
        engine="lancedb",
        restarted_service=False,
        service_name=None,
        dropped_page_cache=True,
        prep_time_ms=(time.perf_counter() - started) * 1000.0,
    )


def build_single_seed_command(args: argparse.Namespace, seed: str, query_spec: str) -> list[str]:
    return [
        os.environ.get("PYTHON_BIN", shutil.which("python3") or "python3"),
        "-m",
        "experiments.ldbc_sf1_graph.benchmarks.run_single_ldbc_query",
        "--db-path",
        str(args.db_path),
        "--seed",
        seed,
        "--query-spec",
        query_spec,
        "--direction",
        args.direction,
        "--materialize",
        args.materialize,
        "--json",
    ]


def parse_json_output(stdout: str) -> dict[str, Any]:
    stripped = stdout.strip()
    if not stripped:
        raise ColdStartError("Child process produced empty stdout; expected JSON")
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise ColdStartError(f"Failed to parse child JSON output: {exc}") from exc
    if not isinstance(payload, dict):
        raise ColdStartError("Child JSON output must be an object")
    return payload


def run_single_seed_subprocess(
    args: argparse.Namespace,
    seed: str,
    timeout_seconds: float,
    query_spec: str,
) -> tuple[dict[str, Any], float, subprocess.CompletedProcess[str]]:
    command = build_single_seed_command(args, seed, query_spec)
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
        cwd=str(config.PROJECT_ROOT),
    )
    invoke_time_ms = (time.perf_counter() - started) * 1000.0
    if completed.returncode != 0:
        raise ColdStartError(
            f"Child process exited with code {completed.returncode}: {completed.stderr.strip() or completed.stdout.strip()}"
        )
    payload = parse_json_output(completed.stdout)
    return payload, invoke_time_ms, completed


def build_metrics(records: list[SingleRunRecord]) -> dict[str, Any]:
    successful = [record for record in records if record.success]
    query_buckets: dict[str, list[dict[str, Any]]] = {spec["query_spec"]: [] for spec in QUERY_SPECS}
    for record in successful:
        if record.query_result is not None:
            query_buckets[record.query_label].append(record.query_result)

    summary: dict[str, Any] = {
        "total_runs": len(records),
        "successful_runs": len(successful),
        "failed_runs": len(records) - len(successful),
        "avg_prep_time_ms": statistics.mean([record.prep["prep_time_ms"] for record in records if record.prep.get("prep_time_ms") is not None]) if records else None,
        "avg_invoke_time_ms": statistics.mean([record.invoke_time_ms for record in successful if record.invoke_time_ms is not None]) if successful else None,
        "queries": {},
    }

    for label, bucket in query_buckets.items():
        times = [float(item["time_ms"]) for item in bucket if item.get("time_ms") is not None]
        counts = [int(item["count"]) for item in bucket if item.get("count") is not None]
        read_bytes = [int((item.get("io_stats") or {}).get("read_bytes", 0)) for item in bucket]
        summary["queries"][label] = {
            "runs": len(bucket),
            "avg_ms": statistics.mean(times) if times else None,
            "median_ms": statistics.median(times) if times else None,
            "p95_ms": percentile(times, 0.95) if times else None,
            "min_ms": min(times) if times else None,
            "max_ms": max(times) if times else None,
            "avg_result_count": statistics.mean(counts) if counts else None,
            "avg_read_bytes": statistics.mean(read_bytes) if read_bytes else None,
        }
    return summary


def summarize_runs(records: list[SingleRunRecord]) -> dict[str, Any]:
    summary = build_metrics(records)
    grouped: dict[str, Any] = {}
    for group_name in sorted({record.group for record in records}):
        grouped[group_name] = build_metrics([record for record in records if record.group == group_name])
    summary["by_group"] = grouped
    return summary


def fmt_metric(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def render_summary_markdown(args: argparse.Namespace, summary: dict[str, Any]) -> str:
    lines = [
        "# Cold Start Query Summary",
        "",
        f"- engine: `{args.engine}`",
        f"- seed_group: `{'low_degree,mid_degree,high_degree' if args.all_degree_groups else args.seed_group}`",
        f"- sample_size: `{args.sample_size}`",
        f"- direction: `{args.direction}`",
        f"- materialize: `{args.materialize}`",
        f"- successful_runs: `{summary['successful_runs']}/{summary['total_runs']}`",
        "",
        "## Timing Semantics",
        "",
        "- `prep_time_ms`: page cache 清理与等待时间，不计入查询调用时间",
        "- `invoke_time_ms`: 从启动 `run_single_ldbc_query.py` 到其结束为止的总调用耗时",
        "- `query time_ms`: 查询脚本内部记录的单条查询执行时间",
        "",
        "## Overall",
        "",
        f"- avg prep_time_ms: `{summary['avg_prep_time_ms']:.3f}`" if summary["avg_prep_time_ms"] is not None else "- avg prep_time_ms: `n/a`",
        f"- avg invoke_time_ms: `{summary['avg_invoke_time_ms']:.3f}`" if summary["avg_invoke_time_ms"] is not None else "- avg invoke_time_ms: `n/a`",
        "",
        "## Query Metrics",
        "",
        "| query | runs | avg_ms | median_ms | p95_ms | min_ms | max_ms | avg_count | avg_read_bytes |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label, metrics in summary["queries"].items():
        lines.append(
            f"| {label} | {metrics['runs']} | {fmt_metric(metrics['avg_ms'])} | {fmt_metric(metrics['median_ms'])} | {fmt_metric(metrics['p95_ms'])} | {fmt_metric(metrics['min_ms'])} | {fmt_metric(metrics['max_ms'])} | {fmt_metric(metrics['avg_result_count'])} | {fmt_metric(metrics['avg_read_bytes'])} |"
        )

    if summary.get("by_group"):
        lines.extend(["", "## Group Metrics", ""])
        for group_name, group_summary in summary["by_group"].items():
            lines.extend(
                [
                    f"### {group_name}",
                    "",
                    f"- successful_runs: `{group_summary['successful_runs']}/{group_summary['total_runs']}`",
                    f"- avg prep_time_ms: `{group_summary['avg_prep_time_ms']:.3f}`" if group_summary["avg_prep_time_ms"] is not None else "- avg prep_time_ms: `n/a`",
                    f"- avg invoke_time_ms: `{group_summary['avg_invoke_time_ms']:.3f}`" if group_summary["avg_invoke_time_ms"] is not None else "- avg invoke_time_ms: `n/a`",
                    "",
                    "| query | runs | avg_ms | median_ms | p95_ms | min_ms | max_ms | avg_count | avg_read_bytes |",
                    "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
                ]
            )
            for label, metrics in group_summary["queries"].items():
                lines.append(
                    f"| {label} | {metrics['runs']} | {fmt_metric(metrics['avg_ms'])} | {fmt_metric(metrics['median_ms'])} | {fmt_metric(metrics['p95_ms'])} | {fmt_metric(metrics['min_ms'])} | {fmt_metric(metrics['max_ms'])} | {fmt_metric(metrics['avg_result_count'])} | {fmt_metric(metrics['avg_read_bytes'])} |"
                )
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    if os.geteuid() != 0:
        raise ColdStartError("run_cold_start_queries.py requires root privileges to clear OS page cache")

    config.ensure_directories()
    groups = load_seed_candidates(args.seeds_file)
    rng = random.Random(args.random_seed)
    selected = select_seeds(
        groups,
        args.seed_group,
        args.sample_size,
        args.sample_strategy,
        rng,
        args.all_degree_groups,
    )
    output_dir = make_output_dir(args.engine, args.output_dir)

    write_json(
        output_dir / "run_config.json",
        {
            "engine": args.engine,
            "seeds_file": str(args.seeds_file),
            "seed_group": args.seed_group,
            "all_degree_groups": args.all_degree_groups,
            "sample_size": args.sample_size,
            "sample_strategy": args.sample_strategy,
            "random_seed": args.random_seed,
            "direction": args.direction,
            "materialize": args.materialize,
            "db_path": str(args.db_path),
            "timeout_seconds": args.timeout_seconds,
            "continue_on_error": args.continue_on_error,
            "restart_wait_seconds": args.restart_wait_seconds,
            "post_drop_sleep_seconds": args.post_drop_sleep_seconds,
            "host": socket.gethostname(),
            "cwd": os.getcwd(),
            "started_at": now_iso(),
        },
    )
    write_json(output_dir / "selected_seeds.json", [asdict(seed) for seed in selected])

    records: list[SingleRunRecord] = []
    runs_jsonl = output_dir / "runs.jsonl"

    run_index = 0
    stop_requested = False
    for seed_record in selected:
        for query_spec in QUERY_SPECS:
            run_index += 1
            started_at = now_iso()
            query_label = query_spec["query_spec"]
            raw_stdout = output_dir / "raw" / f"{run_index:04d}.stdout.txt"
            raw_stderr = output_dir / "raw" / f"{run_index:04d}.stderr.txt"
            prep = None
            try:
                prep = prepare_engine_for_cold_query(args.restart_wait_seconds, args.post_drop_sleep_seconds)
                query_result, invoke_time_ms, completed = run_single_seed_subprocess(
                    args,
                    seed_record.seed,
                    args.timeout_seconds,
                    query_label,
                )
                raw_stdout.write_text(completed.stdout, encoding="utf-8")
                raw_stderr.write_text(completed.stderr, encoding="utf-8")
                record = SingleRunRecord(
                    run_index=run_index,
                    engine=args.engine,
                    seed=seed_record.seed,
                    group=seed_record.group,
                    query_label=query_label,
                    direction=args.direction,
                    materialize=args.materialize == "true",
                    prep=asdict(prep),
                    invoke_time_ms=invoke_time_ms,
                    success=True,
                    returncode=completed.returncode,
                    query_result=compact_query_result(query_result),
                    error=None,
                    stdout_path=str(raw_stdout.relative_to(output_dir)),
                    stderr_path=str(raw_stderr.relative_to(output_dir)),
                    started_at=started_at,
                    finished_at=now_iso(),
                )
            except Exception as exc:
                record = SingleRunRecord(
                    run_index=run_index,
                    engine=args.engine,
                    seed=seed_record.seed,
                    group=seed_record.group,
                    query_label=query_label,
                    direction=args.direction,
                    materialize=args.materialize == "true",
                    prep=asdict(prep) if prep is not None else {
                        "engine": args.engine,
                        "restarted_service": False,
                        "service_name": None,
                        "dropped_page_cache": False,
                        "prep_time_ms": None,
                    },
                    invoke_time_ms=None,
                    success=False,
                    returncode=None,
                    query_result=None,
                    error=str(exc),
                    stdout_path=str(raw_stdout.relative_to(output_dir)) if raw_stdout.exists() else None,
                    stderr_path=str(raw_stderr.relative_to(output_dir)) if raw_stderr.exists() else None,
                    started_at=started_at,
                    finished_at=now_iso(),
                )
                if not args.continue_on_error:
                    stop_requested = True
            records.append(record)
            write_json(
                output_dir / "per_run" / f"{run_index:04d}_{query_label}_{sanitize_seed(seed_record.seed)}.json",
                asdict(record),
            )
            append_jsonl(runs_jsonl, asdict(record))

            if stop_requested:
                break
        if stop_requested:
            break

    summary = summarize_runs(records)
    write_json(output_dir / "summary.json", summary)
    (output_dir / "summary.md").write_text(render_summary_markdown(args, summary), encoding="utf-8")
    print(f"Cold-start run complete. Results written to: {output_dir}")


if __name__ == "__main__":
    main()
