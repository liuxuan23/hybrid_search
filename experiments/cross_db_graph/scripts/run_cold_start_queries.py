import argparse
import json
import os
import shutil
import random
import socket
import statistics
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from experiments.cross_db_graph import config


QUERY_LABELS = {
    ("neighbor", 1): "neighbor",
    ("k_hop", 2): "k_hop_2",
    ("k_hop", 3): "k_hop_3",
}

QUERY_SPECS = [
    {"query_type": "neighbor", "k": 0, "expected_label": "neighbor"},
    {"query_type": "k_hop", "k": 2, "expected_label": "k_hop_2"},
    {"query_type": "k_hop", "k": 3, "expected_label": "k_hop_3"},
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
    query_type: str
    query_k: int
    direction: str
    materialize: bool | None
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


def require_root_for_engine(engine: str) -> bool:
    return engine in {"lancedb", "lance_graph", "postgres", "postgres_age", "arangodb"}


def parse_args():
    parser = argparse.ArgumentParser(description="Run repeated cold-start single-seed graph queries")
    parser.add_argument(
        "--engine",
        choices=["lancedb", "lance_graph", "postgres", "postgres_age", "arangodb"],
        required=True,
    )
    parser.add_argument("--seeds-file", default=str(config.SEEDS_FILE))
    parser.add_argument("--seed-group", default="high_degree")
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--sample-strategy", choices=["head", "random"], default="head")
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--direction", choices=["out", "in"], default="out")
    parser.add_argument("--materialize", choices=["true", "false"], default=None)
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--restart-wait-seconds", type=float, default=2.0)
    parser.add_argument("--post-drop-sleep-seconds", type=float, default=0.25)
    parser.add_argument(
        "--all-degree-groups",
        action="store_true",
        help="Run `low_degree`, `medium_degree`, and `high_degree` together and summarize by group",
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


def write_json(path: Path, payload: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def load_seed_candidates(seeds_file: Path) -> dict[str, list[SeedRecord]]:
    data = json.loads(seeds_file.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ColdStartError(f"Unsupported seeds.json format in {seeds_file}: expected top-level object")

    groups: dict[str, list[SeedRecord]] = {}
    for group_name, values in data.items():
        if not isinstance(values, list):
            raise ColdStartError(f"Unsupported seed group format for '{group_name}': expected list")
        group_records: list[SeedRecord] = []
        for value in values:
            if isinstance(value, str):
                group_records.append(SeedRecord(seed=value, group=group_name, metadata={}))
            elif isinstance(value, dict) and isinstance(value.get("seed"), str):
                metadata = {k: v for k, v in value.items() if k != "seed"}
                group_records.append(SeedRecord(seed=value["seed"], group=group_name, metadata=metadata))
            else:
                raise ColdStartError(
                    f"Unsupported seed entry in group '{group_name}': expected string or object with 'seed'"
                )
        groups[group_name] = group_records
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
        requested_groups = ["low_degree", "medium_degree", "high_degree"]
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


def make_output_dir(engine: str, output_dir: str | None) -> Path:
    if output_dir:
        path = Path(output_dir)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = config.RESULTS_DIR / f"cold_{engine}_{timestamp}"
    path.mkdir(parents=True, exist_ok=True)
    (path / "per_run").mkdir(exist_ok=True)
    (path / "raw").mkdir(exist_ok=True)
    return path


def drop_linux_page_caches():
    subprocess.run(["sync"], check=True)
    with open("/proc/sys/vm/drop_caches", "w", encoding="utf-8") as f:
        f.write("3\n")


def restart_service(service_name: str):
    if shutil.which("systemctl"):
        subprocess.run(["systemctl", "restart", service_name], check=True)
        subprocess.run(["systemctl", "is-active", "--quiet", service_name], check=True)
        return
    subprocess.run(["service", service_name, "restart"], check=True)


def prepare_engine_for_cold_query(engine: str, restart_wait_seconds: float, post_drop_sleep_seconds: float) -> PrepActionRecord:
    started = time.perf_counter()
    restarted_service = False
    service_name = None

    if engine in {"postgres", "postgres_age"}:
        service_name = "postgresql"
        restart_service(service_name)
        restarted_service = True
        time.sleep(restart_wait_seconds)
    elif engine == "arangodb":
        service_name = "arangodb3"
        restart_service(service_name)
        restarted_service = True
        time.sleep(restart_wait_seconds)

    drop_linux_page_caches()
    if post_drop_sleep_seconds > 0:
        time.sleep(post_drop_sleep_seconds)

    return PrepActionRecord(
        engine=engine,
        restarted_service=restarted_service,
        service_name=service_name,
        dropped_page_cache=True,
        prep_time_ms=(time.perf_counter() - started) * 1000.0,
    )


def build_single_seed_command(args, seed: str) -> list[str]:
    command = [
        "uv",
        "run",
        "python",
        "-m",
        "experiments.cross_db_graph.scripts.run_single_seed_queries",
        "--engine",
        args.engine,
        "--seed",
        seed,
        "--direction",
        args.direction,
        "--json",
    ]
    if args.materialize is not None:
        command.extend(["--materialize", args.materialize])
    if args.db_path is not None:
        command.extend(["--db-path", args.db_path])
    return command


def select_single_query_result(payload: list[dict[str, Any]], query_type: str, query_k: int) -> dict[str, Any]:
    for item in payload:
        item_type = item.get("query_type")
        item_k = int(item.get("k", 0))
        if query_type == "neighbor":
            if item_type == "neighbor":
                return item
        elif item_type == query_type and item_k == query_k:
            return item
    raise ColdStartError(f"Unable to find query result for query_type={query_type}, k={query_k}")


def parse_json_output(stdout: str) -> list[dict[str, Any]]:
    stripped = stdout.strip()
    if not stripped:
        raise ColdStartError("Child process produced empty stdout; expected JSON")
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise ColdStartError(f"Failed to parse child JSON output: {exc}") from exc
    if not isinstance(payload, list):
        raise ColdStartError("Child JSON output must be a list")
    return payload


def run_single_seed_subprocess(
    args,
    seed: str,
    timeout_seconds: float,
    query_label: str,
) -> tuple[list[dict[str, Any]], float, subprocess.CompletedProcess[str]]:
    command = build_single_seed_command(args, seed)
    command.extend(["--query-spec", query_label])
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
        cwd=str(Path(__file__).resolve().parents[3]),
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
    query_buckets: dict[str, list[dict[str, Any]]] = {"neighbor": [], "k_hop_2": [], "k_hop_3": []}
    for record in successful:
        if record.query_result is not None:
            query_buckets[record.query_label].append(record.query_result)

    summary = {
        "total_runs": len(records),
        "successful_runs": len(successful),
        "failed_runs": len(records) - len(successful),
        "avg_prep_time_ms": statistics.mean([record.prep["prep_time_ms"] for record in records]) if records else None,
        "avg_invoke_time_ms": statistics.mean([record.invoke_time_ms for record in successful if record.invoke_time_ms is not None]) if successful else None,
        "queries": {},
    }

    for label, bucket in query_buckets.items():
        times = [float(item["time_ms"]) for item in bucket if item.get("time_ms") is not None]
        counts = [int(item["result_count"]) for item in bucket if item.get("result_count") is not None]
        summary["queries"][label] = {
            "runs": len(bucket),
            "avg_ms": statistics.mean(times) if times else None,
            "median_ms": statistics.median(times) if times else None,
            "p95_ms": percentile(times, 0.95) if times else None,
            "min_ms": min(times) if times else None,
            "max_ms": max(times) if times else None,
            "avg_result_count": statistics.mean(counts) if counts else None,
        }
    return summary


def summarize_runs(records: list[SingleRunRecord]) -> dict[str, Any]:
    summary = build_metrics(records)
    grouped: dict[str, Any] = {}
    for group_name in sorted({record.group for record in records}):
        grouped[group_name] = build_metrics([record for record in records if record.group == group_name])
    summary["by_group"] = grouped
    return summary


def render_summary_markdown(args, summary: dict[str, Any]) -> str:
    lines = [
        "# Cold Start Query Summary",
        "",
        f"- engine: `{args.engine}`",
        f"- seed_group: `{'low_degree,medium_degree,high_degree' if args.all_degree_groups else args.seed_group}`",
        f"- sample_size: `{args.sample_size}`",
        f"- successful_runs: `{summary['successful_runs']}/{summary['total_runs']}`",
        "",
        "## Timing Semantics",
        "",
        "- `prep_time_ms`: 服务重启 / page cache 清理等预处理耗时，不计入查询调用时间",
        "- `invoke_time_ms`: 从启动 `run_single_seed_queries.py` 开始，到其结束为止的总调用耗时",
        "- `query time_ms`: 查询脚本内部记录的单条查询执行时间",
        "",
        "## Overall",
        "",
        f"- avg prep_time_ms: `{summary['avg_prep_time_ms']:.3f}`" if summary["avg_prep_time_ms"] is not None else "- avg prep_time_ms: `n/a`",
        f"- avg invoke_time_ms: `{summary['avg_invoke_time_ms']:.3f}`" if summary["avg_invoke_time_ms"] is not None else "- avg invoke_time_ms: `n/a`",
        "",
        "## Query Metrics",
        "",
        "| query | runs | avg_ms | median_ms | p95_ms | min_ms | max_ms | avg_count |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label, metrics in summary["queries"].items():
        def fmt(value):
            if value is None:
                return "n/a"
            if isinstance(value, float):
                return f"{value:.3f}"
            return str(value)

        lines.append(
            f"| {label} | {metrics['runs']} | {fmt(metrics['avg_ms'])} | {fmt(metrics['median_ms'])} | {fmt(metrics['p95_ms'])} | {fmt(metrics['min_ms'])} | {fmt(metrics['max_ms'])} | {fmt(metrics['avg_result_count'])} |"
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
                    "| query | runs | avg_ms | median_ms | p95_ms | min_ms | max_ms | avg_count |",
                    "|---|---:|---:|---:|---:|---:|---:|---:|",
                ]
            )
            for label, metrics in group_summary["queries"].items():
                lines.append(
                    f"| {label} | {metrics['runs']} | {fmt(metrics['avg_ms'])} | {fmt(metrics['median_ms'])} | {fmt(metrics['p95_ms'])} | {fmt(metrics['min_ms'])} | {fmt(metrics['max_ms'])} | {fmt(metrics['avg_result_count'])} |"
                )
    return "\n".join(lines) + "\n"


def main():
    args = parse_args()
    if require_root_for_engine(args.engine) and os.geteuid() != 0:
        raise ColdStartError("run_cold_start_queries.py requires root privileges to clear OS page cache")

    seeds_file = Path(args.seeds_file)
    groups = load_seed_candidates(seeds_file)
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
            "seeds_file": str(seeds_file),
            "seed_group": args.seed_group,
            "all_degree_groups": args.all_degree_groups,
            "sample_size": args.sample_size,
            "sample_strategy": args.sample_strategy,
            "random_seed": args.random_seed,
            "direction": args.direction,
            "materialize": args.materialize,
            "db_path": args.db_path,
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
            query_label = query_spec["expected_label"]
            raw_stdout = output_dir / "raw" / f"{run_index:04d}.stdout.txt"
            raw_stderr = output_dir / "raw" / f"{run_index:04d}.stderr.txt"
            prep = None
            try:
                prep = prepare_engine_for_cold_query(args.engine, args.restart_wait_seconds, args.post_drop_sleep_seconds)
                query_results, invoke_time_ms, completed = run_single_seed_subprocess(
                    args,
                    seed_record.seed,
                    args.timeout_seconds,
                    query_label,
                )
                raw_stdout.write_text(completed.stdout, encoding="utf-8")
                raw_stderr.write_text(completed.stderr, encoding="utf-8")
                selected_result = select_single_query_result(
                    query_results,
                    query_type=query_spec["query_type"],
                    query_k=query_spec["k"],
                )
                record = SingleRunRecord(
                    run_index=run_index,
                    engine=args.engine,
                    seed=seed_record.seed,
                    group=seed_record.group,
                    query_label=query_label,
                    query_type=query_spec["query_type"],
                    query_k=query_spec["k"],
                    direction=args.direction,
                    materialize=None if args.materialize is None else args.materialize == "true",
                    prep=asdict(prep),
                    invoke_time_ms=invoke_time_ms,
                    success=True,
                    returncode=completed.returncode,
                    query_result=selected_result,
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
                    query_type=query_spec["query_type"],
                    query_k=query_spec["k"],
                    direction=args.direction,
                    materialize=None if args.materialize is None else args.materialize == "true",
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
