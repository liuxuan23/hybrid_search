from __future__ import annotations

import argparse
import itertools
import json
import subprocess
import sys
from pathlib import Path

from experiments.ldbc_sf1_graph import config


DEFAULT_SINGLE_GROUPS = ["low_degree", "mid_degree", "high_degree", "random"]
DEFAULT_BATCH_GROUPS = ["low_degree", "mid_degree", "high_degree", "mixed"]
DEFAULT_BATCH_GROUP_INDICES = [0, 1, 2]
DEFAULT_K_VALUES = [1, 2, 3]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a batch of LanceDB LDBC benchmarks")
    parser.add_argument("--db-path", type=Path, default=config.LDBC_LANCEDB_DIR)
    parser.add_argument("--seeds-path", type=Path, default=config.LDBC_SEEDS_PATH)
    parser.add_argument("--output-dir", type=Path, default=config.RESULTS_DIR)
    parser.add_argument("--python-bin", type=str, default=sys.executable)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--single-groups", nargs="+", default=DEFAULT_SINGLE_GROUPS)
    parser.add_argument("--batch-groups", nargs="+", default=DEFAULT_BATCH_GROUPS)
    parser.add_argument("--batch-group-indices", nargs="+", type=int, default=DEFAULT_BATCH_GROUP_INDICES)
    parser.add_argument("--k-values", nargs="+", type=int, default=DEFAULT_K_VALUES)
    parser.add_argument(
        "--max-runs",
        type=int,
        default=0,
        help="Limit total benchmark invocations for smoke testing; 0 means run all combinations",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Pass through to benchmark runner so results are printed but not written to disk",
    )
    return parser.parse_args()


def build_commands(args: argparse.Namespace) -> list[list[str]]:
    commands = []
    for single_group, batch_group, batch_index, k_value in itertools.product(
        args.single_groups,
        args.batch_groups,
        args.batch_group_indices,
        args.k_values,
    ):
        cmd = [
            args.python_bin,
            "-m",
            "experiments.ldbc_sf1_graph.benchmarks.benchmark_lancedb_ldbc",
            "--db-path",
            str(args.db_path),
            "--seeds-path",
            str(args.seeds_path),
            "--output-dir",
            str(args.output_dir),
            "--repeat",
            str(args.repeat),
            "--single-group",
            single_group,
            "--batch-group",
            batch_group,
            "--batch-group-index",
            str(batch_index),
            "--k-hop",
            str(k_value),
        ]
        if args.no_save:
            cmd.append("--no-save")
        commands.append(cmd)

    if args.max_runs > 0:
        return commands[: args.max_runs]
    return commands


def main() -> None:
    args = parse_args()
    commands = build_commands(args)
    if not commands:
        raise ValueError("No benchmark commands generated")

    summary = {
        "total_runs": len(commands),
        "completed_runs": [],
        "failed_runs": [],
    }

    for index, cmd in enumerate(commands, start=1):
        print(f"[{index}/{len(commands)}] Running: {' '.join(cmd)}")
        completed = subprocess.run(
            cmd,
            cwd=str(config.PROJECT_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
        if completed.returncode != 0:
            summary["failed_runs"].append(
                {
                    "command": cmd,
                    "returncode": completed.returncode,
                    "stdout": completed.stdout,
                    "stderr": completed.stderr,
                }
            )
            print(completed.stdout)
            print(completed.stderr, file=sys.stderr)
            break

        stdout = completed.stdout.strip()
        run_payload = json.loads(stdout) if stdout else {}
        summary["completed_runs"].append(
            {
                "command": cmd,
                "result_path": run_payload.get("result_path"),
                "config": run_payload.get("config", {}),
            }
        )
        print(f"Completed run {index}: {run_payload.get('result_path', '<no result path>')}")

    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if summary["failed_runs"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
