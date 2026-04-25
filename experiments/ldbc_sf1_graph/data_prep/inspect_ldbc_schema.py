from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path

from experiments.ldbc_sf1_graph import config


@dataclass
class CsvSchemaSummary:
    relative_path: str
    section: str
    category: str
    entity_name: str
    delimiter: str
    columns: list[str]
    sample_row_count: int
    sample_rows: list[dict[str, str]]


NODE_HINTS = {"person", "post", "comment", "forum", "tag", "tagclass", "place", "organisation"}
EDGE_HINTS = {
    "knows",
    "hascreator",
    "replyof",
    "containsof",
    "containerofforum",
    "hastag",
    "hasmember",
    "hasmoderator",
    "hasinterest",
    "islocatedin",
    "ispartof",
    "issubclassof",
    "studyat",
    "workat",
}

NODE_TYPES = {"comment", "forum", "organisation", "person", "place", "post", "tag", "tagclass"}


def _normalize_stem_tokens(path: Path) -> list[str]:
    return [token for token in path.stem.lower().split("_") if token and token.isalpha()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect the extracted LDBC SF1 CSV schema")
    parser.add_argument("--input-dir", type=Path, default=config.LDBC_EXTRACT_DIR)
    parser.add_argument("--output-path", type=Path, default=config.SCHEMA_SUMMARY_PATH)
    parser.add_argument("--limit", type=int, default=100, help="Max rows to sample per CSV")
    parser.add_argument(
        "--preview-rows",
        type=int,
        default=2,
        help="Number of example rows to keep in the JSON summary and console output",
    )
    return parser.parse_args()


def classify_csv(path: Path) -> str:
    tokens = _normalize_stem_tokens(path)
    if len(tokens) == 1 and tokens[0] in NODE_TYPES:
        return "node"
    if len(tokens) >= 3 and tokens[0] in NODE_TYPES and tokens[-1] in NODE_TYPES:
        return "edge"
    if any(token in tokens for token in EDGE_HINTS):
        return "edge_or_relation"
    if any(token in tokens for token in NODE_HINTS):
        return "node_or_dimension"
    return "unknown"


def infer_section(path: Path, root_dir: Path) -> str:
    relative_parts = path.relative_to(root_dir).parts
    if len(relative_parts) > 1:
        return relative_parts[0]
    return "root"


def infer_entity_name(path: Path, category: str) -> str:
    tokens = _normalize_stem_tokens(path)
    if not tokens:
        return "unknown"
    if category == "node":
        return tokens[0]
    if category in {"edge", "edge_or_relation"}:
        return "_".join(tokens)
    return tokens[0]


def summarize_csv(path: Path, root_dir: Path, limit: int, preview_rows: int) -> CsvSchemaSummary:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="|")
        columns = reader.fieldnames or []
        sample_row_count = 0
        sample_rows: list[dict[str, str]] = []
        for row in reader:
            sample_row_count += 1
            if len(sample_rows) < preview_rows:
                sample_rows.append(dict(row))
            if sample_row_count >= limit:
                break

    category = classify_csv(path)

    return CsvSchemaSummary(
        relative_path=str(path.relative_to(root_dir)),
        section=infer_section(path, root_dir),
        category=category,
        entity_name=infer_entity_name(path, category),
        delimiter="|",
        columns=columns,
        sample_row_count=sample_row_count,
        sample_rows=sample_rows,
    )


def inspect_schema(input_dir: Path, limit: int, preview_rows: int) -> list[CsvSchemaSummary]:
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    summaries: list[CsvSchemaSummary] = []
    for path in sorted(input_dir.rglob("*.csv")):
        summaries.append(summarize_csv(path, input_dir, limit, preview_rows))
    return summaries


def build_dataset_overview(summaries: list[CsvSchemaSummary], input_dir: Path, limit: int, preview_rows: int) -> dict:
    section_counter = Counter(summary.section for summary in summaries)
    category_counter = Counter(summary.category for summary in summaries)
    node_entities = sorted(summary.entity_name for summary in summaries if summary.category == "node")
    edge_entities = sorted(summary.entity_name for summary in summaries if summary.category in {"edge", "edge_or_relation"})
    return {
        "dataset_root": str(input_dir),
        "file_count": len(summaries),
        "scan_limit": limit,
        "preview_rows": preview_rows,
        "sections": dict(section_counter),
        "categories": dict(category_counter),
        "node_entities": node_entities,
        "edge_entities": edge_entities,
        "files": [asdict(summary) for summary in summaries],
    }


def print_console_summary(overview: dict) -> None:
    print(f"Dataset root: {overview['dataset_root']}")
    print(f"Discovered CSV files: {overview['file_count']}")
    print(f"Sections: {overview['sections']}")
    print(f"Categories: {overview['categories']}")
    print(f"Node entities ({len(overview['node_entities'])}): {', '.join(overview['node_entities'])}")
    print(f"Edge entities ({len(overview['edge_entities'])}): {', '.join(overview['edge_entities'])}")
    print("\nSample file previews:")
    for file_summary in overview["files"][:8]:
        print(f"- {file_summary['relative_path']} [{file_summary['category']}] columns={file_summary['columns']}")
        if file_summary["sample_rows"]:
            print(f"  first_row={file_summary['sample_rows'][0]}")


def main() -> None:
    args = parse_args()
    config.ensure_directories()
    summaries = inspect_schema(args.input_dir, args.limit, args.preview_rows)
    payload = build_dataset_overview(summaries, args.input_dir, args.limit, args.preview_rows)
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print_console_summary(payload)
    print(f"Schema summary saved to {args.output_path}")


if __name__ == "__main__":
    main()
