from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from experiments.ldbc_sf1_graph import config


@dataclass
class NodeRecord:
    node_id: str
    node_type: str
    raw_id: str
    attrs_json: str


@dataclass
class EdgeRecord:
    edge_id: str
    src_id: str
    dst_id: str
    edge_type: str
    src_type: str
    dst_type: str
    attrs_json: str


NODE_FILE_REGISTRY = {
    "dynamic/person_0_0.csv": {"node_type": "Person", "id_column": "id"},
    "dynamic/post_0_0.csv": {"node_type": "Post", "id_column": "id"},
    "dynamic/comment_0_0.csv": {"node_type": "Comment", "id_column": "id"},
    "dynamic/forum_0_0.csv": {"node_type": "Forum", "id_column": "id"},
    "static/tag_0_0.csv": {"node_type": "Tag", "id_column": "id"},
    "static/tagclass_0_0.csv": {"node_type": "TagClass", "id_column": "id"},
    "static/place_0_0.csv": {"node_type": "Place", "id_column": "id"},
    "static/organisation_0_0.csv": {"node_type": "Organisation", "id_column": "id"},
}

EDGE_FILE_REGISTRY = {
    "dynamic/person_knows_person_0_0.csv": {
        "edge_type": "knows",
        "src_type": "Person",
        "dst_type": "Person",
        "src_column": "Person.id__0",
        "dst_column": "Person.id__1",
    },
    "dynamic/post_hasCreator_person_0_0.csv": {
        "edge_type": "hasCreator",
        "src_type": "Post",
        "dst_type": "Person",
        "src_column": "Post.id",
        "dst_column": "Person.id",
    },
    "dynamic/comment_hasCreator_person_0_0.csv": {
        "edge_type": "hasCreator",
        "src_type": "Comment",
        "dst_type": "Person",
        "src_column": "Comment.id",
        "dst_column": "Person.id",
    },
    "dynamic/comment_replyOf_post_0_0.csv": {
        "edge_type": "replyOf",
        "src_type": "Comment",
        "dst_type": "Post",
        "src_column": "Comment.id",
        "dst_column": "Post.id",
    },
    "dynamic/comment_replyOf_comment_0_0.csv": {
        "edge_type": "replyOf",
        "src_type": "Comment",
        "dst_type": "Comment",
        "src_column": "Comment.id__0",
        "dst_column": "Comment.id__1",
    },
    "dynamic/forum_containerOf_post_0_0.csv": {
        "edge_type": "containerOf",
        "src_type": "Forum",
        "dst_type": "Post",
        "src_column": "Forum.id",
        "dst_column": "Post.id",
    },
    "dynamic/forum_hasMember_person_0_0.csv": {
        "edge_type": "hasMember",
        "src_type": "Forum",
        "dst_type": "Person",
        "src_column": "Forum.id",
        "dst_column": "Person.id",
    },
    "dynamic/forum_hasModerator_person_0_0.csv": {
        "edge_type": "hasModerator",
        "src_type": "Forum",
        "dst_type": "Person",
        "src_column": "Forum.id",
        "dst_column": "Person.id",
    },
    "dynamic/post_hasTag_tag_0_0.csv": {
        "edge_type": "hasTag",
        "src_type": "Post",
        "dst_type": "Tag",
        "src_column": "Post.id",
        "dst_column": "Tag.id",
    },
    "dynamic/comment_hasTag_tag_0_0.csv": {
        "edge_type": "hasTag",
        "src_type": "Comment",
        "dst_type": "Tag",
        "src_column": "Comment.id",
        "dst_column": "Tag.id",
    },
    "dynamic/person_hasInterest_tag_0_0.csv": {
        "edge_type": "hasInterest",
        "src_type": "Person",
        "dst_type": "Tag",
        "src_column": "Person.id",
        "dst_column": "Tag.id",
    },
    "dynamic/person_isLocatedIn_place_0_0.csv": {
        "edge_type": "isLocatedIn",
        "src_type": "Person",
        "dst_type": "Place",
        "src_column": "Person.id",
        "dst_column": "Place.id",
    },
    "dynamic/post_isLocatedIn_place_0_0.csv": {
        "edge_type": "isLocatedIn",
        "src_type": "Post",
        "dst_type": "Place",
        "src_column": "Post.id",
        "dst_column": "Place.id",
    },
    "dynamic/comment_isLocatedIn_place_0_0.csv": {
        "edge_type": "isLocatedIn",
        "src_type": "Comment",
        "dst_type": "Place",
        "src_column": "Comment.id",
        "dst_column": "Place.id",
    },
    "dynamic/person_studyAt_organisation_0_0.csv": {
        "edge_type": "studyAt",
        "src_type": "Person",
        "dst_type": "Organisation",
        "src_column": "Person.id",
        "dst_column": "Organisation.id",
    },
    "dynamic/person_workAt_organisation_0_0.csv": {
        "edge_type": "workAt",
        "src_type": "Person",
        "dst_type": "Organisation",
        "src_column": "Person.id",
        "dst_column": "Organisation.id",
    },
    "dynamic/person_likes_post_0_0.csv": {
        "edge_type": "likes",
        "src_type": "Person",
        "dst_type": "Post",
        "src_column": "Person.id",
        "dst_column": "Post.id",
    },
    "dynamic/person_likes_comment_0_0.csv": {
        "edge_type": "likes",
        "src_type": "Person",
        "dst_type": "Comment",
        "src_column": "Person.id",
        "dst_column": "Comment.id",
    },
    "dynamic/forum_hasTag_tag_0_0.csv": {
        "edge_type": "hasTag",
        "src_type": "Forum",
        "dst_type": "Tag",
        "src_column": "Forum.id",
        "dst_column": "Tag.id",
    },
    "static/organisation_isLocatedIn_place_0_0.csv": {
        "edge_type": "isLocatedIn",
        "src_type": "Organisation",
        "dst_type": "Place",
        "src_column": "Organisation.id",
        "dst_column": "Place.id",
    },
    "static/place_isPartOf_place_0_0.csv": {
        "edge_type": "isPartOf",
        "src_type": "Place",
        "dst_type": "Place",
        "src_column": "Place.id__0",
        "dst_column": "Place.id__1",
    },
    "static/tag_hasType_tagclass_0_0.csv": {
        "edge_type": "hasType",
        "src_type": "Tag",
        "dst_type": "TagClass",
        "src_column": "Tag.id",
        "dst_column": "TagClass.id",
    },
    "static/tagclass_isSubclassOf_tagclass_0_0.csv": {
        "edge_type": "isSubclassOf",
        "src_type": "TagClass",
        "dst_type": "TagClass",
        "src_column": "TagClass.id__0",
        "dst_column": "TagClass.id__1",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build normalized nodes/edges files from extracted LDBC SF1 CSVs")
    parser.add_argument("--input-dir", type=Path, default=config.LDBC_EXTRACT_DIR)
    parser.add_argument("--nodes-output", type=Path, default=config.NORMALIZED_NODES_PATH)
    parser.add_argument("--edges-output", type=Path, default=config.NORMALIZED_EDGES_PATH)
    parser.add_argument("--verbose", action="store_true", help="Print per-file normalization stats")
    return parser.parse_args()


def make_typed_id(node_type: str, raw_id: str | int) -> str:
    return f"{node_type}:{raw_id}"


def serialize_attrs(row: dict) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True)


def _rename_duplicate_headers(headers: list[str]) -> list[str]:
    counts: Counter[str] = Counter()
    renamed: list[str] = []
    for header in headers:
        counts[header] += 1
        if counts[header] == 1 and headers.count(header) == 1:
            renamed.append(header)
        else:
            renamed.append(f"{header}__{counts[header] - 1}")
    return renamed


def iter_csv_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter="|")
        raw_headers = next(reader)
        headers = _rename_duplicate_headers(raw_headers)
        for row in reader:
            if not row:
                continue
            if len(row) < len(headers):
                row = row + [""] * (len(headers) - len(row))
            elif len(row) > len(headers):
                row = row[: len(headers)]
            yield dict(zip(headers, row, strict=False))


def normalize_node_row(node_type: str, id_column: str, row: dict[str, str]) -> NodeRecord:
    raw_id = row[id_column]
    attrs = {key: value for key, value in row.items() if key != id_column}
    return NodeRecord(
        node_id=make_typed_id(node_type, raw_id),
        node_type=node_type,
        raw_id=str(raw_id),
        attrs_json=serialize_attrs(attrs),
    )


def collect_node_records(input_dir: Path) -> Iterable[NodeRecord]:
    for relative_path, spec in NODE_FILE_REGISTRY.items():
        path = input_dir / relative_path
        if not path.exists():
            continue
        for row in iter_csv_rows(path):
            yield normalize_node_row(spec["node_type"], spec["id_column"], row)


def normalize_edge_row(spec: dict[str, str], row: dict[str, str], index: int) -> EdgeRecord:
    src_raw = row[spec["src_column"]]
    dst_raw = row[spec["dst_column"]]
    src_id = make_typed_id(spec["src_type"], src_raw)
    dst_id = make_typed_id(spec["dst_type"], dst_raw)
    attrs = {
        key: value
        for key, value in row.items()
        if key not in {spec["src_column"], spec["dst_column"]}
    }
    edge_id = f"{src_id}-{spec['edge_type']}->{dst_id}#{index}"
    return EdgeRecord(
        edge_id=edge_id,
        src_id=src_id,
        dst_id=dst_id,
        edge_type=spec["edge_type"],
        src_type=spec["src_type"],
        dst_type=spec["dst_type"],
        attrs_json=serialize_attrs(attrs),
    )


def collect_edge_records(input_dir: Path) -> Iterable[EdgeRecord]:
    for relative_path, spec in EDGE_FILE_REGISTRY.items():
        path = input_dir / relative_path
        if not path.exists():
            continue
        for index, row in enumerate(iter_csv_rows(path)):
            yield normalize_edge_row(spec, row, index)


def records_to_dataframe(records: Iterable[NodeRecord | EdgeRecord]) -> pd.DataFrame:
    return pd.DataFrame([asdict(record) for record in records])


def write_parquet_records(records: Iterable[NodeRecord | EdgeRecord], output_path: Path, batch_size: int = 100_000) -> int:
    writer = None
    batch: list[dict] = []
    total_rows = 0

    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        for record in records:
            batch.append(asdict(record))
            if len(batch) >= batch_size:
                table = pa.Table.from_pylist(batch)
                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema)
                writer.write_table(table)
                total_rows += len(batch)
                batch = []

        if batch:
            table = pa.Table.from_pylist(batch)
            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)
            writer.write_table(table)
            total_rows += len(batch)
    finally:
        if writer is not None:
            writer.close()

    return total_rows


def main() -> None:
    args = parse_args()
    config.ensure_directories()

    node_records = list(collect_node_records(args.input_dir)) if args.verbose else collect_node_records(args.input_dir)
    edge_records = list(collect_edge_records(args.input_dir)) if args.verbose else collect_edge_records(args.input_dir)

    if args.verbose:
        node_counter = Counter(record.node_type for record in node_records)
        edge_counter = Counter(record.edge_type for record in edge_records)
        print(f"Node counts by type: {dict(node_counter)}")
        print(f"Edge counts by type: {dict(edge_counter)}")

    if args.verbose:
        node_total = write_parquet_records(iter(node_records), args.nodes_output)
        edge_total = write_parquet_records(iter(edge_records), args.edges_output)
    else:
        node_total = write_parquet_records(node_records, args.nodes_output)
        edge_total = write_parquet_records(edge_records, args.edges_output)

    print(f"Wrote {node_total} normalized nodes to {args.nodes_output}")
    print(f"Wrote {edge_total} normalized edges to {args.edges_output}")


if __name__ == "__main__":
    main()
