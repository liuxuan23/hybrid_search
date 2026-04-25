from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
STORAGE_DIR = PROJECT_ROOT / "storage" / "ldbc_sf1"

LDBC_ARCHIVE_PATH = Path(
    os.environ.get(
        "LDBC_ARCHIVE_PATH",
        str(DATA_DIR / "social_network-sf1-CsvComposite-StringDateFormatter.tar.zst"),
    )
)
LDBC_EXTRACT_DIR = Path(
    os.environ.get(
        "LDBC_EXTRACT_DIR",
        str(DATA_DIR / "cluster" / "social_network-sf1-CsvComposite-StringDateFormatter"),
    )
)
LDBC_NORMALIZED_DIR = Path(
    os.environ.get(
        "LDBC_NORMALIZED_DIR",
        str(STORAGE_DIR / "normalized"),
    )
)
LDBC_LANCEDB_DIR = Path(
    os.environ.get(
        "LDBC_LANCEDB_DIR",
        str(PROJECT_ROOT / "storage" / "lancedb_graph" / "ldbc_sf1"),
    )
)
LDBC_SEEDS_PATH = Path(
    os.environ.get(
        "LDBC_SEEDS_PATH",
        str(BASE_DIR / "seeds.json"),
    )
)
RESULTS_DIR = Path(
    os.environ.get(
        "LDBC_RESULTS_DIR",
        str(BASE_DIR / "results"),
    )
)
SCHEMA_SUMMARY_PATH = Path(
    os.environ.get(
        "LDBC_SCHEMA_SUMMARY_PATH",
        str(LDBC_NORMALIZED_DIR / "schema_summary.json"),
    )
)
NORMALIZED_NODES_PATH = LDBC_NORMALIZED_DIR / "nodes.parquet"
NORMALIZED_EDGES_PATH = LDBC_NORMALIZED_DIR / "edges.parquet"

NODES_TABLE_NAME = "nodes"
EDGES_TABLE_NAME = "edges"
ADJ_INDEX_TABLE_NAME = "adj_index"

DEFAULT_RANDOM_SEED = 42
DEFAULT_BATCH_SIZE = 10_000
DEFAULT_SEED_SAMPLE_SIZE = 10
DEFAULT_BATCH_QUERY_SIZE = 32
DEFAULT_PERSON_NODE_TYPE = "Person"
DEFAULT_DIRECTION = "out"
DEFAULT_LOW_PERCENTILE = 0.50
DEFAULT_MID_PERCENTILE = 0.90


def ensure_directories() -> None:
    """Create the standard output directories used by the LDBC SF1 experiment."""
    for path in (LDBC_NORMALIZED_DIR, RESULTS_DIR, LDBC_LANCEDB_DIR.parent):
        path.mkdir(parents=True, exist_ok=True)


__all__ = [
    "ADJ_INDEX_TABLE_NAME",
    "BASE_DIR",
    "DATA_DIR",
    "DEFAULT_BATCH_QUERY_SIZE",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_DIRECTION",
    "DEFAULT_LOW_PERCENTILE",
    "DEFAULT_MID_PERCENTILE",
    "DEFAULT_PERSON_NODE_TYPE",
    "DEFAULT_RANDOM_SEED",
    "DEFAULT_SEED_SAMPLE_SIZE",
    "EDGES_TABLE_NAME",
    "LDBC_ARCHIVE_PATH",
    "LDBC_EXTRACT_DIR",
    "LDBC_LANCEDB_DIR",
    "LDBC_NORMALIZED_DIR",
    "LDBC_SEEDS_PATH",
    "NODES_TABLE_NAME",
    "NORMALIZED_EDGES_PATH",
    "NORMALIZED_NODES_PATH",
    "PROJECT_ROOT",
    "RESULTS_DIR",
    "SCHEMA_SUMMARY_PATH",
    "STORAGE_DIR",
    "ensure_directories",
]
