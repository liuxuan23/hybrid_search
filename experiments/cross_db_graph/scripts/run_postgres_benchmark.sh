#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/home/liuxuan/workplace/hybrid_search"
PYTHON_BIN="/home/liuxuan/workplace/.venv/bin/python"
DEFAULT_TSV_PATH="/data/dataset/graph_data/cluster/synthetic_community_100000.tsv"
DEFAULT_POSTGRES_DSN="postgresql://postgres:postgres123@localhost:5432/graph_bench"

cleanup() {
    echo "[cleanup] Dropping PostgreSQL benchmark tables ..."
    POSTGRES_DSN="$POSTGRES_DSN" "$PYTHON_BIN" - <<'PY'
import os

import psycopg

dsn = os.environ["POSTGRES_DSN"]
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS graph_edges")
        cur.execute("DROP TABLE IF EXISTS graph_nodes")
    conn.commit()
print("Dropped PostgreSQL tables: graph_edges, graph_nodes")
PY
}

cd "$ROOT_DIR"

TSV_PATH="${1:-$DEFAULT_TSV_PATH}"
POSTGRES_DSN="${2:-$DEFAULT_POSTGRES_DSN}"

trap cleanup EXIT

echo "Running full PostgreSQL benchmark pipeline ..."
echo "  tsv_path       = $TSV_PATH"
echo "  postgres_dsn   = $POSTGRES_DSN"

echo "[1/2] Importing TSV graph data into PostgreSQL ..."
PYTHONPATH="$ROOT_DIR" POSTGRES_DSN="$POSTGRES_DSN" "$PYTHON_BIN" -m experiments.cross_db_graph.scripts.import_postgres "$TSV_PATH"

echo "[2/2] Running PostgreSQL benchmark ..."
PYTHONPATH="$ROOT_DIR" POSTGRES_DSN="$POSTGRES_DSN" "$PYTHON_BIN" -m experiments.cross_db_graph.runner --engine postgres

echo "Done. Check results under experiments/cross_db_graph/results/"
