#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-uv run python}"
DEFAULT_TSV_PATH="$ROOT_DIR/data/cluster/synthetic_community_1000000.tsv"
DEFAULT_POSTGRES_DSN="postgresql://postgres:postgres123@localhost:5432/graph_bench"

run_python() {
    if [[ "$PYTHON_BIN" == "uv run python" ]]; then
        uv run python "$@"
    else
        "$PYTHON_BIN" "$@"
    fi
}

cleanup() {
    echo "[cleanup] Dropping PostgreSQL benchmark tables ..."
    POSTGRES_DSN="$POSTGRES_DSN" run_python - <<'PY'
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

if [[ ! -f "$TSV_PATH" ]]; then
    echo "TSV file not found: $TSV_PATH" >&2
    echo "Tip: pass an explicit TSV path, e.g. $ROOT_DIR/data/cluster/synthetic_community_1000000.tsv" >&2
    exit 1
fi

trap cleanup EXIT

echo "Running full PostgreSQL benchmark pipeline ..."
echo "  tsv_path       = $TSV_PATH"
echo "  postgres_dsn   = $POSTGRES_DSN"

echo "[1/2] Importing TSV graph data into PostgreSQL ..."
PYTHONPATH="$ROOT_DIR" POSTGRES_DSN="$POSTGRES_DSN" run_python -m experiments.cross_db_graph.scripts.import_postgres "$TSV_PATH"

echo "[2/2] Running PostgreSQL benchmark ..."
PYTHONPATH="$ROOT_DIR" POSTGRES_DSN="$POSTGRES_DSN" run_python -m experiments.cross_db_graph.runner --engine postgres

echo "Done. Check results under experiments/cross_db_graph/results/"
