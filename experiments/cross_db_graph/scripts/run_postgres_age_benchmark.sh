#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-uv run python}"
DEFAULT_TSV_PATH="$ROOT_DIR/data/cluster/synthetic_community_1000000.tsv"
DEFAULT_POSTGRES_DSN="postgresql://postgres:postgres123@localhost:5432/graph_bench"
DEFAULT_GRAPH_NAME="graph_bench_age"

run_python() {
    if [[ "$PYTHON_BIN" == "uv run python" ]]; then
        uv run python "$@"
    else
        "$PYTHON_BIN" "$@"
    fi
}

cleanup() {
    echo "[cleanup] Dropping PostgreSQL AGE benchmark graph ..."
    POSTGRES_DSN="$POSTGRES_DSN" POSTGRES_AGE_GRAPH="$GRAPH_NAME" run_python - <<'PY'
import os
import psycopg

dsn = os.environ["POSTGRES_DSN"]
graph_name = os.environ["POSTGRES_AGE_GRAPH"]

with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS age")
        cur.execute("LOAD 'age'")
        cur.execute('SET search_path = ag_catalog, "$user", public')
        cur.execute("SELECT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s)", (graph_name,))
        if cur.fetchone()[0]:
            cur.execute("SELECT drop_graph(%s, true)", (graph_name,))
    conn.commit()
print(f"Dropped AGE graph if existed: {graph_name}")
PY
}

cd "$ROOT_DIR"

TSV_PATH="${1:-$DEFAULT_TSV_PATH}"
POSTGRES_DSN="${2:-$DEFAULT_POSTGRES_DSN}"
GRAPH_NAME="${3:-$DEFAULT_GRAPH_NAME}"

if [[ ! -f "$TSV_PATH" ]]; then
    echo "TSV file not found: $TSV_PATH" >&2
    echo "Tip: pass an explicit TSV path, e.g. $ROOT_DIR/data/cluster/synthetic_community_1000000.tsv" >&2
    exit 1
fi

trap cleanup EXIT

echo "Running full PostgreSQL AGE benchmark pipeline ..."
echo "  tsv_path       = $TSV_PATH"
echo "  postgres_dsn   = $POSTGRES_DSN"
echo "  graph_name     = $GRAPH_NAME"

echo "[1/2] Importing TSV graph data into PostgreSQL AGE ..."
PYTHONPATH="$ROOT_DIR" POSTGRES_DSN="$POSTGRES_DSN" POSTGRES_AGE_GRAPH="$GRAPH_NAME" run_python -m experiments.cross_db_graph.scripts.import_postgres_age "$TSV_PATH"

echo "[2/2] Running PostgreSQL AGE benchmark ..."
PYTHONPATH="$ROOT_DIR" POSTGRES_DSN="$POSTGRES_DSN" POSTGRES_AGE_GRAPH="$GRAPH_NAME" run_python -m experiments.cross_db_graph.runner --engine postgres_age

echo "Done. Check results under experiments/cross_db_graph/results/"
