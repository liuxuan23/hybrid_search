#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/home/liuxuan/workplace/hybrid_search"
PYTHON_BIN="/home/liuxuan/workplace/.venv/bin/python"
DEFAULT_TSV_PATH="/data/dataset/graph_data/cluster/synthetic_community_100000.tsv"
DEFAULT_ARANGODB_URL="http://127.0.0.1:8529"
DEFAULT_ARANGODB_DB="graph_bench"
DEFAULT_ARANGODB_USERNAME="root"
DEFAULT_ARANGODB_PASSWORD="123456"

cd "$ROOT_DIR"

TSV_PATH="${1:-$DEFAULT_TSV_PATH}"
ARANGODB_URL="${2:-$DEFAULT_ARANGODB_URL}"
ARANGODB_DB="${3:-$DEFAULT_ARANGODB_DB}"
ARANGODB_USERNAME="${4:-$DEFAULT_ARANGODB_USERNAME}"
ARANGODB_PASSWORD="${5:-$DEFAULT_ARANGODB_PASSWORD}"

echo "Running full ArangoDB benchmark pipeline ..."
echo "  tsv_path           = $TSV_PATH"
echo "  arangodb_url       = $ARANGODB_URL"
echo "  arangodb_db        = $ARANGODB_DB"
echo "  arangodb_username  = $ARANGODB_USERNAME"

echo "[1/2] Importing TSV graph data into ArangoDB ..."
PYTHONPATH="$ROOT_DIR" \
ARANGODB_URL="$ARANGODB_URL" \
ARANGODB_DB="$ARANGODB_DB" \
ARANGODB_USERNAME="$ARANGODB_USERNAME" \
ARANGODB_PASSWORD="$ARANGODB_PASSWORD" \
"$PYTHON_BIN" -m experiments.cross_db_graph.scripts.import_arangodb "$TSV_PATH"

echo "[2/2] Running ArangoDB benchmark ..."
PYTHONPATH="$ROOT_DIR" \
ARANGODB_URL="$ARANGODB_URL" \
ARANGODB_DB="$ARANGODB_DB" \
ARANGODB_USERNAME="$ARANGODB_USERNAME" \
ARANGODB_PASSWORD="$ARANGODB_PASSWORD" \
"$PYTHON_BIN" -m experiments.cross_db_graph.runner --engine arangodb

echo "Done. Check results under experiments/cross_db_graph/results/"
