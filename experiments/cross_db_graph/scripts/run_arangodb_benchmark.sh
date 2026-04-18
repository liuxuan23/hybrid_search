#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-uv run python}"
DEFAULT_TSV_PATH="$ROOT_DIR/data/cluster/synthetic_community_1000000.tsv"
DEFAULT_ARANGODB_URL="http://127.0.0.1:8529"
DEFAULT_ARANGODB_DB="graph_bench"
DEFAULT_ARANGODB_USERNAME="root"
DEFAULT_ARANGODB_PASSWORD=""

run_python() {
	if [[ "$PYTHON_BIN" == "uv run python" ]]; then
		uv run python "$@"
	else
		"$PYTHON_BIN" "$@"
	fi
}

cleanup() {
  echo "[cleanup] Dropping ArangoDB benchmark collections and graph ..."
  ARANGODB_URL="$ARANGODB_URL" \
  ARANGODB_DB="$ARANGODB_DB" \
  ARANGODB_USERNAME="$ARANGODB_USERNAME" \
  ARANGODB_PASSWORD="$ARANGODB_PASSWORD" \
  run_python - <<'PY'
import os

from arango import ArangoClient

db_name = os.environ["ARANGODB_DB"]
url = os.environ["ARANGODB_URL"]
username = os.environ["ARANGODB_USERNAME"]
password = os.environ["ARANGODB_PASSWORD"]

client = ArangoClient(hosts=url)
db = client.db(db_name, username=username, password=password)

if db.has_graph("graph_bench_graph"):
	db.delete_graph("graph_bench_graph", drop_collections=False, ignore_missing=True)

if db.has_collection("graph_edges"):
	db.delete_collection("graph_edges", ignore_missing=True)

if db.has_collection("graph_nodes"):
	db.delete_collection("graph_nodes", ignore_missing=True)

print("Dropped ArangoDB graph/collections: graph_bench_graph, graph_edges, graph_nodes")
PY
}

cd "$ROOT_DIR"

TSV_PATH="${1:-$DEFAULT_TSV_PATH}"
ARANGODB_URL="${2:-$DEFAULT_ARANGODB_URL}"
ARANGODB_DB="${3:-$DEFAULT_ARANGODB_DB}"
ARANGODB_USERNAME="${4:-$DEFAULT_ARANGODB_USERNAME}"
ARANGODB_PASSWORD="${5:-$DEFAULT_ARANGODB_PASSWORD}"

if [[ ! -f "$TSV_PATH" ]]; then
  echo "TSV file not found: $TSV_PATH" >&2
  echo "Tip: pass an explicit TSV path, e.g. $ROOT_DIR/data/cluster/synthetic_community_1000000.tsv" >&2
  exit 1
fi

trap cleanup EXIT

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
run_python -m experiments.cross_db_graph.scripts.import_arangodb "$TSV_PATH"

echo "[2/2] Running ArangoDB benchmark ..."
PYTHONPATH="$ROOT_DIR" \
ARANGODB_URL="$ARANGODB_URL" \
ARANGODB_DB="$ARANGODB_DB" \
ARANGODB_USERNAME="$ARANGODB_USERNAME" \
ARANGODB_PASSWORD="$ARANGODB_PASSWORD" \
run_python -m experiments.cross_db_graph.runner --engine arangodb

echo "Done. Check results under experiments/cross_db_graph/results/"
