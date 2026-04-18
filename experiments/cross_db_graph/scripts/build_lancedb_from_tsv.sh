#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "$SCRIPT_DIR/../../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-uv run python}"

TSV_PATH="${1:-$ROOT_DIR/data/cluster/synthetic_community_1000000.tsv}"
DB_PATH="${2:-$ROOT_DIR/storage/lancedb_graph/cross_db_graph_benchmark}"
CLUSTER_STRATEGY="${3:-community}"

run_python() {
	if [[ "$PYTHON_BIN" == "uv run python" ]]; then
		uv run python "$@"
	else
		"$PYTHON_BIN" "$@"
	fi
}

echo "Building LanceDB graph storage from TSV ..."
echo "  tsv_path          = $TSV_PATH"
echo "  db_path           = $DB_PATH"
echo "  cluster_strategy  = $CLUSTER_STRATEGY"

if [[ ! -f "$TSV_PATH" ]]; then
	echo "TSV file not found: $TSV_PATH" >&2
	echo "Tip: pass an explicit TSV path, e.g. $ROOT_DIR/data/cluster/synthetic_community_1000000.tsv" >&2
	exit 1
fi

cd "$ROOT_DIR"
PYTHONPATH="$ROOT_DIR" run_python - <<PY
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency

tsv_path = r"$TSV_PATH"
db_path = r"$DB_PATH"
cluster_strategy = r"$CLUSTER_STRATEGY"

graph = LanceDBGraphAdjacency(db_path=db_path)
graph.build_from_tsv(tsv_path=tsv_path, cluster_strategy=cluster_strategy)
print(f"Build completed: {db_path}")
PY

echo "Done. You may update experiments/cross_db_graph/config.py to point LANCEDB_DB_PATH to this directory."