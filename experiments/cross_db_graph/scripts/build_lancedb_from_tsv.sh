#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/home/liuxuan/workplace/hybrid_search"
PYTHON_BIN="/home/liuxuan/workplace/.venv/bin/python"

TSV_PATH="${1:-/data/dataset/graph_data/triples.tsv}"
DB_PATH="${2:-/home/liuxuan/workplace/hybrid_search/storage/lancedb_graph/cross_db_graph_benchmark}"
CLUSTER_STRATEGY="${3:-by_node_type}"

echo "Building LanceDB graph storage from TSV ..."
echo "  tsv_path          = $TSV_PATH"
echo "  db_path           = $DB_PATH"
echo "  cluster_strategy  = $CLUSTER_STRATEGY"

cd "$ROOT_DIR"
PYTHONPATH="$ROOT_DIR" "$PYTHON_BIN" - <<PY
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency

tsv_path = r"$TSV_PATH"
db_path = r"$DB_PATH"
cluster_strategy = r"$CLUSTER_STRATEGY"

graph = LanceDBGraphAdjacency(db_path=db_path)
graph.build_from_tsv(tsv_path=tsv_path, cluster_strategy=cluster_strategy)
print(f"Build completed: {db_path}")
PY

echo "Done. You may update experiments/cross_db_graph/config.py to point LANCEDB_DB_PATH to this directory."