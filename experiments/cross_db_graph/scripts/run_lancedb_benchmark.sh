#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/home/liuxuan/workplace/hybrid_search"
PYTHON_BIN="/home/liuxuan/workplace/.venv/bin/python"
DEFAULT_TSV_PATH="/data/dataset/graph_data/cluster/synthetic_community_100000.tsv"
DEFAULT_DB_PATH="/home/liuxuan/workplace/hybrid_search/storage/lancedb_graph/cross_db_graph_benchmark"
DEFAULT_CLUSTER_STRATEGY="community"

cd "$ROOT_DIR"

TSV_PATH="${1:-$DEFAULT_TSV_PATH}"
DB_PATH="${2:-$DEFAULT_DB_PATH}"
CLUSTER_STRATEGY="${3:-$DEFAULT_CLUSTER_STRATEGY}"

echo "Running full LanceDB benchmark pipeline ..."
echo "  tsv_path          = $TSV_PATH"
echo "  db_path           = $DB_PATH"
echo "  cluster_strategy  = $CLUSTER_STRATEGY"

echo "[1/3] Building LanceDB graph storage from TSV ..."
PYTHONPATH="$ROOT_DIR" "$PYTHON_BIN" - <<PY
from experiments.lancedb_graph.storage_models.lancedb_graph_adjacency import LanceDBGraphAdjacency

tsv_path = r"$TSV_PATH"
db_path = r"$DB_PATH"
cluster_strategy = r"$CLUSTER_STRATEGY"

graph = LanceDBGraphAdjacency(db_path=db_path)
graph.build_from_tsv(tsv_path=tsv_path, cluster_strategy=cluster_strategy)
print(f"Build completed: {db_path}")
PY

echo "[2/3] Generating seeds.json from LanceDB ..."
PYTHONPATH="$ROOT_DIR" LANCEDB_DB_PATH="$DB_PATH" "$PYTHON_BIN" - <<PY
from experiments.cross_db_graph.scripts.export_graph_data import generate_seeds
payload = generate_seeds()
print("Generated seeds counts:", {key: len(value) for key, value in payload.items()})
PY

echo "[3/3] Running LanceDB benchmark ..."
PYTHONPATH="$ROOT_DIR" LANCEDB_DB_PATH="$DB_PATH" "$PYTHON_BIN" - <<PY
from experiments.cross_db_graph.adapters.lancedb_adapter import LanceDBGraphAdapter
from experiments.cross_db_graph.runner import execute_benchmark, load_seeds, write_results
from experiments.cross_db_graph.workloads import build_default_workloads

db_path = r"$DB_PATH"
single_seeds, batch_seeds = load_seeds()
workloads = build_default_workloads(single_seeds, batch_seeds)
print(f"Loaded {len(workloads)} workloads from seeds.json")

adapter = LanceDBGraphAdapter(db_path=db_path)
adapter.connect()
try:
	results = execute_benchmark(adapter, workloads)
	output_dir = write_results(results)
finally:
	adapter.close()

print(f"Completed LanceDB benchmark run. Results written to {output_dir}")
PY

echo "Done. Check results under experiments/cross_db_graph/results/"