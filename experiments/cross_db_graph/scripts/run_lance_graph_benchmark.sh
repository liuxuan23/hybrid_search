#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="/home/lx/workplace/hybrid_search"
PYTHON_BIN="${PYTHON_BIN:-uv run python}"
DEFAULT_TSV_PATH="/data/dataset/graph_data/cluster/synthetic_community_100000.tsv"
DEFAULT_DB_PATH="/home/lx/workplace/hybrid_search/storage/lance_graph/cross_db_graph_benchmark"
DEFAULT_MODE="warm"

cd "$ROOT_DIR"

TSV_PATH="${1:-$DEFAULT_TSV_PATH}"
DB_PATH="${2:-$DEFAULT_DB_PATH}"
MODE="${3:-$DEFAULT_MODE}"

run_python() {
    if [[ "$PYTHON_BIN" == "uv run python" ]]; then
        uv run python "$@"
    else
        "$PYTHON_BIN" "$@"
    fi
}

echo "Running full official lance_graph benchmark pipeline ..."
echo "  tsv_path   = $TSV_PATH"
echo "  db_path    = $DB_PATH"
echo "  mode       = $MODE"

echo "[1/3] Building official lance_graph storage from TSV ..."
PYTHONPATH="$ROOT_DIR" run_python -m experiments.cross_db_graph.scripts.build_lance_graph_from_tsv "$TSV_PATH" --output-dir "$DB_PATH"

echo "[2/3] Generating seeds.json from TSV ..."
PYTHONPATH="$ROOT_DIR" run_python - <<PY
from pathlib import Path
from experiments.cross_db_graph.scripts.export_graph_data import generate_seeds_from_tsv
payload = generate_seeds_from_tsv(Path(r"$TSV_PATH"))
print("Generated seeds counts:", {key: len(value) for key, value in payload.items()})
PY

echo "[3/3] Running official lance_graph benchmark ..."
PYTHONPATH="$ROOT_DIR" LANCE_GRAPH_DB_PATH="$DB_PATH" run_python -m experiments.cross_db_graph.runner --engine lance_graph --mode "$MODE" | cat

echo "Done. Check results under experiments/cross_db_graph/results/"
