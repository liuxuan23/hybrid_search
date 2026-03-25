#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/home/liuxuan/workplace/hybrid_search"
INPUT_PATH="/data/dataset/graph_data/cluster/synthetic_community_100000.tsv"
DB_PATH="$PROJECT_ROOT/experiments/lancedb_graph/storage/lance"

NUM_NODES="${NUM_NODES:-100000}"
NUM_EDGES="${NUM_EDGES:-1000000}"
NUM_RELATIONS="${NUM_RELATIONS:-1000}"
NUM_NODE_TYPES="${NUM_NODE_TYPES:-1000}"
NUM_COMMUNITIES="${NUM_COMMUNITIES:-1000}"
SAMPLE_SIZE="${SAMPLE_SIZE:-100}"
REPEAT="${REPEAT:-3}"
K_HOP="${K_HOP:-3}"

cd "$PROJECT_ROOT"

echo "[1/3] 生成 synthetic community 图"
uv run python -m experiments.lancedb_graph.data_prep.generate_synthetic_graph \
  --graph-mode=community \
  --num-nodes="$NUM_NODES" \
  --num-edges="$NUM_EDGES" \
  --num-relations="$NUM_RELATIONS" \
  --num-node-types="$NUM_NODE_TYPES" \
  --num-communities="$NUM_COMMUNITIES" \
  --output-path="$INPUT_PATH"

echo "[2/3] 运行 baseline vs adjacency benchmark"
uv run python -m experiments.lancedb_graph.benchmarks.benchmark_adjacency_vs_baseline \
  --input-path "$INPUT_PATH" \
  --db-path "$DB_PATH" \
  --sample-size "$SAMPLE_SIZE" \
  --repeat "$REPEAT" \
  --k-hop "$K_HOP" \
  --cluster-strategy none

echo "[3/3] 运行 clustered vs unclustered locality benchmark"
uv run python -m experiments.lancedb_graph.benchmarks.benchmark_cluster_locality \
  --input-path "$INPUT_PATH" \
  --db-path "$DB_PATH" \
  --sample-size "$SAMPLE_SIZE" \
  --repeat "$REPEAT" \
  --k-hop "$K_HOP" \
  --clustered-strategy community

echo "完成：数据生成 + benchmark 已全部执行。"
