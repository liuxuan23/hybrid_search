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
REPEAT="${REPEAT:-1}"
K_HOP="${K_HOP:-3}"
CACHE_MODE="${CACHE_MODE:-mixed}"
WARMUP_RUNS="${WARMUP_RUNS:-1}"
DROP_CACHE_COMMAND='sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null'
RUN_BASELINE_BENCHMARK="${RUN_BASELINE_BENCHMARK:-1}"
RUN_WARM_LOCALITY_BENCHMARK="${RUN_WARM_LOCALITY_BENCHMARK:-1}"
RUN_COLD_LOCALITY_BENCHMARK="${RUN_COLD_LOCALITY_BENCHMARK:-1}"

run_locality_benchmark() {
  local mode="$1"
  local title="$2"

  echo "$title"
  uv run python -m experiments.lancedb_graph.benchmarks.benchmark_cluster_locality \
    --input-path "$INPUT_PATH" \
    --db-path "$DB_PATH" \
    --sample-size "$SAMPLE_SIZE" \
    --repeat "$REPEAT" \
    --k-hop "$K_HOP" \
    --clustered-strategy community \
    --cache-mode "$mode" \
    --warmup-runs "$WARMUP_RUNS" \
    --drop-cache-command "$DROP_CACHE_COMMAND"
}

cd "$PROJECT_ROOT"

echo "[1/4] 生成 synthetic community 图"
uv run python -m experiments.lancedb_graph.data_prep.generate_synthetic_graph \
  --graph-mode=community \
  --num-nodes="$NUM_NODES" \
  --num-edges="$NUM_EDGES" \
  --num-relations="$NUM_RELATIONS" \
  --num-node-types="$NUM_NODE_TYPES" \
  --num-communities="$NUM_COMMUNITIES" \
  --output-path="$INPUT_PATH"

if [[ "$RUN_BASELINE_BENCHMARK" == "1" ]]; then
  echo "[2/4] 运行 baseline vs adjacency benchmark"
  uv run python -m experiments.lancedb_graph.benchmarks.benchmark_adjacency_vs_baseline \
    --input-path "$INPUT_PATH" \
    --db-path "$DB_PATH" \
    --sample-size "$SAMPLE_SIZE" \
    --repeat "$REPEAT" \
    --k-hop "$K_HOP" \
    --cluster-strategy none
else
  echo "[2/4] 跳过 baseline vs adjacency benchmark"
fi

if [[ "$RUN_WARM_LOCALITY_BENCHMARK" == "1" ]]; then
  run_locality_benchmark "warm" "[3/4] 运行 clustered vs unclustered locality benchmark（warm）"
else
  echo "[3/4] 跳过 clustered vs unclustered locality benchmark（warm）"
fi

if [[ "$RUN_COLD_LOCALITY_BENCHMARK" == "1" ]]; then
  run_locality_benchmark "cold" "[4/4] 运行 clustered vs unclustered locality benchmark（cold）"
else
  echo "[4/4] 跳过 clustered vs unclustered locality benchmark（cold）"
fi

echo "完成：数据生成与冷 / 热缓存 benchmark 已全部执行。"
