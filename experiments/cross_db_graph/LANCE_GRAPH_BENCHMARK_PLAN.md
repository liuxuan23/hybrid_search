# Lance Graph vs Adjacency Large-Graph Benchmark Plan

## 1. Background

The current `lance-graph` benchmark mainly measures Cypher execution over in-memory `RecordBatch` inputs and simple synthetic graph structure. That is useful for validating parser / planner / DataFusion execution speed, but it does not fully represent the query path used by very large graphs.

In contrast, the adjacency-based implementation under `experiments/lancedb_graph/` is designed around local adjacency access:

- seed node lookup
- `node_id -> physical_row_id` resolution
- row-local `take()` / `_rowid` reads
- BFS-like frontier expansion for multi-hop queries

To compare the two systems fairly, we need a benchmark that is closer to real large-graph access patterns.

---

## 2. Benchmark Goal

The benchmark should compare:

- `lance-graph`: Cypher -> logical plan -> DataFusion scan/join execution
- adjacency: seed-driven local adjacency reads and frontier expansion

The key question is not just whether a query is logically expressible, but how the two systems behave for realistic graph access patterns on large graphs.

---

## 3. Why the Existing Benchmark Is Not Enough

Current `lance-graph` benchmark characteristics:

- uses synthetic ring-like graph structure
- loads query inputs as in-memory `RecordBatch`
- emphasizes end-to-end query execution on in-memory tables
- does not reflect adjacency-local access patterns

This means it is closer to a columnar relational execution benchmark than a realistic large-graph traversal benchmark.

---

## 4. Benchmark Principles

A more realistic benchmark must:

1. start from explicit seed nodes
2. emphasize local expansion instead of whole-graph semantics only
3. measure both warm and cold-ish execution behavior
4. distinguish ID-only and materialized result modes
5. use the same graph data and same query semantics for both engines

---

## 5. Workload Design

### 5.1 Single-Seed Neighbor Query

Goal:
- query 1-hop neighbors from one seed node

Why:
- this is the most common local graph query shape
- should highlight adjacency-local lookup advantage

Variants:
- low-degree seed
- medium-degree seed
- high-degree seed
- out / in / both direction
- ID-only / materialized

---

### 5.2 Batch Neighbor Query

Goal:
- query neighbors for a seed batch in one request

Why:
- real systems often expand multiple seeds or frontier nodes at once

Variants:
- batch size 16 / 64 / 256 / 1024
- mixed degree seeds
- out / in / both
- ID-only / materialized

---

### 5.3 Seeded K-Hop Query

Goal:
- expand from one seed for fixed `k`

Why:
- most meaningful comparison point for traversal cost
- expected to enlarge the difference between scan/join and local adjacency access

Variants:
- `k = 2`
- `k = 3`
- optionally `k = 4`
- low / medium / high degree seed
- ID-only / materialized

---

### 5.4 Frontier Expansion Step Benchmark

Goal:
- benchmark one frontier expansion step rather than only full end-to-end k-hop query

Why:
- closer to actual execution behavior in traversal systems
- easier to explain scaling with frontier size

Variants:
- frontier sizes 64 / 256 / 1024
- out / both direction
- ID-only / materialized

---

## 6. Dataset Design

At least two graph families are recommended.

### 6.1 Uniform Graph

Use case:
- isolate pure scaling behavior
- reduce degree skew impact

Suggested scale:
- 1M nodes
- 10M edges

### 6.2 Power-Law / Skewed Graph

Use case:
- better approximate real knowledge graph / social graph behavior
- stress high-degree hotspots and frontier explosion

Suggested scale:
- 1M nodes
- 10M to 50M edges

---

## 7. Measurement Metrics

### 7.1 Core Metrics

- `latency_ms`
- `result_count`
- `query_type`
- `seed_type`
- `batch_size`
- `k`

### 7.2 IO Metrics

- process-level `read_bytes`
- process-level `write_bytes`

### 7.3 Memory Metrics

Recommended future extension:
- `rss_mb_before`
- `rss_mb_after`
- `rss_mb_peak`

### 7.4 Plan Diagnostics for Lance Graph

Recommended future extension:
- explain output
- logical plan summary
- physical plan summary
- join count / union count for variable-length queries

---

## 8. Warm vs Cold-ish Modes

### Warm Mode

Definition:
- repeated execution in the same process without resetting in-memory objects

Purpose:
- measure steady-state execution cost

### Cold-ish Mode

Definition:
- rerun workloads in a fresh process or fresh engine instance
- avoid accidental cache reuse where practical

Purpose:
- approximate first-query / low-cache behavior

Notes:
- full OS page cache drop is optional and may require elevated privileges
- process restart is an acceptable first-phase approximation

---

## 9. Fairness Rules

To compare results responsibly:

1. both engines must use the same graph data
2. both engines must implement the same query semantics
3. result counting must follow the same dedup / direction rules
4. benchmark modes must be explicit:
   - ID-only
   - materialized
5. `lance-graph` in later phases should be tested through a large-table/provider path, not only through in-memory `RecordBatch`

---

## 10. Phase-One Scope

Phase one focuses on quick integration without modifying upstream `lance-graph`.

### Phase-One Objective

Add `lance_graph` as a new engine under `experiments/cross_db_graph/` and run the existing benchmark workloads with equivalent Cypher queries.

### What Phase One Includes

- add `LanceGraphAdapter`
- support single neighbor query
- support batch neighbor query
- support fixed-k hop query
- reuse current seeds and benchmark runner
- use existing Python bindings only

### What Phase One Does Not Include

- modifying upstream `lance-graph`
- adding a new native large-table execution API
- adding full cold-cache harness
- adding per-query plan capture
- adding memory/RSS metrics

---

## 11. Phase-Two Direction

After phase one is working, phase two should improve fairness and realism.

Suggested next steps:

- expose a more realistic `lance-graph` execution path from Python
- prefer provider / context-backed execution over pre-materialized in-memory tables
- add cold-ish process-isolated execution mode
- add plan capture and richer IO / memory metrics
- add a more realistic skewed graph dataset

---

## 12. Phase-One Implementation Plan

### 12.1 New Adapter

Add:
- `experiments/cross_db_graph/adapters/lance_graph_adapter.py`

Responsibilities:
- load graph data from the benchmark storage
- build `GraphConfig`
- translate benchmark workloads to Cypher queries
- execute via Python `lance_graph` bindings
- normalize result shape to the benchmark schema

### 12.2 Runner Integration

Update:
- `experiments/cross_db_graph/runner.py`

Changes:
- add `lance_graph` engine option
- add adapter construction path

### 12.3 Initial Query Mapping

Single neighbor:
- `MATCH (a:Entity {entity_id: $seed})-[:RELATIONSHIP]->(b:Entity) RETURN b.entity_id`

Batch neighbor:
- `MATCH (a:Entity)-[:RELATIONSHIP]->(b:Entity) WHERE a.entity_id IN [...] RETURN a.entity_id, b.entity_id`

K-hop:
- `MATCH (a:Entity {entity_id: $seed})-[:RELATIONSHIP*2..2]->(b:Entity) RETURN b.entity_id`

---

## 13. Expected Outcome of Phase One

Phase one will not yet be a perfect large-graph fairness benchmark. However, it will:

- integrate `lance-graph` into the same cross-db benchmark harness
- establish query mapping parity
- generate initial timing comparisons against adjacency
- identify what upstream `lance-graph` capabilities are still missing for a stronger large-graph evaluation

---

## 14. Summary

This benchmark plan shifts the comparison from:

- “Can both systems answer the query?”

To:

- “How do the two execution paths behave when real large-graph traversal patterns are used?”

Phase one starts with practical integration inside `experiments/cross_db_graph/`. Later phases can improve realism by reducing in-memory bias on the `lance-graph` side and adding colder execution modes.

---

## 15. Immediate Next Step

Implement phase one directly inside:
- `experiments/cross_db_graph/`

without changing upstream `lance-graph` first.
