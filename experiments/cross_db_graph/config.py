from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent
RESULTS_DIR = BASE_DIR / "results"
SEEDS_FILE = BASE_DIR / "seeds.json"

DATASET_NAME = "default"
NODES_FILE = BASE_DIR / "nodes.jsonl"
EDGES_FILE = BASE_DIR / "edges.jsonl"
LANCEDB_DB_PATH = Path(
	os.environ.get(
		"LANCEDB_DB_PATH",
		str(BASE_DIR.parent.parent / "storage" / "lancedb_graph" / "phase2_test_validation"),
	)
)

WARMUP_RUNS = 1
MEASURE_RUNS = 3
DEFAULT_BATCH_SIZE = 32
DEFAULT_DIRECTION = "out"
DEFAULT_MAX_RESULTS = 10000

POSTGRES_DSN = os.environ.get(
	"POSTGRES_DSN",
	"postgresql://postgres:postgres123@localhost:5432/graph_bench",
)
ARANGODB_URL = "http://127.0.0.1:8529"
ARANGODB_DB = "graph_bench"
ARANGODB_USERNAME = "root"
ARANGODB_PASSWORD = "openSesame"
