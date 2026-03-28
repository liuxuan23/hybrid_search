from dataclasses import asdict, dataclass
from typing import Optional


@dataclass
class BenchmarkResult:
    engine: str
    query_type: str
    seed: str = ""
    k: int = 0
    batch_size: int = 0
    time_ms: float = 0.0
    result_count: int = 0
    success: bool = True
    error_message: str = ""

    def to_dict(self):
        return asdict(self)
