from dataclasses import dataclass
from typing import List, Optional


@dataclass
class NeighborQuery:
    seed: str
    direction: str = "out"
    query_type: str = "neighbor"


@dataclass
class KHopQuery:
    seed: str
    k: int
    direction: str = "out"
    query_type: str = "k_hop"


@dataclass
class BatchNeighborQuery:
    seeds: List[str]
    direction: str = "out"
    query_type: str = "batch_neighbor"


def build_default_workloads(single_seeds: List[str], batch_seeds: Optional[List[str]] = None):
    workloads = []
    for seed in single_seeds:
        workloads.append(NeighborQuery(seed=seed))
        workloads.append(KHopQuery(seed=seed, k=2))
        workloads.append(KHopQuery(seed=seed, k=3))

    if batch_seeds:
        workloads.append(BatchNeighborQuery(seeds=batch_seeds))

    return workloads
