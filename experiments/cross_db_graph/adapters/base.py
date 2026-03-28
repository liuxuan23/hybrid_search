from abc import ABC, abstractmethod


class GraphAdapter(ABC):
    engine_name = "base"

    @abstractmethod
    def connect(self):
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError

    @abstractmethod
    def query_neighbors(self, seed: str, direction: str = "out"):
        raise NotImplementedError

    @abstractmethod
    def query_k_hop(self, seed: str, k: int, direction: str = "out"):
        raise NotImplementedError

    @abstractmethod
    def query_batch_neighbors(self, seeds, direction: str = "out"):
        raise NotImplementedError
