from typing import Dict, List

import pandas as pd


def build_degree_bucket_samples(nodes_df: pd.DataFrame, sample_size: int) -> Dict[str, List[str]]:
    """构建低/中/高度节点样本。"""
    raise NotImplementedError()
