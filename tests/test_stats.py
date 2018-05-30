import os.path as osp
from pathlib import Path

import pandas as pd

from jitenshea.stats import find_cluster


_here = Path(osp.dirname(osp.abspath(__file__)))
DATADIR = _here / 'data'
CENTROIDS_CSV = DATADIR / 'centroids.csv'


def test_find_cluster():
    df = pd.read_csv(CENTROIDS_CSV)
    df = df.set_index('cluster_id')
    cluster = find_cluster(df)
    expected = {3: 'evening', 1: 'high', 0: 'morning', 2: 'noon'}
    assert expected == cluster
