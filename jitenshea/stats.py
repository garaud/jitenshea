# coding: utf-8

"""Statistical methods used for analyzing the shared bike data
"""

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

from jitenshea import config

def compute_clusters(df):
    """Compute station clusters based on bike availability time series

    Parameters
    ----------
    df : pandas.DataFrame
        Input data, *i.e.* city-related timeseries, supposed to have
    `station_id`, `ts` and `nb_bikes` columns

    Returns
    -------
    dict
        Two pandas.DataFrame, the former for station clusters and the latter
    for cluster centroids
    
    """
    df_norm = preprocess_data(df)
    model = KMeans(n_clusters=4, random_state=0)
    kmeans = model.fit(df_norm.T)
    df_labels = pd.DataFrame({"id_station": df_norm.columns, "labels": kmeans.labels_})
    df_centroids = pd.DataFrame(kmeans.cluster_centers_).reset_index()
    return {"labels": df_labels, "centroids": df_centroids}

def preprocess_data(df):
    """Prepare data in order to apply a clustering algorithm

    Parameters
    ----------
    df : pandas.DataFrame
        Input data, *i.e.* city-related timeseries, supposed to have
    `station_id`, `ts` and `nb_bikes` columns

    Returns
    -------
    pandas.DataFrame
        Simpified version of `df`, ready to be used for clustering
    
    """
    # Filter unactive stations
    max_bikes = df.groupby("station_id")["nb_bikes"].max()
    unactive_stations = max_bikes[max_bikes==0].index.tolist()
    active_station_mask = np.logical_not(df['station_id'].isin(unactive_stations))
    df = df[active_station_mask]
    # Set timestamps as the DataFrame index and resample it with 5-minute periods
    df = (df.set_index("ts")
          .groupby("station_id")["nb_bikes"]
          .resample("5T")
          .mean()
          .bfill())
    df = df.unstack(0)
    # Drop week-end records
    df = df[df.index.weekday < 5]
    # Gather data regarding hour of the day
    df['hour'] = df.index.hour
    df = df.groupby("hour").mean()
    return df / df.max()
