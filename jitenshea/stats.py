
"""Statistical methods used for analyzing the shared bike data
"""

import logging
import daiquiri

import numpy as np
import pandas as pd

from sklearn.cluster import KMeans

import xgboost as xgb

import seaborn as sns
from matplotlib import pyplot as plt


daiquiri.setup(logging.INFO)
logger = daiquiri.getLogger("stats")


def preprocess_data_for_clustering(df):
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
    df_norm = preprocess_data_for_clustering(df)
    model = KMeans(n_clusters=4, random_state=0)
    kmeans = model.fit(df_norm.T)
    df_labels = pd.DataFrame({"id_station": df_norm.columns, "labels": kmeans.labels_})
    df_centroids = pd.DataFrame(kmeans.cluster_centers_).reset_index()
    return {"labels": df_labels, "centroids": df_centroids}


def find_cluster(centroid):
    """Identify the different clusters with their trends.

    Parameters
    ----------
    centroid: pd.DataFrame
        Hourly profile for each cluster

    Returns
    -------
    dict
        name: cluster_id
    """
    # There must be just 4 clusters
    assert centroid.shape[0] == 4
    hours = ['h{:02d}'.format(i) for i in range(24)]
    centroid = centroid[hours].T * 100.
    # the cluster which has an almost-always high availability
    high_avail = centroid.std().idxmin()
    centroid = centroid.drop(columns=[high_avail])
    # the cluster where the availability increases through the evening
    evening = (centroid.loc['h21'] - centroid.loc['h04']).idxmax()
    centroid = centroid.drop(columns=[evening])
    # the cluster where the availability is min at the morning (and max at noon)
    noon = centroid.loc['h06'].idxmin()
    centroid = centroid.drop(columns=[noon])
    # the last one, where the availability is max at the morning.
    morning = centroid.columns[0]
    return {high_avail: "high",
            evening: "evening",
            noon: "noon",
            morning: "morning"}


def plot_cluster_profile(city, centroid, palette='tab10'):
    """Plot the profiles for each cluster.

    Parameters
    ----------
    city : str
        city name
    centroid : pd.DataFrame
        cluster profiles
    palette : str
        color palette name
    """
    colors = sns.color_palette(palette, 4)
    hours = ['h{:02d}'.format(i) for i in range(24)]
    centroid = centroid.set_index('cluster_id')
    with sns.axes_style("whitegrid", {'xtick.major.size': 8.0}):
        fig, ax = plt.subplots(figsize=(10, 6))
    for (cluster_id, label), color in zip(find_cluster(centroid).items(), colors):
        plt.plot(range(24), 100*centroid[hours].T[cluster_id], color=color, label=label)
    plt.legend()
    plt.title("{} Cluster".format(city.capitalize()))
    plt.xlabel('Hour')
    plt.xticks(np.linspace(0, 24, 13))
    plt.yticks(np.linspace(0, 100, 11))
    plt.ylabel("available bikes%")
    sns.despine()


def compute_geo_clusters(df):
    """Compute stations clusters based on their geolocalization

    Parameters
    ----------
    df : pd.DataFrame

    Returns
    ------
    dict
        labels: id station and their cluster id
        centroids: cluster centroids
    """
    X = df[['lat', 'lon']].copy()
    k_means = KMeans(init='k-means++', n_clusters=12).fit(X)
    labels = pd.DataFrame({"station_id": df['id'],
                           'cluster_id': k_means.labels_})
    labels.sort_values(by='station_id', inplace=True)
    centroids = pd.DataFrame(k_means.cluster_centers_, columns=['lat', 'lon'])
    return {"labels": labels, "centroids": centroids}


def time_resampling(df, freq="10T"):
    """Normalize the timeseries by resampling its timestamps

    Parameters
    ----------
    df : pandas.DataFrame
        Input data, contains columns `ts`, `nb_bikes`, `nb_stands`, `station_id`
    freq : str
        Time resampling frequency

    Returns
    -------
    pandas.DataFrame
        Resampled data
    """
    logger.info("Time resampling for each station by '%s'", freq)
    df = (df.groupby("station_id")
          .resample(freq, on="ts")[["ts", "nb_bikes", "nb_stands", "probability"]]
          .mean()
          .bfill())
    return df.reset_index()


def complete_data(df):
    """Add some temporal columns to the dataset

    - day of the week
    - hour of the day
    - minute

    Parameters
    ----------
    df : pandas.DataFrame
        Input data ; must contain a `ts` column

    Returns
    -------
    pandas.DataFrame
        Data with additional columns `day`, `hour` and `minute`

    """
    logger.info("Complete some data")
    df = df.copy()
    df['day'] = df['ts'].apply(lambda x: x.weekday())
    df['hour'] = df['ts'].apply(lambda x: x.hour)
    df['minute'] = df['ts'].apply(lambda x: x.minute)
    return df


def add_future(df, frequency):
    """Add future bike availability to each observation by shifting input data
    accurate columns with respect to a given `frequency`

    Parameters
    ----------
    df : pd.DataFrame
        Input data
    frequency : DateOffset, timedelta or str
        Indicates the prediction frequency

    Returns
    -------
    pd.DataFrame
        Enriched data, with additional column "future"
    """
    logger.info("Compute the future bike availability (freq='%s')", frequency)
    df = df.set_index(["ts", "station_id"])
    label = df["probability"].copy()
    label.name = "future"
    label = (label.reset_index(level=1)
             .shift(-1, freq=frequency)
             .reset_index()
             .set_index(["ts", "station_id"]))
    logger.info("Merge future data with current observations")
    df = df.merge(label, left_index=True, right_index=True)
    df.reset_index(level=1, inplace=True)
    return df


def prepare_data_for_training(df, date, frequency='1H', start=None, periods=1):
    """Prepare data for training

    Parameters
    ----------
    df : pd.DataFrame
        Input data; must contains a "future" column and a `datetime` index
    date : date.datetime
        Date for the prediction
    frequency : str
        Delay between the training and validation set; corresponds to
    prediction frequency
    start : date.Timestamp
        Start of the history data (for training)
    periods : int
        Number of predictions

    Returns
    -------
    tuple of 4 pandas.DataFrames
        two for training, two for testing (train_X, train_Y, test_X, test_Y)
    """
    logger.info("Split train and test according to a validation date")
    cut = date - pd.Timedelta(frequency.replace('T', 'm'))
    stop = date + periods * pd.Timedelta(frequency.replace('T', 'm'))
    if start is not None:
        df = df[df.index >= start]
    logger.info("Data shape after start cut: %s", df.shape)
    train = df[df.index <= cut].copy()
    logger.info("Data shape after prediction date cut: %s", df.shape)
    train_X = train.drop(["probability", "future"], axis=1)
    train_Y = train['future'].copy()
    # time window
    mask = np.logical_and(df.index >= date, df.index <= stop)
    test = df[mask].copy()
    test_X = test.drop(["probability", "future"], axis=1)
    test_Y = test['future'].copy()
    return train_X, train_Y, test_X, test_Y


def fit(train_X, train_Y, test_X, test_Y):
    """Train the xgboost model

    Parameters
    ----------
    train_X : pandas.DataFrame
    test_X : pandas.DataFrame
    train_Y : pandas.DataFrame
    test_Y : pandas.DataFrame

    Returns
    -------
    XGBoost.model
        Booster trained model
    """
    logger.info("Fit training data with the model...")
    # param = {'objective': 'reg:linear'}
    param = {'objective': 'reg:logistic'}
    param['eta'] = 0.2
    param['max_depth'] = 6
    param['silent'] = 1
    param['nthread'] = 4
    training_progress = dict()
    xg_train = xgb.DMatrix(train_X, label=train_Y)
    xg_test = xgb.DMatrix(test_X, label=test_Y)
    watchlist = [(xg_train, 'train'), (xg_test, 'test')]
    num_round = 25
    bst = xgb.train(params=param,
                    dtrain=xg_train,
                    num_boost_round=num_round,
                    evals=watchlist,
                    evals_result=training_progress)
    return bst, training_progress


def train_prediction_model(df, validation_date, frequency):
    """Train a XGBoost model on `df` data with a train/validation split given
    by `predict_date` starting from temporal information (time of the day, day
    of the week) and previous bike availability

    Parameters
    ----------
    df : pandas.DataFrame
        Input data, contains columns `ts`, `nb_bikes`, `nb_stands`,
    `station_id`
    validation_date : datetime.date
        Reference date to split the input data between training and validation
    sets
    frequency : DateOffset, timedelta or str
        Indicates the prediction frequency

    Returns
    -------
    XGBoost.model
        Trained XGBoost model

    """
    df = time_resampling(df)
    df = complete_data(df)
    df = add_future(df, frequency)
    train_test_split = prepare_data_for_training(df,
                                                 validation_date,
                                                 frequency=frequency,
                                                 start=df.index.min(),
                                                 periods=2)
    train_X, train_Y, test_X, test_Y = train_test_split
    trained_model = fit(train_X, train_Y, test_X, test_Y)
    return trained_model[0]
