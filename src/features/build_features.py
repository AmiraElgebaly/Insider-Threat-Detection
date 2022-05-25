import pandas as pd
from functools import reduce
from src.features.logon_features import logon_merged_features
from src.features.device_features import device_merged_features
from src.features.file_features import file_merged_features
from src.features.http_features import http_merged_features
from src.features.email_features import email_merged_features


def build_features(logon_features, device_features, file_features, http_features, email_features):
    feature_dfs = [email_merged_features(email_features),
                   http_merged_features(http_features),
                   device_merged_features(device_features),
                   file_merged_features(file_features),
                   logon_merged_features(logon_features)]

    merged_features = reduce(lambda left, right: pd.merge(left, right, on=['user', 'day'],
                                                          how='outer'), feature_dfs).fillna(0)

    return merged_features
