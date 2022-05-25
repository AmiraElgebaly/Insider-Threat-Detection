import numpy as np
from src.features.general_features import *
from src.utils_path.path_utils import  get_data_path , get_models_path
import os

def add_connection_time(dev_df):
    grouped_df = dev_df.groupby(['user', 'pc'])
    df_list = []

    for key, _ in grouped_df:
        group = grouped_df.get_group(key)
        group = group.sort_values(['date'], ascending=[False])

        difference = group["date"].diff().apply(lambda x: x / np.timedelta64(1, 'm') * -1).fillna(0).astype('int64')
        group['connection_time'] = pd.Series(difference)
        group = group[group['activity'] == 'Connect']

        df_list.append(group)

    device_df_time = pd.concat(df_list, ignore_index=True, axis=0)

    return device_df_time


def get_file_tree_length(path):
    if path != path: return 0
    return len(path.split(';'))


def process_device(dev_df):
    df = dev_df.copy()
    dev_df_time = add_connection_time(df)
    dev_df_time['file_depth'] = dev_df_time['file_tree'].apply(get_file_tree_length)

    return dev_df_time


def device_statistical_features(device_df, return_stats=True):
    all_logon_df = pd.read_csv( os.path.join(get_data_path(),'raw/logon.csv'))
    all_logon_df['date'] = pd.to_datetime(all_logon_df['date'])
    user_pcs_df = map_user_pcs(all_logon_df)

    device_df['date'] = pd.to_datetime(device_df['date'])

    tmp_device_df = process_device(device_df)
    tmp_device_df['weekday'] = tmp_device_df.date.dt.weekday
    tmp_device_df['hour'] = tmp_device_df['date'].dt.hour
    tmp_device_df['day'] = pd.to_datetime(tmp_device_df['date']).dt.normalize()

    tmp_device_df['is_weekend'] = tmp_device_df['weekday'].apply(is_weekend)
    tmp_device_df['is_working_hour'] = tmp_device_df['hour'].apply(is_working_hour)
    tmp_device_df['is_personal_pc'] = tmp_device_df.apply(is_personal_pc, args=[user_pcs_df], axis=1)

    user_daily_device_stats = tmp_device_df.groupby(["day", "user"])[
        'file_depth', 'connection_time'].mean().reset_index()

    if return_stats:
        return user_daily_device_stats
    else:
        return tmp_device_df


def device_count_features(device_df):
    tmp_device_df = device_statistical_features(device_df, False)

    groupby_cols = ['day', 'user', 'activity', 'is_working_hour', 'is_personal_pc']
    user_daily_device_counts = pd.DataFrame({"count": tmp_device_df.groupby(groupby_cols).size()}).reset_index()
    user_daily_device_counts = user_daily_device_counts.set_index(groupby_cols).unstack(
        ['activity', 'is_working_hour', 'is_personal_pc'])
    user_daily_device_counts.columns = ['{}_{}_{}_{}'.format(a, b, c, d) for a, b, c, d in
                                        user_daily_device_counts.columns]

    user_daily_device_counts.fillna(0, inplace=True)
    user_daily_device_counts = user_daily_device_counts.reset_index()

    return user_daily_device_counts


def device_merged_features(device_df):
    user_daily_device_stats = device_statistical_features(device_df)
    user_daily_device_counts = device_count_features(device_df)

    user_daily_device = user_daily_device_counts.merge(user_daily_device_stats, on=['day', 'user'])

    return user_daily_device
