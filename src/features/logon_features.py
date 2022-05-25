import os
import numpy as np
from src.features.general_features import *
from src.utils import *
from src.utils_path.path_utils import  get_data_path , get_models_path


def add_session_time(df):
    grouped_df = df.groupby('pc')
    df_list = []

    for key, _ in grouped_df:
        group = grouped_df.get_group(key)
        group = group.sort_values(['date'], ascending=[False])

        difference = group["date"].diff().apply(lambda x: x / np.timedelta64(1, 'm') * -1).fillna(0).astype('int64')
        group['session_time'] = pd.Series(difference)
        group = group[group['activity'] == 'Logon']

        df_list.append(group)

    df_time = pd.concat(df_list, ignore_index=True, axis=0)

    return df_time


def process_logon(df):
    df = df.copy()
    df_time = add_session_time(df)

    return df_time


def logon_statistical_features(logon_df, return_stats=True, data_dir= os.path.join(get_data_path(),'raw/logon.csv')):
    all_logon_df = pd.read_csv(data_dir)
    all_logon_df['date'] = pd.to_datetime(all_logon_df['date'])
    user_pcs_df = map_user_pcs(all_logon_df)

    logon_df['date'] = pd.to_datetime(logon_df['date'])

    tmp_logon_df = process_logon(logon_df)
    tmp_logon_df['weekday'] = tmp_logon_df.date.dt.weekday
    tmp_logon_df['hour'] = tmp_logon_df['date'].dt.hour
    tmp_logon_df['day'] = pd.to_datetime(tmp_logon_df['date']).dt.normalize()

    tmp_logon_df['is_weekend'] = tmp_logon_df['weekday'].apply(is_weekend)
    tmp_logon_df['is_working_hour'] = tmp_logon_df['hour'].apply(is_working_hour)
    tmp_logon_df['is_personal_pc'] = tmp_logon_df.apply(is_personal_pc, args=[user_pcs_df], axis=1)

    user_daily_logon_stats = tmp_logon_df.groupby(["day", "user"])['session_time'].mean().reset_index()

    if return_stats:
        return user_daily_logon_stats
    else:
        return tmp_logon_df


def logon_count_features(logon_df):
    tmp_logon_df = logon_statistical_features(logon_df, False)
    groupby_cols = ['day', 'user', 'activity', 'is_working_hour', 'is_personal_pc']
    user_daily_logon_counts = pd.DataFrame({"count": tmp_logon_df.groupby(groupby_cols).size()}).reset_index()
    user_daily_logon_counts = user_daily_logon_counts.set_index(groupby_cols).unstack(
        ['activity', 'is_working_hour', 'is_personal_pc'])
    user_daily_logon_counts.columns = ['{}_{}_{}_{}'.format(a, b, c, d) for a, b, c, d in
                                       user_daily_logon_counts.columns]

    user_daily_logon_counts.fillna(0, inplace=True)
    user_daily_logon_counts = user_daily_logon_counts.reset_index()

    return user_daily_logon_counts


def logon_merged_features(logon_df):
    user_daily_logon_stats = logon_statistical_features(logon_df)
    user_daily_logon_counts = logon_count_features(logon_df)

    user_daily_logon = user_daily_logon_counts.merge(user_daily_logon_stats, on=['day', 'user'])

    return user_daily_logon
