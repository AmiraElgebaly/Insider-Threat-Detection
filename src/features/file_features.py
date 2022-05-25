import os
from src.features.general_features import *
from src.utils_path.path_utils import  get_data_path , get_models_path

def process_file(file_df):
    file_df['file_extension'] = file_df['filename'].apply(lambda x: x.split('.')[-1])
    file_df['file_length'] = file_df['content'].apply(lambda x: len(x.split()))
    file_df['file_depth'] = file_df['filename'].apply(lambda x: len(x.split('\\')))
    return file_df


def file_statistical_features(file_df, return_stats=True):
    all_logon_df = pd.read_csv(os.path.join(get_data_path(),'raw/logon.csv'))
    all_logon_df['date'] = pd.to_datetime(all_logon_df['date'])
    user_pcs_df = map_user_pcs(all_logon_df)

    file_df['date'] = pd.to_datetime(file_df['date'])

    tmp_file_df = process_file(file_df)
    tmp_file_df['weekday'] = tmp_file_df.date.dt.weekday
    tmp_file_df['day'] = pd.to_datetime(tmp_file_df['date']).dt.normalize()
    tmp_file_df['hour'] = tmp_file_df['date'].dt.hour

    tmp_file_df['is_weekend'] = tmp_file_df['weekday'].apply(is_weekend)
    tmp_file_df['is_working_hour'] = tmp_file_df['hour'].apply(is_working_hour)
    tmp_file_df['is_personal_pc'] = tmp_file_df.apply(is_personal_pc, args=[user_pcs_df], axis=1)

    user_daily_file_stats = tmp_file_df.groupby(["day", "user"])['file_length', 'file_depth'].mean().reset_index()

    if return_stats:
        return user_daily_file_stats
    else:
        return tmp_file_df


def file_count_features(file_df):
    tmp_file_df = file_statistical_features(file_df, False)
    groupby_cols = ['day', 'user', 'activity', 'file_extension',
                'from_removable_media', 'is_working_hour', 'is_personal_pc']
    user_daily_counts = pd.DataFrame({"count" : tmp_file_df.groupby(groupby_cols).size()}).reset_index()
    user_daily_counts = user_daily_counts.set_index(groupby_cols).unstack(['activity', 'file_extension',
                                     'from_removable_media', 'is_working_hour', 'is_personal_pc'])
    user_daily_counts.columns = ['{}_{}_{}_{}_{}_{}'.format(a,b,c,d,e,f) for a,b,c,d,e,f in user_daily_counts.columns]

    user_daily_counts.fillna(0, inplace=True)
    user_daily_counts = user_daily_counts.reset_index()

    return user_daily_counts


def file_merged_features(file_df):
    user_daily_file_stats = file_statistical_features(file_df)
    user_daily_counts = file_count_features(file_df)
    user_daily_file = user_daily_counts.merge(user_daily_file_stats, on=['day', 'user'])

    return user_daily_file
