import re
import os
import numpy as np
from src.features.general_features import *
from src.utils_path.path_utils import  get_data_path , get_models_path

def get_no_attachments_type(attachment):
    if (attachment != attachment): return 0

    if len(attachment) == 0:
        return 0

    types = [re.findall('\.[a-zA-Z0-9]*\(',i)[0][1:-1] for i in attachment.split(';')]
    unique_types = np.unique(np.array(types))
    return len(unique_types)


def get_attachments_size(attachment):
    if (attachment != attachment): return 0

    if len(attachment) == 0:
        return 0

    sizes = [int(re.findall('\([0-9]+\)',i)[0][1:-1]) for i in attachment.split(';')]
    #sizes = np.array(sizes)/1024/1024
    sum_sizes = np.sum(np.array(sizes))
    return sum_sizes


def get_no_external_dest(to):
    dests_count = len(to.split(';'))
    internal_count = len(re.findall('@dtaa.com',to))
    return dests_count - internal_count


def is_external_mail(to):
    if to != to: return False

    if len(to) == 0:
        return False

    dests_count = len(to.split(';'))
    internal_count = len(re.findall('@dtaa.com',to))
    return (dests_count - internal_count) > 0


def process_email(e_df):
    df = e_df.copy()
    # statistical features
    df['no_destinations'] = df['to'].apply(lambda x : len(x.split(';')))
    df['no_attachments'] = df['attachments'].apply(lambda x : 0 if len(x) == 0 else len(x.split(';')))
    df['no_attachments_type'] = df['attachments'].apply(get_no_attachments_type)
    df['attachments_size'] = df['attachments'].apply(get_attachments_size)
    df['no_external_dest'] = df['to'].apply(get_no_external_dest)
    df['no_bcc_dest'] = df['bcc'].apply(lambda x : 0 if len(x) == 0 else len(x.split(';')))
    df['no_words'] = df['content'].apply(lambda x : 0 if x != x else len(x.split(' ')))
    # df['size'] = df['size'].apply(lambda x : x/1024/1024)

    # count features
    df['is_external'] = df['to'].apply(lambda x :1 if is_external_mail(x) else 0)
    df['is_external_bcc'] = df['bcc'].apply(lambda x :1 if is_external_mail(x) else 0)

    return df


def email_statistical_features(email_df, return_stats=True):
    all_logon_df = pd.read_csv(os.path.join(get_data_path(),'raw/logon.csv'))
    all_logon_df['date'] = pd.to_datetime(all_logon_df['date'])
    user_pcs_df = map_user_pcs(all_logon_df)

    email_df['date'] = pd.to_datetime(email_df['date'])

    tmp_email_df = process_email(email_df)
    tmp_email_df['weekday'] = tmp_email_df.date.dt.weekday
    tmp_email_df['hour'] = tmp_email_df['date'].dt.hour
    tmp_email_df['day'] = pd.to_datetime(tmp_email_df['date']).dt.normalize()

    tmp_email_df['is_weekend'] = tmp_email_df['weekday'].apply(is_weekend)
    tmp_email_df['is_working_hour'] = tmp_email_df['hour'].apply(is_working_hour)
    tmp_email_df['is_personal_pc'] = tmp_email_df.apply(is_personal_pc, args=[user_pcs_df], axis=1)

    user_daily_email_stats = tmp_email_df.groupby(["day", "user"])[
        'no_destinations', 'no_attachments', 'no_attachments_type',
        'attachments_size', 'no_external_dest', 'no_bcc_dest', 'no_words', 'size'].mean().reset_index()

    if return_stats:
        return user_daily_email_stats
    else:
        return tmp_email_df


def email_count_features(email_df):
    tmp_email_df = email_statistical_features(email_df, False)
    groupby_cols = ['day', 'user', 'activity', 'is_working_hour', 'is_personal_pc' ,'is_external' , 'is_external_bcc']
    user_daily_email_counts = pd.DataFrame({"count" : tmp_email_df.groupby(groupby_cols).size()}).reset_index()
    user_daily_email_counts = user_daily_email_counts.set_index(groupby_cols).unstack(['activity', 'is_working_hour', 'is_personal_pc' , 'is_external' , 'is_external_bcc'])
    user_daily_email_counts.columns = ['{}_{}_{}_{}_{}_{}'.format(a,b,c,d,e,f) for a,b,c,d,e,f in user_daily_email_counts.columns]

    user_daily_email_counts.fillna(0, inplace=True)
    user_daily_email_counts = user_daily_email_counts.reset_index()

    return user_daily_email_counts


def email_merged_features(email_df):
    user_daily_email_stats = email_statistical_features(email_df)
    user_daily_email_counts = email_count_features(email_df)
    user_daily_email = user_daily_email_counts.merge(user_daily_email_stats, on=['day', 'user'])

    return user_daily_email
