import os
import glob2
import pandas as pd
from datetime import timedelta
from src.utils_path.path_utils import  get_data_path , get_models_path

all_files = glob2.glob( os.path.join(get_data_path(),'raw/LDAP/*.csv') )
li = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)
LDAP_df = pd.concat(li, axis=0, ignore_index=True).drop_duplicates()


def is_weekend(x):
    if (x == 5) or (x == 6):
        return "weekend"
    else:
        return "weekday"


def is_working_hour(x):
    if (x >= 7 and x <= 18):
        return 'working_hour'
    else:
        return "after_hours"


def get_user_pc(x, unique_pcs):
    '''
    '''
    user = unique_pcs[unique_pcs['pc'] == x['pc']].user.values
    if (len(user)):
        return user[0]


def get_supervisor(user_id, ldap_file=LDAP_df):
    supervisor_name = ldap_file[ldap_file['user_id'] == user_id].supervisor.values[0]
    supervisor_id = ldap_file[ldap_file['employee_name'] == supervisor_name].user_id
    if (len(supervisor_id)):
        return supervisor_id.values[0]
    else:
        return None


def is_personal_pc(x, user_pcs_df):
    '''
    '''
    user_pc = user_pcs_df[user_pcs_df['user'] == x.user].pc.values[0]

    if (user_pc == x.pc):
        return "personal_pc"

    supervisor = get_supervisor(x.user)
    if (supervisor is not None):
        supervisor_pc = user_pcs_df[user_pcs_df['user'] == supervisor].pc.values[0]
        if (supervisor_pc == x.pc):
            return "supervisor_pc"
        else:
            return "other_pc"
    return "other_pc"


def map_user_pcs(df, time_period=15):
    '''
    '''
    user_pc_df = pd.DataFrame(columns=['pc', 'user'])
    start_date = df.date.min()
    end_date = start_date + timedelta(days=time_period)

    time_period_logons = df[(df['date'] > start_date) & (df['date'] < end_date)]

    user_pc_df['pc'] = time_period_logons['pc'].unique()
    user_pc_counts = time_period_logons.groupby(['user', 'pc']).size()
    user_pc_counts = user_pc_counts.to_frame(name='count').reset_index()
    idxs = user_pc_counts.groupby(['user'])['count'].transform(max) == user_pc_counts['count']
    unique_pcs = user_pc_counts[idxs]
    user_pc_df['user'] = user_pc_df.apply(get_user_pc, args=[unique_pcs], axis=1)

    return user_pc_df.dropna()
