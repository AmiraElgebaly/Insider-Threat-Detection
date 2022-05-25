
import os
import glob2
import random
import pandas as pd


def sample_random_users(df, insiders_df, test_size=0.25, random_state=0):
    """Randomly sample a percentage of available malicious and benign
    users and splits them into training and testing sets. Malicious
    users are identified from insiders_df, but as our data is already
    downsampled not all the users are available so the intersection of 
    the available users and all malicious users is computed. 

    args:
        df: Dataframe containing sampled users
        insiders_df: DataFrame containing all malicious users
        test_size (float): percentage of the test data
        random_state (int): random state for to be set as seed. 
    """
    all_malicious_users = insiders_df['user'].values
    all_users = df["user"].values

    random.seed(random_state)

    malicious_users = list(set(all_malicious_users).intersection(all_users))
    benign_users = list(set(all_users).difference(malicious_users))

    test_malicious_users = random.sample(malicious_users,
                                         int(len(malicious_users) * test_size))

    test_benign_users = random.sample(benign_users,
                                      int(len(benign_users) * test_size))

    train_malicious_users = list(set(malicious_users).difference(test_malicious_users))
    train_benign_users = list(set(benign_users).difference(test_benign_users))

    train_users = train_benign_users + train_malicious_users
    test_users = test_benign_users + test_malicious_users
    return train_users, test_users

def split_by_date(filepath, start_date, end_date, output_dir, chunk_size=2000):
    """Read a csv file and extract rows that lie within a certain 
    period of time. As the csv size may be large, the data is read in 
    chunks. It assumes that the csv is ordered by date.

    args:
        filepath: path to the csv file.
        start_date (str): date marking the beginning of the required period.
        end_date (str): date the end of the required period
        output_dir (str): path to which the extracted csv will be written.
        chunk_size (int): number of rows to be read at once.
    """
    #read header to construct empty df
    df = pd.read_csv(filepath, nrows=0)
    
    for chunk in pd.read_csv(filepath, chunksize=chunk_size):
        chunk['date'] = pd.to_datetime(chunk['date'])
        
        # data is ordered by date. If first row is after our end date, then all the remaining rows will be. We can stop.
        if(chunk.loc[chunk.index[0], 'date'].date() >  pd.to_datetime(end_date).date()):
            break
        
        chunk_inrange =  chunk[(chunk['date'] > start_date) & (chunk['date'] < end_date)]
        df = pd.concat([df, chunk_inrange])

    filename = os.path.basename(filepath)
    output_path = os.path.join(output_dir, filename)
    df.to_csv(output_path)

def get_delay(df):
    """Find the delay of the first  detection of a malicious scenario in days.
    args: 
        df: DataFrame contaiining malicious users' daily actions, whether they were correctly classified or not.
    returns:
        DataFrame containing the delay of detecting each malicious user.
    """
    df = df.sort_values(by="day")
    users = df.user.unique()
    results = []
    for user in users:
        scenario = df[df['user']==user]['scenario'].values[0]
        idxs = df[df['user']==user]["correctly_clf"].to_numpy().nonzero()[0]
        if(len(idxs)):
            first_detected_date = df[df['user']==user].iloc[idxs[0], 0]
            delay = (first_detected_date - df[df['user']==user].iloc[0, 0]).days
        else:
            delay = None 
        results.append((user, scenario, delay))  
    delay_df = pd.DataFrame(results, columns=['user', 'scenario', 'delay'])
    return delay_df

def read_answer_file(path):
    """Read an answer file from the dataset"""
    df = pd.read_csv(path, header=None, usecols=[0,1,2,3,4]).rename(columns={0: 'action',
                                                                            1: 'ID',
                                                                            2: 'date',
                                                                            3: 'user',
                                                                            4: 'pc'})
    df['date'] = pd.to_datetime(df['date'])
    df['day'] = df['date'].dt.date
    return df


def read_malicious_events(answers_dir, dataset_ver=5.2):
    """Read answer files of the dataset
    args: 
        answers_dir: directory containing answer files
        dataset_ver: version of the dataset
    Returns:
        dictionary where keys are user ids and values are dataframes with corresponding malicious actions"""
    mal_event_paths = glob2.glob(os.path.join(answers_dir, f'r{dataset_ver}-*/*'))
    mal_users = [get_user_from_path(p) for p in mal_event_paths]
    mal_user_events = {user: read_answer_file(path)
                       for (user, path) in zip(mal_users, mal_event_paths)}
    return mal_user_events


def is_malicious_day(x , mal_users_events):
    """Labels a users activity within a day as malicious or benign.
    args:
        x: Pandas series containing user and day.
        mal_users_events: dictionary containing users and their corresponding malicios actions
    returns:
        int 
    """
    day = x.day
    user = x.user

    if(user not in mal_users_events):
        return 0

    user_df = mal_users_events[user]
    mal_events_count = user_df[(user_df['day'] == day)].shape[0]
    return int(mal_events_count > 0)


def is_malicious_user(user, X, clf, all_features):
    """Labels a user as malicious if any of their daily predictions is malicious.
    """
    idxs = all_features[all_features["user"] == user].index.values
    user_preds = clf.predict(X.loc[idxs])
    return int(user_preds.any())


def is_malicious(x , mal_users_events):
    """Label a day as either malicious or benign
    """
    x = list(x)
    user = x[1]
    min_date = x[0]
    max_date = x[0]
    if(user not in mal_users_events):
        return 0
    else:
        user_df = mal_users_events[user]
        mal_events_count = user_df[(user_df['date'] >= min_date) &
                                   (user_df['date'] <= max_date)].shape[0]
        return int(mal_events_count > 0)

def get_user_from_path(path):
    user = path.split('/')[-1].split('.csv')[0].split('-')[-1]
    return user
