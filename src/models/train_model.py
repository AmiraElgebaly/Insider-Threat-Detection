import pickle
import os
from src.utils import *
from xgboost import XGBClassifier
from src.features.build_features import *
from src.utils_path.path_utils import  get_data_path , get_models_path


root_dir =os.path.join(get_data_path(),"raw")

logon_df = pd.read_csv(os.path.join(root_dir, "train/logon.csv"))
device_df = pd.read_csv(os.path.join(root_dir, "train/device.csv"))
file_df = pd.read_csv(os.path.join(root_dir, "train/file.csv"))
http_df = pd.read_csv(os.path.join(root_dir, "train/http.csv"))
email_df = pd.read_csv(os.path.join(root_dir, "train/email.csv"))

features = build_features(logon_df, device_df, file_df, http_df, email_df)

mal_users_events = read_malicious_events(os.path.join(root_dir, "answers"))

features['day'] = pd.to_datetime(features['day'])
features['is_malicious'] = features.apply(is_malicious_day, args=[mal_users_events], axis=1)

drop_cols = ["day", "user", "is_malicious"]

X = features.drop(columns=drop_cols)
y = features.iloc[:, -1]

top_features = open(os.path.join(get_models_path(),'top_features.txt')

X_train_top_features = X[top_features]

xgboost = XGBClassifier(subsample=0.8999999999999999, max_depth=7, n_estimators=112)
xgboost.fit(X_train_top_features, y)

pickle.dump(xgboost, open(os.path.join(get_models_path(),'xgboost.sav'), 'wb'))
