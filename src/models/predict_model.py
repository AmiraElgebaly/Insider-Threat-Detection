import json
import pickle
import time
from src.features.build_features import *

import pandas as pd
from kafka import KafkaConsumer
from kafka import KafkaProducer
from elasticsearch import Elasticsearch, helpers
from src.utils_path.path_utils import  get_data_path , get_models_path
import os


def extract_logs(data, col_num, content=False):
    if content:
        data = [d.split("\r")[0] for d in data.split(",")]
        columns = data[1:col_num + 1]
        rows = data[col_num + 1:]
    else:
        data = [d.split("\n")[0] for d in data.split(" ") if d != ""]
        columns = data[:col_num]
        rows = data[col_num:]

    df = [rows[r:r + col_num] for r in range(0, len(rows), col_num)]
    df = pd.DataFrame(df, columns=columns, index=None)
    return df


def detect_insiders(features):
    xgb = pickle.load(open(os.path.join(get_models_path(),"xgboost.sav"), 'rb'))
    prediction_labels = xgb.predict(features)
    return prediction_labels


consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], auto_offset_reset='earliest',
                         consumer_timeout_ms=5000)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

elastic_client = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "90ryEhZZt4fverYocHj_"), ca_certs="D:\Program Files\elasticsearch-8.1.2\config\certs\http_ca.crt"
)

consumer.subscribe(['logon_logs', 'device_logs', 'file_logs', 'http_logs', 'email_logs'])

logon_data = []
device_data = []
file_data = []
http_data = []
email_data = []

for query in consumer:
    data = query.value.decode('utf-8')
    topic = query.topic

    if topic == "logon_logs":
        df = extract_logs(data, 5)
        logon_data.append(df)

    if topic == "device_logs":
        df = extract_logs(data, 6)
        device_data.append(df)

    if topic == "file_logs":
        df = extract_logs(data, 9, True)
        file_data.append(df)

    if topic == "http_logs":
        df = extract_logs(data, 7, True)
        http_data.append(df)

    if topic == "email_logs":
        df = extract_logs(data, 12, True)
        email_data.append(df)


print("Log files consumed...")

merged_features = build_features(pd.concat(logon_data), pd.concat(device_data), pd.concat(file_data), pd.concat(http_data), pd.concat(email_data))
merged_features.to_csv(os.path.join(get_data_path(),"processed/test.csv"))

print("Features extracted...")

file = open(os.path.join(get_models_path(),"top_features.txt"))
top_features = file.read().split("\n")

merged_features = pd.read_csv(os.path.join(get_data_path() , "processed/test.csv"))
unique_dates = merged_features.day.unique()

for date in unique_dates:
    day = merged_features[merged_features.day == date]
    day = day[top_features]
    labels = detect_insiders(day)
    day["prediction"] = labels
    day["day"] = merged_features["day"]
    day["user"] = merged_features["user"]
    result = day[day.prediction == 1]
    producer.send('insider_threat-predictions', value=result.to_dict())
    producer.flush()
    helpers.bulk(elastic_client, result.transpose().to_dict().values(), index="insider_threat_predictions")
    time.sleep(2)


consumer.close()
