import os
import numpy as np
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
from src.utils_path.path_utils import  get_data_path , get_models_path

TOPIC_NAME = "email_logs"


def instantiate_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
    )

    if producer.bootstrap_connected():
        print(f"Successfully connected to bootstrap server")
    else:
        print("Couldn't connect to bootstrap server.")

    return producer


def produce_message(producer_instance, topic, message):
    producer_instance.send(topic, message)
    producer_instance.flush()
    return


if __name__ == "__main__":

    root_dir = os.path.join(get_data_path(),'raw/test')
    email_logfile = pd.read_csv(os.path.join(root_dir, "email.csv"))
    email_logfile["date"] = pd.to_datetime(email_logfile['date']).dt.normalize()
    unique_dates = email_logfile["date"].unique()

    producer = instantiate_kafka_producer()

    print("Ingesting the Data in Batches per Day")

    for batch_id, date in enumerate(unique_dates):
        start_time = datetime.now()
        print(f"Ingesting Batch: {batch_id}")

        email_logfile['attachments'] = email_logfile['attachments'].replace(np.nan, '', regex=True)
        email_logfile['bcc'] = email_logfile['bcc'].replace(np.nan, '', regex=True)
        day_records = email_logfile[email_logfile.date == date]
        day_records = day_records.to_csv()
        day_records = bytes(day_records, encoding="utf-8")
        produce_message(producer_instance=producer, topic=TOPIC_NAME, message=day_records)

        end_time = datetime.now()
        print(f"Batch {batch_id} took {end_time - start_time} time for ingesting data")

    print("Ingestion Completed")
