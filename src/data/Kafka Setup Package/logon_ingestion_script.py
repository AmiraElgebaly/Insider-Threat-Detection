import os
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
from src.utils_path.path_utils import  get_data_path

TOPIC_NAME = "logon_logs"


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

    root_dir = os.path.join(get_data_path(),'raw')
    logon_logfile = pd.read_csv(os.path.join(root_dir, "logon.csv"))
    logon_logfile["date"] = pd.to_datetime(logon_logfile['date']).dt.normalize()
    unique_dates = logon_logfile["date"].unique()

    producer = instantiate_kafka_producer()

    print("Ingesting the Data in Batches per Day")
    
    for batch_id, date in enumerate(unique_dates):
        start_time = datetime.now()
        print(f"Ingesting Batch: {batch_id}")

        day_records = logon_logfile[logon_logfile.date == date]
        day_records = bytes(day_records.to_string(), encoding="utf-8")
        produce_message(producer_instance=producer, topic=TOPIC_NAME, message=day_records)

        end_time = datetime.now()
        print(f"Batch {batch_id} took {end_time-start_time} time for ingesting data")

    print("Ingestion Completed")
    
