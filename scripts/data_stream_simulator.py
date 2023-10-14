import os
from confluent_kafka import Producer
import time
from utils import connect_to_mysql, publish_message, handle_non_serializable_types
from threading import Thread
from dotenv import load_dotenv
import json
import datetime
from decimal import Decimal
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

print('data_stream_sim is running')

kafka_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS")
}

# Initialize Kafka producer
producer = Producer(kafka_config)

from decimal import Decimal

def process_dataset(dataset_name):
    local_conn = connect_to_mysql()
    local_cursor = local_conn.cursor()

    query = f"SELECT * FROM {dataset_name}"
    local_cursor.execute(query)
    records = local_cursor.fetchall()

    for record in records:
        cleaned_record = tuple(map(handle_non_serializable_types, record))
        serialized_record = json.dumps(cleaned_record)  # Serialize cleaned record to JSON
        logging.debug(f"Serialized record: {serialized_record}")
        publish_message(producer, dataset_name, serialized_record)
        time.sleep(1)

    local_cursor.close()
    local_conn.close()



# Fetch data from MySQL tables and publish to Kafka topic
dataset_list = [
    'cultural_events',
    # 'library_events',
    # 'parking',
    # 'pollution',
    # 'road_traffic',
    # 'social_events',
    # 'weather',
]

threads = []

# Start threads for each dataset
for dataset_name in dataset_list:
    thread = Thread(target=process_dataset, args=(dataset_name,))
    thread.start()
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()

# producer.close()
