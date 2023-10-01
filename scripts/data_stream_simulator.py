import os
from confluent_kafka import Producer
import time
from utils import connect_to_mysql, publish_message
from threading import Thread
from dotenv import load_dotenv

load_dotenv()

print('data_stream_sim is running')

kafka_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS")
}

# Initialize Kafka producer
producer = Producer(kafka_config)

def process_dataset(dataset):
    local_conn = connect_to_mysql()
    local_cursor = local_conn.cursor()

    query = f"SELECT * FROM {dataset}"
    local_cursor.execute(query)
    records = local_cursor.fetchall()

    for record in records:
        serialized_record = ','.join(str(field) for field in record)
        publish_message(producer, dataset, serialized_record)
        time.sleep(1)

    local_cursor.close()
    local_conn.close()


# Fetch data from MySQL tables and publish to Kafka topic
dataset_list = [
    # 'cultural_events',
    # 'library_events',
    # 'parking',
    'pollution',
    'road_traffic',
    'social_events',
    'weather',
]

threads = []

# Start threads for each dataset
for dataset in dataset_list:
    thread = Thread(target=process_dataset, args=(dataset,))
    thread.start()
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()

cursor.close()
conn.close()
producer.close()
