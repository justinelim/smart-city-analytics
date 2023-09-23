import os
from kafka import KafkaProducer
import time
from utils import connect_to_mysql
# from .utils import connect_to_mysql

from dotenv import load_dotenv
load_dotenv()

print('data_stream_sim is running')

kafka_config = {
    'bootstrap_servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'value_serializer': str.encode
}

# Connect to MySQL database
conn = connect_to_mysql()
cursor = conn.cursor()

# Initialize Kafka producer
producer = KafkaProducer(**kafka_config)

# Fetch data from MySQL tables and publish to Kafka topic
tables = [
    # 'cultural_events',
    # 'library_events',
    'parking',
    # 'pollution',
    # 'road_traffic',
    # 'social_events',
    # 'weather',
    ]

for table in tables:
    query = f"SELECT * FROM {table}"
    cursor.execute(query)
    records = cursor.fetchall()

    for record in records:
        serialized_record = ','.join(str(field) for field in record)
        producer.send(table, value=serialized_record)
        time.sleep(1)

cursor.close()
conn.close()
producer.close()
