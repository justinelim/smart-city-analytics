"""
Read data from Kafka, transform using PyFlink & 
use native PyFlink Elasticsearch sink to push transformed data to Elasticsearch

Adv: directly integrates Elasticsearch into Flink job, 
can be a more efficient and scalable option when dealing with large volumes of data,
designed to handle the complexities of writing data to Elasticsearch 
while ensuring fault tolerance and high performance.

Risks: compatibility & dependency issues
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.elasticsearch import Elasticsearch7SinkBuilder, ElasticsearchEmitter
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

def deserialize_kafka_message_str(message: str):
    fields = message.split(',')
    return (int(fields[0]), fields[1], int(fields[2]), int(fields[3]), fields[4], fields[5], fields[6], int(fields[7]), fields[8], fields[9], float(fields[10]), float(fields[11]))

def transform_data(data):
    vehicle_count, update_time, _, total_spaces, garage_code, _, _, _, _, _, _, _ = data
    update_time = update_time.strip()
    parsed_date = datetime.strptime(update_time, '%Y-%m-%d %H:%M:%S')
    formatted_date = parsed_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
     # Remove special characters from garage_code
    garage_code = ''.join(e for e in garage_code if e.isalnum())
    garage_code = garage_code.lower()

    return {
        "vehicle_count": str(vehicle_count),
        "update_time": formatted_date,
        "garage_code": garage_code,
        "total_spaces": str(total_spaces)
    }

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///Users/justinelim/Documents/GitHub/smart-city-analytics/src/data_processing/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar")

    env.set_parallelism(1)  # Set the parallelism according to your requirement

    properties = {
        "bootstrap.servers": "localhost:9092",  # Replace with your Kafka server
        "group.id": "my_consumer_group"
    }

    kafka_source = FlinkKafkaConsumer(
        "clean_parking",
        SimpleStringSchema(),
        properties
    ).set_start_from_earliest()

    stream = env.add_source(kafka_source)
    stream.print()

    # Deserialize and transform the message
    transformed_stream = stream \
        .map(deserialize_kafka_message_str, output_type=Types.TUPLE([
            Types.INT(), Types.STRING(), Types.INT(), Types.INT(),
            Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(),
            Types.STRING(), Types.STRING(), Types.FLOAT(), Types.FLOAT()])) \
        .map(transform_data, output_type=Types.MAP(Types.STRING(), Types.STRING()))

    transformed_stream.print()
    
    # Set up Elasticsearch sink
    es7_sink = Elasticsearch7SinkBuilder() \
        .set_hosts([f"{os.getenv('ELASTICSEARCH_HOST')}:{os.getenv('ELASTICSEARCH_PORT')}"])  \
        .set_emitter(ElasticsearchEmitter.dynamic_index('processed_parking')) \
        .set_bulk_flush_max_actions(1)  \
        .set_bulk_flush_interval(60000)  \
        .build()


    # Send the transformed data to Elasticsearch
    transformed_stream.sink_to(es7_sink)

    # Execute the job
    env.execute("Kafka to Elasticsearch Job")
