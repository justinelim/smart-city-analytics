from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import requests
import os
from datetime import datetime
import pytz
from utils import load_config
from abc import ABC, abstractmethod
import json
from src.data_processing.config_loader import ConfigLoader
from src.data_processing.elasticsearch_client import ElasticsearchClient
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

print('kafka_es_http is running')
"""
Read data from Kafka topic, process and send processed data to Elasticsearch.
"""

class DataProcessor(ABC):
    def __init__(self, config_path, kafka_topic) -> None:
        self.config_loader = ConfigLoader(config_path)
        self.elasticsearch_client = ElasticsearchClient()
        self.config_path = config_path
        self.kafka_topic = kafka_topic
        self.dataset = self.kafka_topic.replace('clean_', '')
        self.field_names = []
        self.field_types = []
    
    def load_clean_config(self):
        self.config_loader.load_clean_config(self.dataset)
    
    def deserialize_kafka_message(self, message: str):
        message_json = json.loads(message)
        # Instead of iterating over field names, simply access the values by their index
        conversion_funcs = [self.get_conversion_function(type_str) for type_str in self.field_types]
        return tuple(func(message_json[i]) for i, func in enumerate(conversion_funcs))


    def get_conversion_function(self, type_str):
        """
        Map each string type to its corresponding function
        """
        TYPE_CONVERSION_MAPPING = {
            "INT": int,
            "STRING": str,
            "FLOAT": float,
            "DATETIME": lambda x: datetime.strptime(x.strip(), '%Y-%m-%d %H:%M:%S'),
        }
        type_str = type_str.upper()
        if type_str in TYPE_CONVERSION_MAPPING:
            return TYPE_CONVERSION_MAPPING[type_str]
        raise ValueError(f"Unsupported field type: {type_str}")

    def get_pyflink_field_type(self, type_str):
        """
        Map type strings to PyFlink Types
        e.g. "STRING" -> Types.STRING()
        """
        TYPE_MAPPING = {
            "INT": Types.INT(),
            "STRING": Types.STRING(),
            "FLOAT": Types.FLOAT(),
            "DATETIME": Types.SQL_TIMESTAMP(),
        }
        type_str = type_str.upper()
        if type_str in TYPE_MAPPING:
            return TYPE_MAPPING[type_str]
        raise ValueError(f"Unsupported field type: {type_str}")
    
    @abstractmethod
    def transform_data(self, data):
        """
        Abstract method for transforming data. Must be implemented by subclasses.
        """
        pass

    def send_to_elasticsearch(self, value):
        self.elasticsearch_client.send_to_elasticsearch(self.dataset, value)
        
    def process_data(self):
        env = StreamExecutionEnvironment.get_execution_environment()

        properties = {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "group.id": "flink-test-group",
        }

        kafka_consumer = FlinkKafkaConsumer(
            self.kafka_topic,
            SimpleStringSchema(),
            properties
        ).set_start_from_earliest()

        stream = env.add_source(kafka_consumer)
        logging.debug('----------------------BEFORE------------------------')
        stream.print()

        pyflink_field_types = [self.get_pyflink_field_type(type_str) for type_str in self.field_types]
        transformed_stream = stream \
                        .map(self.deserialize_kafka_message, output_type=Types.TUPLE(pyflink_field_types)) \
                        .map(self.transform_data, output_type=Types.MAP(Types.STRING(), Types.STRING()))

        # 0 vehicle_count, 1 update_time, 2 _id, 3 total_spaces, 4 garage_code, 5 stream_time, 6 city, 7 postal_code, 8 street, 9 house_number, 10 latitude, 11 longitude
        logging.debug('----------------------AFTER------------------------')
        transformed_stream.print()

        transformed_stream.map(self.send_to_elasticsearch, output_type=Types.INT())

        env.execute("Kafka to Elasticsearch pipeline")

class CulturalEventsDataProcessor(DataProcessor):
    def transform_data(self, data):
        if len(data) != 19:
            logging.error(f"Unexpected data length: {len(data)}. Data: {data}")
            return None  # or some default value
        _, _, _, _, avg_ticket_price, _, _, _, event_id, _, _, _, event_date, _, _, _, _, _, event_type = data
        input_timezone = pytz.timezone('Asia/Shanghai')
        parsed_date = input_timezone.localize(event_date).astimezone(pytz.UTC)
        formatted_date = parsed_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        return {
            "event_id": str(event_id),
            "event_date": formatted_date,
            "event_type": event_type,
            "avg_ticket_price": str(avg_ticket_price)
        }
class ParkingDataProcessor(DataProcessor):
    def transform_data(self, data):
        vehicle_count, update_time, _, total_spaces, garage_code, _, _, _, _, _, _, _ = data
        input_timezone = pytz.timezone('Asia/Shanghai')
        parsed_date = input_timezone.localize(update_time).astimezone(pytz.UTC)
        
        formatted_date = parsed_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
        # Calculate the percentage
        occupancy_rate = (vehicle_count / total_spaces) * 100
        
        return {
            "vehicle_count": str(vehicle_count),
            "update_time": formatted_date,
            "garage_code": garage_code,
            "total_spaces": str(total_spaces),
            "occupancy_rate": str(occupancy_rate)  # Add percentage to the output
        }
    
def main():
    # Set the Kafka topic (e.g., "clean_parking")
    kafka_topic = "clean_cultural_events"
    # kafka_topic = "clean_parking"
    config_path = "config.json"

    if kafka_topic == "clean_parking":
        data_processor = ParkingDataProcessor(
            config_path=config_path,
            kafka_topic=kafka_topic
        )
    elif kafka_topic == "clean_cultural_events":
        data_processor = CulturalEventsDataProcessor(
            config_path=config_path,
            kafka_topic=kafka_topic
        )
    
    data_processor.load_clean_config()
    data_processor.process_data()


if __name__ == '__main__':
    main()
