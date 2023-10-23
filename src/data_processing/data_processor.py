from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import os
from datetime import datetime
from abc import ABC, abstractmethod
import json
from src.data_processing.config_loader import ConfigLoader
from src.data_processing.elasticsearch_client import ElasticsearchClient
import logging

from dotenv import load_dotenv
load_dotenv()

class DataProcessor(ABC):
    def __init__(self, config_path, kafka_topic) -> None:
        self.logger = logging.getLogger(__name__)
        self.config_loader = ConfigLoader(config_path)
        self.elasticsearch_client = ElasticsearchClient()
        self.config_path = config_path
        self.kafka_topic = kafka_topic
        self.dataset = self.kafka_topic.replace('clean_', '')
        self.field_names = []
        self.field_types = []

    
    def load_clean_config(self):
        self.field_names, self.field_types = self.config_loader.load_clean_config(self.dataset)
    
    def deserialize_kafka_message(self, message: str):
        # print('message received in deserialize_kafka_message():', message)
        try:
            message_json = json.loads(message)
            if len(message_json) != len(self.field_types):
                raise ValueError("Mismatched number of fields in the message")

            conversion_funcs = [self.get_conversion_function(type_str) for type_str in self.field_types]

            converted_data = []
            for i, func in enumerate(conversion_funcs):
                if i < len(message_json) and message_json[i] is not None:
                    converted_data.append(func(message_json[i]))
                else:
                    # Handle missing or None values
                    converted_data.append(None)

            return tuple(converted_data)
        
        except (json.JSONDecodeError, ValueError) as e:
            # Handle JSON decoding error
            logging.error(f"Error processing message: {e}")
            return None

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
        self.logger.debug('Processing data...')

        env = StreamExecutionEnvironment.get_execution_environment()

        # kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        # print(f"KAFKA_BOOTSTRAP_SERVERS: {kafka_bootstrap_servers}")

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
        logging.debug('Kafka Consumer initialized')
        pyflink_field_types = [self.get_pyflink_field_type(type_str) for type_str in self.field_types]
        transformed_stream = stream \
                    .map(self.deserialize_kafka_message, output_type=Types.TUPLE(pyflink_field_types)) \
                    .map(self.transform_data, output_type=Types.MAP(Types.STRING(), Types.STRING()))

        logging.debug('Transformation completed')

        transformed_stream.map(self.send_to_elasticsearch, output_type=Types.INT())
        logging.debug('Data sent to Elasticsearch')

        env.execute("Kafka to Elasticsearch pipeline")