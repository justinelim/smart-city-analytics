from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import requests
import os
from datetime import datetime
import pytz
# from utils import deserialize_kafka_message_str, connect_to_mysql, handle_html_field
from abc import ABC, abstractmethod

import json
# import dill
from dotenv import load_dotenv

load_dotenv()

print('kafka_es_http is running')

class DataProcessor(ABC):
    def __init__(self, config_path, kafka_topic) -> None:
        self.config_path = config_path
        self.kafka_topic = kafka_topic
        self.dataset = self.kafka_topic.replace('clean_', '')
        self.field_names = []
        self.field_types = []
    
    def load_config(self):
        with open(self.config_path, 'r') as config_file:
            config = json.load(config_file)
        dataset_config = config.get(self.kafka_topic, {})
        self.field_names = dataset_config.get('field_names', [])
        self.field_types = dataset_config.get('field_types', [])
        print(self.field_names)
        print(self.field_types)
    

    def deserialize_kafka_message(self, message: str):
        fields = message.split(',')
        conversion_funcs = [self.get_conversion_function(type_str) for type_str in self.field_types]
        return tuple(func(fields[i]) for i, func in enumerate(conversion_funcs))

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
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(f"{os.getenv('ELASTIC_URL')}/processed_{self.dataset}/_doc/", json=value, headers=headers)
            print(f"Elasticsearch Response: {response.status_code}, {response.text}")
            return response.status_code
        except Exception as e:
            print(f"Error sending to Elasticsearch: {e}")
            return -1  # Return an error code
        
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
        stream.print()
        
        # transformed_stream = stream \
        #     .map(self.deserialize_kafka_message, output_type=Types.TUPLE([
        #         Types.INT(), Types.STRING(), Types.INT(), Types.INT(),
        #         Types.STRING(), Types.STRING(), Types.STRING(), Types.INT(),
        #         Types.STRING(), Types.STRING(), Types.FLOAT(), Types.FLOAT()])) \
        #     .map(self.transform_data, output_type=Types.MAP(Types.STRING(), Types.STRING()))

        pyflink_field_types = [self.get_pyflink_field_type(type_str) for type_str in self.field_types]
        transformed_stream = stream \
                        .map(self.deserialize_kafka_message, output_type=Types.TUPLE(pyflink_field_types)) \
                        .map(self.transform_data, output_type=Types.MAP(Types.STRING(), Types.STRING()))

        # 0 vehicle_count, 1 update_time, 2 _id, 3 total_spaces, 4 garage_code, 5 stream_time, 6 city, 7 postal_code, 8 street, 9 house_number, 10 latitude, 11 longitude

        transformed_stream.print()
          # Use dill for serialization
        # transformed_stream.map(lambda x: dill.loads(dill.dumps(self.send_to_elasticsearch, recurse=True)), output_type=Types.INT())

        transformed_stream.map(self.send_to_elasticsearch, output_type=Types.INT())

        env.execute("Kafka to Elasticsearch pipeline")

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
    kafka_topic = "clean_parking"
    config_path = "src/data_processing/config.json"

    if kafka_topic == "clean_parking":
        data_processor = ParkingDataProcessor(
            config_path=config_path,
            kafka_topic=kafka_topic
        )
    
    data_processor.load_config()
    data_processor.process_data()


if __name__ == '__main__':
    main()
