from confluent_kafka import Consumer, Producer
import os
from dotenv import load_dotenv
from utils import connect_to_mysql, load_config, remove_html_for_sql_parsing, convert_unix_timestamp, publish_message, handle_non_serializable_types
import logging
import json
load_dotenv()
# sys.path.append('/Users/justinelim/Documents/GitHub/smart-city-analytics')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

"""
Read data from raw Kafka stream, clean and write to clean_ Kafka topic and 
store/persist cleaned data in MySQL db.
"""

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        return handle_non_serializable_types(obj)


class DataProcessor:
    """
    Base class for specific dataset processors: sets the general structure
    & common functionalities that all specific dataset processors will use.
    """
    primary_key = None
    primary_key_idx = None
    html_field = None

    def __init__(self, kafka_topic, json_config):
        self.kafka_topic = kafka_topic
        self.json_config = json_config
        self.topic_config = self.json_config[kafka_topic]
        self.raw_field_names = self.topic_config["raw_field_names"]

    @staticmethod
    def deserialize_data(data: bytes) -> list:
        return json.loads(data)

    @staticmethod
    def serialize_data(data: list) -> bytes:
        return json.dumps(data, cls=DecimalEncoder)
    
    def transform_data(self, data: bytes) -> list:
        return self.deserialize_kafka_message(data)
    
    
    def get_sql_query(self, data):
        placeholders = ', '.join(["%s"] * len(data))
        insert_query = f"INSERT INTO clean_{self.kafka_topic} VALUES ({placeholders})"
        if self.primary_key and self.html_field and self.primary_key_idx is not None:
            update_query = f"UPDATE clean_{self.kafka_topic} SET {self.html_field} = %s WHERE {self.primary_key} = %s"
            return insert_query, data, update_query, self.html_field, self.primary_key_idx
        else:
            return insert_query, data

# class CulturalEventsProcessor(DataProcessor):
#     primary_key = "organizer_id"
#     html_field = "event_description"
#     primary_key_idx = -4

#     def transform_data(self, data):
#         """
#         Handle special processing required before inserting the data into the database:
#         (1) Remove HTML for SQL parsing (`event_description`)
#         (2) Handle field which has comma-separated values (`event_type`)
#         (3) Convert Unix timestamp values to human-readable timestamp (`timestamp`)
#         """
#         logging.debug("Reached function transform_data()")
#         no_of_fields = 19
#         html_field_idx = 9
        
#         # Remove HTML for SQL parsing
#         data, html_field = remove_html_for_sql_parsing(self.deserialize_data(data), html_field_idx)

#         # Handle field which has comma-separated values (`event_type`)
#         if len(data) > no_of_fields:
#             # Concatenate the last columns from index 18 onwards
#             last_field = ','.join(data[18:])
#             # Create a modified message with the concatenated field at index 18
#             data = data[:18] + (last_field,)

#         placeholders = ', '.join(["%s"] * len(data))

#         # Convert Unix timestamp values to human-readable timestamp
#         data = convert_unix_timestamp(data, 5)
#         logging.debug(f"data in transform_data: {data}")
#         serialized_data = self.serialize_data(data)
#         return serialized_data, data, placeholders, html_field

# class LibraryEventsProcessor(DataProcessor):
#     primary_key = "id"
#     html_field = "content"
#     primary_key_idx = -2

#     def transform_data(self, data):
#         """
#         Handle special processing required before inserting the data into the database:
#         (1) Remove HTML for SQL parsing (`content`)
#         (2) Handle field which has comma-separated values (`teaser`)
#         (3) Standardize column names
#         """
#         logging.debug("Reached transform_data() method of the LibraryEventsProcessor class")
#         no_of_fields = 20
#         html_field_idx = 7

#         # Remove HTML for SQL parsing
#         data, html_field = remove_html_for_sql_parsing(self.deserialize_data(data), html_field_idx)
        
#         # Handle field which has comma-separated values (`teaser`) i.e. at index 11 or -9
#         if len(data) > no_of_fields:
#             # Create a modified message with the values of the field from index 11 to the index before index -8 concatenated
#             concatenated_value = ''.join(data[11:-8])

#             # Create a new tuple with the modified values
#             data = data[:11] + (concatenated_value,) + data[-8:]
        
#         placeholders = ', '.join(["%s"] * len(data))

#         logging.debug(f"data in transform_data: {data}")
#         serialized_data = self.serialize_data(data)
#         return serialized_data, data, placeholders, html_field
        
class ParkingProcessor(DataProcessor):
    no_of_fields = 20
    primary_key = "garagecode"
    primary_key_idx = 4

    def transform_data(self, data: bytes):
        """
        Handle special processing required before inserting the data into the database:
        (1) Enrich with metadata
        """
        logging.debug("Reached transform_data() method of the ParkingProcessor class")

        deserialized_data = self.deserialize_data(data)
        
        enriched_data = self.enrich_parking_data(deserialized_data, self.primary_key, self.primary_key_idx)
        # serialized_data = self.serialize_data(enriched_data)
        # logging.debug(f"Type(Enriched data): {type(enriched_data)}")

        return enriched_data
    
    def enrich_parking_data(self, data: list, primary_key: str, primary_key_idx: int):
        """
        Add parking metadata to the main parking dataset.
        :data: incoming message or a list of values
        :primary_key:
        :primary_key_idx:
        """
        garage_code_value = data[primary_key_idx]  # Get the actual value from data
        logging.debug(f"Garage code value for lookup: {garage_code_value}")

        conn = connect_to_mysql()
        cursor = conn.cursor()
        query = f"SELECT * FROM smart_city.parking_metadata WHERE {primary_key} = %s"
        
        cursor.execute(query, (garage_code_value,))
        row = cursor.fetchone()

        if row:
            city = row[1]
            postal_code = row[2]
            street = row[3]
            house_number = row[4]
            latitude = row[5]
            longitude = row[6]

            # Add columns from `parking_metadata` to the incoming_message
            data.extend([city, postal_code, street, house_number, latitude, longitude])
            enriched_message = tuple(data)

            cursor.close()
            conn.close()
            return enriched_message  # Convert tuple back to list for serialization and then return the enriched data in byte format.
        else:
            cursor.close()
            conn.close()
            return data  # If there's no additional data to enrich with, return the original message in byte format.


class RawDataProcessor:
    """
    Main orchestrator:
    - consumes raw messages from Kafka topics
    - decides which dataset-specific processor to use
    - processes the message using that processor
    - publishes cleaned data back to Kafka
    - stores cleaned data in MySQL db
    """
    def __init__(self) -> None:
        self.kafka_config = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            'group.id': 'consumer_group_id',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'true',
            # 'auto.commit.interval.ms': '5000',
        }
        self.consumer = Consumer(self.kafka_config)
        self.producer = Producer(self.kafka_config)
        self.json_config = load_config(config_path="config.json")
        self.topics = [
            # 'cultural_events',  # done (convert Unix timestamp field to human-readable timestamp, ran into a lot of trouble because of the HTML & event_type field values that were comma-separated)
            # 'library_events',  # done (standardize column names in MySQL, required extra processing of HTML field & concatenation issue when persisting in MySQL similar to that faced with cultural_events)
            'parking',  # done (enrich with metadata)
            # 'pollution',  # done
            # 'road_traffic',  # done
            # 'social_events',  # done
            # 'weather',  # done
            ]
        self.consumer.subscribe(self.topics)
        self._start_processing()


    def _start_processing(self):
        try:
            while True:
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    logging.error(f"Error while consuming message: {message.error()}")
                else:
                    logging.debug(f"Consumed message with key {message.key()} and value {message.value()}")
                    self.process_message(message)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def process_message(self, message):
        """
        Dynamically selects and instantiates the correct processor.
        """
        logging.info("Reached function process_message()")

        topic = message.topic()
        logging.debug(f"Processing message from topic: {topic}")

        try:
            # processor_class_name = self.topic_config[topic]["processor_class"]
            processor_config = self.json_config[topic]  # Assuming self.config contains the entire loaded configuration
            processor_class_name = processor_config["processor_class"]

        except KeyError as e:
            logging.error(f"Failed to get processor class for topic {topic}. Error: {e}")
            return  # Return early if there's an error
        
        # Dynamically instantiate the correct processor using its class name
        processor_class = globals()[processor_class_name]
        processor = processor_class(topic, self.json_config)
        
        incoming_message = message.value() # incoming_message: bytes
        # serialized_message = ', '.join(str(field) for field in incoming_message)

        data = processor.transform_data(incoming_message)
    
        # logging.debug(f"Data: {data}")
        # logging.debug(f"Type(Data): {type(data)}")

        publish_message(self.producer, f"clean_{topic}", processor.serialize_data(data))
        logging.debug(f"Serialized Data: {processor.serialize_data(data)}")

        self.persist_data_in_mysql(processor, data)

    def persist_data_in_mysql(self, processor, message):

        logging.info("Reached persist_data_in_mysql method")
        conn = connect_to_mysql()
        cursor = conn.cursor()
        
        # preprocessed_message, html_field = processor.preprocess_for_db(message)
        query_data = processor.get_sql_query(message)

        # Check if there's an additional update query and html index for the data to insert
        if len(query_data) == 5:
            insert_query, data_to_insert, update_query, html_field, primary_key_idx = query_data
            logging.debug(f"Insert Query received in persist_data_in_mysql: {insert_query}")
            logging.debug(f"data_to_insert received in persist_data_in_mysql: {data_to_insert}")

            logging.debug(f"Update Query received in persist_data_in_mysql: {update_query}")
            cursor.execute(insert_query, data_to_insert)
            # Assuming there's an HTML field attribute in your processor
            cursor.execute(update_query, (html_field, data_to_insert[primary_key_idx]))
        else:
            insert_query, data_to_insert = query_data
            # logging.debug(f"Insert Query received in persist_data_in_mysql: {insert_query}")
            # logging.debug(f"data_to_insert received in persist_data_in_mysql: {data_to_insert}")
            cursor.execute(insert_query, data_to_insert)

        conn.commit()
        cursor.close()
        conn.close()


def main():
    processor = RawDataProcessor()