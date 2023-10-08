from confluent_kafka import Consumer, Producer
import os
from dotenv import load_dotenv
from utils import connect_to_mysql, load_config, handle_html, convert_unix_timestamp, publish_message
import logging
load_dotenv()
# sys.path.append('/Users/justinelim/Documents/GitHub/smart-city-analytics')
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

"""
Read data from raw Kafka stream, clean and write to clean_ Kafka topic and store/persist cleaned data in MySQL db.
"""


class DataProcessor:
    """
    Base class for specific dataset processors: sets the general structure
    & common functionalities that all specific dataset processors will use.
    """
    def __init__(self, kafka_topic, json_config):
        self.kafka_topic = kafka_topic
        self.json_config = json_config
        self.topic_config = self.json_config[kafka_topic]
        self.raw_field_names = self.topic_config["raw_field_names"]

    @staticmethod
    def deserialize_kafka_message(incoming_message: bytes) -> list:
        logging.info("Reached function deserialize_kafka_message()")

        incoming_message_str = str(incoming_message.decode('utf-8'))
        return incoming_message_str.split(',')

    @staticmethod
    def deserialize_data(incoming_message: bytes) -> str:
        logging.info("Deserialize Kafka message")
        return incoming_message.decode('utf-8')

    @staticmethod
    def serialize_data(data: list) -> bytes:
        return ','.join(data).encode('utf-8')
    
    def transform_data(self, data: bytes) -> list:
        return self.deserialize_kafka_message(data)
    
    def preprocess_for_db(self, data):
        return data  # default is no preprocessing
    
    def get_sql_query(self, data):
        placeholders = ', '.join(["%s"] * len(data))
        insert_query = f"INSERT INTO clean_{self.kafka_topic} VALUES ({placeholders})"
        return insert_query, data

class CulturalEventsProcessor(DataProcessor):
    def transform_data(self, data):
        """
        Handle special processing required before inserting the data into the database:
        (1) Parse HTML field (`event_description`)
        (2) Handle last field (`event_type`) whose values can be comma-separated
        (3) Convert Unix timestamp field to human-readable timestamp (`timestamp`)
        """
        logging.debug("Reached function transform_data()")
        no_of_fields = 19
        html_field_idx = 9
        # message_str = ', '.join(data)  # Re-create the message string from the message list
        
        data, html_field = handle_html(self.deserialize_data(data), no_of_fields, html_field_idx)
        

        if len(data) > no_of_fields: # handle last field whose values are also comma-separated
            # Concatenate the last columns from index 18 onwards
            last_field = ','.join(data[18:])
            # print('LAST_FIELD', last_field)
            # Create a modified message with the concatenated field at index 18
            data = data[:18] + (last_field,)
        # return modified_message, html_field
        placeholders = ', '.join(["%s"] * len(data))


        # data = convert_unix_timestamp(self.deserialize_kafka_message(data), 5)
        data = convert_unix_timestamp(data, 5)
        logging.debug(f"data in transform_data: {data}")
        serialized_data = self.serialize_data(data)
        return serialized_data, data, placeholders, html_field
    
    
    def get_sql_query(self, data, placeholders, html_field):
        """
        Provide appropriate SQL query e.g. insert & update
        """
        logging.debug("Reached function get_sql_query()")
        # logging.debug(f"Incoming data: {data}")
        # data = tuple(self.deserialize_data(data))
        # modified_message, html_field = self.preprocess_for_db(data)

        # placeholders = ', '.join(["%s"] * len(data))  # Update the number of placeholders
        # print('MOD_MSG', modified_message)
        insert_query = f"INSERT INTO clean_{self.kafka_topic} VALUES ({placeholders})"
        # cursor.execute(query, modified_message)  # Exclude the HTML field from the modified_message
        # conn.commit()
        # Separate update query for HTML field - this will be handled separately after the initial insert
        update_query = f"UPDATE clean_{self.kafka_topic} SET event_description = %s WHERE organizer_id = %s"
        # print('HTML_FIELD', html_field)
        # print('ORGANIZER_ID', message[-4])
        # cursor.execute(html_query, (html_field, modified_message[-4]))  # Pass the actual HTML field and primary key organizer_id
        return insert_query, data, update_query, html_field, -4

class LibraryEventsProcessor(DataProcessor):

    def preprocess_for_db(self, data):
        no_of_fields = 20

        message_str = ', '.join(data)  # Re-create the message string from the message list
        modified_message, placeholders, html_field = handle_html(message_str, 7)

        if len(modified_message) > no_of_fields:
            # Create a modified message with the values of the field from index 11 to the index before index -8 concatenated
            concatenated_value = ''.join(modified_message[11:-8])

            # Create a new tuple with the modified values
            modified_message = modified_message[:11] + (concatenated_value,) + modified_message[-8:]
        return modified_message, html_field
            
    def get_sql_query(self, data):
        modified_message, html_field = self.preprocess_for_db(data)

        placeholders = ', '.join(["%s"] * len(data))  # Update the number of placeholders
        # print('MODIFIED_MESSAGE', data)
        insert_query = f"INSERT INTO clean_{self.kafka_topic} VALUES ({placeholders})"
        # print('INSERT_QUERY', insert_query)

        # Insert the HTML field separately
        update_query = f"UPDATE clean_{self.kafka_topic} SET content = %s WHERE id = %s"  # Assuming there's an 'id' column
        # cursor.execute(html_query, (html_field, modified_message[-2]))  # Pass the actual HTML field and message primary key index
        return insert_query, data, update_query, html_field, -2
class ParkingProcessor(DataProcessor):
    def transform_data(self, data: bytes):
        data = self.enrich_parking_data(self.deserialize_kafka_message(data), 4)
        return data
    
    def enrich_parking_data(self, data: list, idx: int):
        """
        Add parking metadata to the main parking dataset.
        :data: incoming message or a list of fields
        :int:
        """
        fields = data
        print('FIELDS', fields)

        primary_key = fields[idx]  # The primary key column `garagecode` is at index 4 in the incoming message
        print('PRIMARY_KEY', primary_key)

        conn = connect_to_mysql()
        cursor = conn.cursor()
        query = f"SELECT * FROM smart_city.parking_metadata WHERE garagecode = %s"
        
        cursor.execute(query, (primary_key,))
        row = cursor.fetchone()

        if row:
            city = row[1]
            postal_code = row[2]
            street = row[3]
            house_number = row[4]
            latitude = row[5]
            longitude = row[6]

            # Add columns from `parking_metadata` to the incoming_message
            fields.extend([city, postal_code, street, house_number, latitude, longitude])
            enriched_message = tuple(fields)

            cursor.close()
            conn.close()
            return self.serialize_data(list(enriched_message))  # Convert tuple back to list for serialization and then return the enriched data in byte format.
        else:
            cursor.close()
            conn.close()
            return self.serialize_data(data)  # If there's no additional data to enrich with, return the original message in byte format.


class RawDataProcessor:
    """
    Main orchestrator: consumes raw messages from Kafka topics, decides which dataset-specific processor
    to use and processes the message using that processor, after which it publishes 
    cleaned data back to Kafka as well as stores it in MySQL db.
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
            'cultural_events',  # done (convert Unix timestamp field to human-readable timestamp, ran into a lot of trouble because of the HTML & event_type field values that were comma-separated)
            # 'library_events',  # done (standardize column names in MySQL, required extra processing of HTML field & concatenation issue when persisting in MySQL similar to that faced with cultural_events)
            # 'parking',  # done (enrich with metadata)
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

    # def _start_processing(self):
    #     try:
    #         while True:
    #             messages = self.consumer.consume(num_messages=10, timeout=1.0)
    #             logging.debug(f"Messages value: {messages}")

    #             for message in messages:
    #                 if message is None:
    #                     continue
    #                 if message.error():
    #                     print(f"Consumer error: {message.error()}")
    #                     continue

    #                 # Process the received message
    #                 self.process_message(message)
                
    #             self.consumer.commit()

    #     except KeyboardInterrupt:
    #         pass
    #     finally:
    #         self.consumer.close()

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

        serialized_data, data, placeholders, html_field = processor.transform_data(incoming_message)
    
        # logging.debug(f"Serialized message: {serialized_message}")

        publish_message(self.producer, f"clean_{topic}", serialized_data)

        self.persist_data_in_mysql(processor, data, placeholders, html_field)

        # self.persist_message_in_mysql(serialized_message, topic)

        # print('MESSAGE_TYPE', type(message))
        # incoming_message = message.value()  
        # print('INCOMING_MESSAGE', incoming_message)

        # if message.topic() == 'cultural_events':
        #     print('RAW_INCOMING_TYPE', type(incoming_message))
        #     incoming_message = convert_unix_timestamp(incoming_message, 5)
        # elif message.topic() == 'parking':
        #     incoming_message = enrich_parking_data(incoming_message, 4)
        # else:
            # incoming_message = deserialize_kafka_message_bytes(incoming_message)

        # print('PREPROCESSED_MESSAGE', incoming_message)
        # publish_message(incoming_message, message.topic())
        # persist_message_in_mysql(incoming_message, message.topic())
    

    def persist_data_in_mysql(self, processor, message, placeholders, html_field):

        logging.info("Reached persist_data_in_mysql method")
        conn = connect_to_mysql()
        cursor = conn.cursor()
        
        # preprocessed_message, html_field = processor.preprocess_for_db(message)
        query_data = processor.get_sql_query(message, placeholders, html_field)
        

        # Check if there's an additional update query and html index for the data to insert
        if len(query_data) == 5:
            insert_query, data_to_insert, update_query, html_field, html_idx = query_data
            logging.debug(f"Insert Query received in persist_data_in_mysql: {insert_query}")
            logging.debug(f"data_to_insert received in persist_data_in_mysql: {data_to_insert}")

            logging.debug(f"Update Query received in persist_data_in_mysql: {update_query}")
            cursor.execute(insert_query, data_to_insert)
            # Assuming there's an HTML field attribute in your processor
            cursor.execute(update_query, (html_field, data_to_insert[html_idx]))
        else:
            insert_query, data_to_insert = query_data
            cursor.execute(insert_query, data_to_insert)

        conn.commit()
        cursor.close()
        conn.close()


def main():
    processor = RawDataProcessor()