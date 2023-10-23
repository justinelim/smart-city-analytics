from confluent_kafka import Consumer, Producer
import os
from dotenv import load_dotenv
from utils import connect_to_mysql, load_config, convert_unix_timestamp, publish_message, handle_non_serializable_types
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


class DataCleaner:
    """
    Base class for specific dataset cleaning processors: sets the general structure
    & common functionalities that all specific dataset cleaning processors will use.
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
        return self.deserialize_data(data)
    
    def get_sql_query(self, data):
        placeholders = ', '.join(["%s"] * len(data))
        insert_query = f"INSERT INTO clean_{self.kafka_topic} VALUES ({placeholders})"
        if self.primary_key and self.html_field and self.primary_key_idx is not None:
            update_query = f"UPDATE clean_{self.kafka_topic} SET {self.html_field} = %s WHERE {self.primary_key} = %s"
            return insert_query, data, update_query, self.html_field, self.primary_key_idx
        else:
            return insert_query, data

class CulturalEventsCleaner(DataCleaner):
    ticket_price_field_idx = 4
    timestamp_field_idx = 5

    def transform_data(self, data):
        """
        Handle special processing required before inserting the data into the database:
        (1) Convert Unix timestamp values to human-readable timestamp (`timestamp`)
        (2) Calculate the average ticket price

        """
        logging.debug("Reached function transform_data()")
        
        deserialized_data = self.deserialize_data(data)
        transformed_data = convert_unix_timestamp(deserialized_data, self.timestamp_field_idx)
        transformed_data = self.calc_avg_price(transformed_data, self.ticket_price_field_idx)
        logging.debug(f"data in Cultural transform_data: {transformed_data}")
        return transformed_data
    
    def calc_avg_price(self, data: list, ticket_price_field_idx):
        """
        (1) Clean and parse ticket prices into numerical data type
        (2) Calculate the average ticket price for price ranges
        (3) Map 'Gratis' to 0
        (4) Map 'Billet' to null
        """
        ticket_price = data[ticket_price_field_idx]
        
        # If the ticket price contains a "-", it's treated as a range.
        if '-' in ticket_price:
            # Remove the 'DKK' suffix and split by '-'
            min_price_str, max_price_str = ticket_price.replace('DKK', '').split('-')
            # Convert the extracted prices to float and compute the average
            min_price, max_price = float(min_price_str.strip()), float(max_price_str.strip())
            avg_price = (min_price + max_price) / 2
            data[ticket_price_field_idx] = avg_price
        elif 'DKK' in ticket_price:
            clean_price = float(ticket_price.replace('DKK', '').strip())
            data[ticket_price_field_idx] = clean_price
        elif ticket_price.lower() == 'gratis':
            data[ticket_price_field_idx] = 0.0
        elif ticket_price.lower() == 'billet':
            data[ticket_price_field_idx] = None
        
        return data
        
class ParkingCleaner(DataCleaner):
    primary_key = "garagecode"
    primary_key_idx = 4

    def transform_data(self, data: bytes):
        """
        Handle special processing required before inserting the data into the database:
        (1) Enrich with metadata
        """
        logging.debug("Reached transform_data() method of the ParkingCleaner class")

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


class RawDataCleaner:
    """
    Main orchestrator:
    - consumes raw messages from Kafka topics
    - decides which dataset-specific cleaning processor to use
    - processes the message using that cleaner
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
            # 'parking',
            # 'pollution',
            # 'road_traffic',
            'social_events',
            # 'weather',
            ]
        self.consumer.subscribe(self.topics)
        self._start_cleaning()


    def _start_cleaning(self):
        try:
            while True:
                message = self.consumer.poll(1.0)
                if message is None:
                    # print('message is None!')
                    continue
                if message.error():
                    logging.error(f"Error while consuming message: {message.error()}")
                else:
                    logging.debug(f"Consumed message with key {message.key()} and value {message.value()}")
                    self.clean_message(message)
        except KeyboardInterrupt:
            pass
        finally:
            self.consumer.close()

    def clean_message(self, message):
        """
        Dynamically selects and instantiates the correct cleaner.
        """
        logging.info("Reached function clean_message()")

        topic = message.topic()
        logging.debug(f"Cleaning message from topic: {topic}")

        try:
            # processor_class_name = self.topic_config[topic]["processor_class"]
            cleaner_config = self.json_config[topic]
            cleaner_class_name = cleaner_config["cleaner_class"]

        except KeyError as e:
            logging.error(f"Failed to get cleaner class for topic {topic}. Error: {e}")
            return  # Return early if there's an error
        
        # Dynamically instantiate the correct processor using its class name
        cleaner_class = globals()[cleaner_class_name]
        cleaner = cleaner_class(topic, self.json_config)
        
        incoming_message = message.value() # incoming_message: bytes

        data = cleaner.transform_data(incoming_message)
    
        # logging.debug(f"Data: {data}")
        # logging.debug(f"Type(Data): {type(data)}")

        publish_message(self.producer, f"clean_{topic}", cleaner.serialize_data(data))
        logging.debug(f"Serialized Data: {cleaner.serialize_data(data)}")

        self.persist_data_in_mysql(cleaner, data)

    def persist_data_in_mysql(self, cleaner, message):

        logging.info("Reached persist_data_in_mysql method")
        conn = connect_to_mysql()
        cursor = conn.cursor()
        
        # preprocessed_message, html_field = processor.preprocess_for_db(message)
        query_data = cleaner.get_sql_query(message)

        # Check if there's an additional update query and html index for the data to insert
        if len(query_data) == 5:
            insert_query, data_to_insert, update_query, html_field, primary_key_idx = query_data
            logging.debug(f"Insert Query received in persist_data_in_mysql: {insert_query}")
            logging.debug(f"data_to_insert received in persist_data_in_mysql: {data_to_insert}")

            logging.debug(f"Update Query received in persist_data_in_mysql: {update_query}")
            cursor.execute(insert_query, data_to_insert)
            # Assuming there's an HTML field attribute in the cleaner
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
    logging.debug('Start cleaning..')
    cleaner = RawDataCleaner()