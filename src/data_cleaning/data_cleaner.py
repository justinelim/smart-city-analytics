import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import os
from dotenv import load_dotenv
from utils import create_pool, connect_to_mysql, load_config, publish_message, \
    handle_non_serializable_types, \
    read_offset_from_database, \
    update_offset_in_database
import logging
import json
from data_persistence import DataPersistence


load_dotenv()

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

"""
Consume messages from raw Kafka stream, clean and write to clean_ Kafka topic and 
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

    def __init__(self, kafka_topic, json_config, pool):
        self.kafka_topic = kafka_topic
        self.json_config = json_config
        self.pool = pool
        self.raw_field_names = self.json_config["raw_field_names"]

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


class DataCleaningCoordinator:
    """
    Orchestrates the cleaning process for Kafka messages:
    - Consumes raw messages from Kafka topics.
    - Utilizes a pre-determined cleaner instance to process messages.
    - Publishes cleaned data back to Kafka.
    - Coordinates with DataPersistence to store cleaned data in MySQL database.
    """
    def __init__(self, pool, config, cleaner, data_persistence) -> None:
        self.kafka_config = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            'group.id': 'consumer_group_id',
            'enable.auto.commit': 'false',  # Disable auto commit
            'auto.offset.reset': 'earliest',
        }
        self.config = config
        self.pool = pool
        self.cleaner = cleaner  # Cleaner instance
        self.data_persistence = data_persistence

    async def start_cleaning(self):
        consumer = AIOKafkaConsumer(
            *self.topics,  # List of topics to subscribe to
            bootstrap_servers=self.kafka_config['bootstrap.servers'],
            group_id=self.kafka_config['group.id'],
            enable_auto_commit=False,  # Important for manual offset control
            isolation_level='read_committed',  # Ensure EOS
        )
        producer = AIOKafkaProducer(bootstrap_servers=self.kafka_config['bootstrap.servers'])

        # Start the consumer and producer
        await consumer.start()
        await producer.start()

        try:
            async for message in consumer:
                processed = await self.clean_message(message, producer, consumer)
                if processed:
                    await consumer.commit()  # Manually commit the offset
        finally:
            await consumer.stop()
            await producer.stop()

    async def clean_message(self, message, consumer, producer):
        """
        Processes a given message using the assigned cleaner instance:
        - Checks if the message has already been processed to maintain idempotency.
        - Uses the cleaner instance to transform the message data.
        - Publishes the cleaned data to a specified Kafka topic.
        - Persists the cleaned data in the MySQL database.
        - Updates the offset in the database and commits the offset in Kafka.

        :param message: The message to be processed, typically from a Kafka topic.
        :param consumer: The Kafka consumer instance.
        :param producer: The Kafka producer instance for publishing cleaned data.
        :return: A boolean indicating whether the message was processed.
        """
        logging.info("Reached function clean_message()")

        topic = message.topic()
        offset_table_name = "cleaned_offsets"
        logging.debug(f"Cleaning message from topic: {topic}")
        current_offset = await read_offset_from_database(self.pool, topic, offset_table_name)
        if message.offset() <= current_offset:
            return False # Skip processing this message
        
        try:
            # processor_class_name = self.topic_config[topic]["processor_class"]
            cleaner_config = self.json_config[topic]
            cleaner_class_name = cleaner_config["cleaner_class"]

        except KeyError as e:
            logging.error(f"Failed to get cleaner class for topic {topic}. Error: {e}")
            return  # Return early if there's an error
        
        incoming_message = message.value() # incoming_message: bytes

        cleaned_data = self.cleaner.transform_data(incoming_message)
    
        # logging.debug(f"Data: {data}")
        # logging.debug(f"Type(Data): {type(data)}")

        await publish_message(producer, f"clean_{topic}", self.cleaner.serialize_data(cleaned_data))
        logging.debug(f"Serialized Data: {self.cleaner.serialize_data(cleaned_data)}")

        await self.data_persistence.persist_data_in_mysql(self.cleaner, cleaned_data)


        # Update the offset in the database after processing
        await update_offset_in_database(message.topic(), message.key(), message.offset())
        
        # Manually commit the offset in Kafka
        await consumer.commit()

        return True


# Entry point for the asyncio event loop
async def main():
    """
    Event loop starter, which kicks off the coordination process.
    Signals to DataCleaningCoordinator to start cleaning.
    """
    config = load_config(config_path="config.json")
    pool = await create_pool()
    data_persistence = DataPersistence(pool)

    tasks = []
    for dataset_name in config:
        if dataset_name == "parking_metadata":
            continue
        cleaner_class = globals()[config[dataset_name]['cleaner_class']]
        cleaner_instance = cleaner_class(dataset_name, config[dataset_name], pool) # Pass the pool to each cleaner instance
        cleaning_coordinator = DataCleaningCoordinator(pool, config, cleaner_instance, data_persistence)
        tasks.append(asyncio.create_task(cleaning_coordinator.clean()))
    await asyncio.gather(*tasks)

    # Close the pool after all tasks are done
    pool.close()
    await pool.wait_closed()
    

# Run the event loop
if __name__ == '__main__':
    asyncio.run(main())