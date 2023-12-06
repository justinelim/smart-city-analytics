import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import os
from dotenv import load_dotenv
from utils import create_pool, load_config, publish_message, \
    read_offset, \
    update_offset
import logging
from src.data_cleaning.data_persistence import DataPersistence
from src.data_cleaning.cleaner_factory import get_cleaner


load_dotenv()

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

"""
Consume messages from raw Kafka stream, clean and write to clean_ Kafka topic and 
store/persist cleaned data in MySQL db.
"""

class DataCleaningCoordinator:
    """
    Orchestrates the cleaning process for Kafka messages:
    - Consumes raw messages from Kafka topics.
    - Utilizes a pre-determined cleaner instance to process messages.
    - Publishes cleaned data back to Kafka.
    - Coordinates with DataPersistence to store cleaned data in MySQL database.
    """
    def __init__(self, pool, config, topic, cleaner, data_persistence) -> None:
        self.kafka_config = {
            'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            'group.id': 'consumer_group_id',
            'enable.auto.commit': 'false',  # Disable auto commit
            'auto.offset.reset': 'earliest',
        }
        self.pool = pool
        self.config = config
        self.topic = topic
        self.cleaner = cleaner
        self.data_persistence = data_persistence


    async def start_cleaning(self):
        consumer = AIOKafkaConsumer(
            self.topic,
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
                processed = await self.clean_message(message, consumer, producer)
                if processed:
                    await consumer.commit()  # Manually commit the offset
        # except Exception as e:
        #     logging.error(f"Error in start_cleaning: {e}")

        finally:
            logging.info("Stopping the producer")
            await producer.stop()
            logging.info("Producer stopped")
            await consumer.stop()

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

        topic = message.topic
        offset_table_name = "cleaned_offsets"
        partition = message.partition
        logging.debug(f"Cleaning message from topic: {topic}")
        current_offset = await read_offset(self.pool, topic, partition, offset_table_name)
        if message.offset <= current_offset:
            return False # Skip processing this message
        
        try:
            cleaner_name = self.config[message.topic]['cleaner_class']
            logging.debug(f"### Cleaner name: {cleaner_name}")

            self.cleaner = get_cleaner(cleaner_name, message.topic, self.config, self.pool)


        except KeyError as e:
            logging.error(f"Failed to get cleaner class for topic {topic}. Error: {e}")
            return  # Return early if there's an error
        
        incoming_message = message.value # incoming_message: bytes

        cleaned_data = await self.cleaner.transform_data(incoming_message)
    
        logging.debug(f"Data: {cleaned_data}")
        logging.debug(f"Type(Data): {type(cleaned_data)}")

        await publish_message(producer, f"clean_{topic}", self.cleaner.serialize_data(cleaned_data))
        logging.debug(f"Serialized Data: {self.cleaner.serialize_data(cleaned_data)}")

        await self.data_persistence.persist_data_in_mysql(self.cleaner, cleaned_data)

        # Update the offset in the database after processing
        new_offset = message.offset
        await update_offset(self.pool, topic, partition, new_offset, offset_table_name)
        
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
        cleaner_instance = get_cleaner(config[dataset_name]['cleaner_class'], dataset_name, config[dataset_name], pool)
        cleaning_coordinator = DataCleaningCoordinator(pool, config, dataset_name, cleaner_instance, data_persistence)
        tasks.append(asyncio.create_task(cleaning_coordinator.start_cleaning()))
    await asyncio.gather(*tasks)

    # Close each DataPersistence instance
    for task in tasks:
        await task.data_persistence.close_pool()

    # Close the pool after all tasks are done
    pool.close()
    await pool.wait_closed()