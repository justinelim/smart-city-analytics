import os
import json
import asyncio
from confluent_kafka import Producer
from dotenv import load_dotenv

from utils import load_config, connect_to_mysql, \
    read_offset_from_database, update_offset_in_database, \
    handle_non_serializable_types, publish_message
import logging

started_event = asyncio.Event()

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

kafka_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'enable.idempotence': 'true',
}

producer = Producer(kafka_config)

async def process_dataset(dataset_name, dataset_config):
    pool = await connect_to_mysql()    
    offset_table_name = 'streamed_offsets'

    async with pool.acquire() as conn, conn.cursor() as cursor:

        # Read offset from database
        offset = await read_offset_from_database(pool, dataset_name, offset_table_name)
        primary_key = dataset_config['primary_key']
        
        # Use a parameterized query
        logging.debug(f"Offset: {offset} for table {dataset_name}")
        query = f"SELECT * FROM {dataset_name} WHERE {primary_key} > %s ORDER BY {primary_key}"
        logging.debug(f"### Query to run: {query}")
        await cursor.execute(query, (offset,))  # Pass the offset as a parameter
        records = await cursor.fetchall()
        logging.debug(f"Number of records fetched: {len(records)}")


        for record in records:
            # logging.debug(f"Record: {record}")
            serialized_record = tuple(map(handle_non_serializable_types, record))
            # logging.debug(f"Serialized record: {serialized_record} for table {dataset_name}")
            serialized_json = json.dumps(serialized_record)
            logging.debug(f"Serialized json: {serialized_json} for table {dataset_name}")

            # publish_message(producer, dataset_name, serialized_json)
            # logging.debug(f"Attempting to publish record: {serialized_json} to topic {dataset_name}")
            try:
                publish_message(producer, dataset_name, serialized_json)
            except Exception as e:
                logging.error(f"Failed to publish message: {e}. Table: {dataset_name}")

            new_offset = record[dataset_config['raw_field_names'].index(primary_key)]
            await update_offset_in_database(pool, dataset_name, offset_table_name, primary_key, new_offset)

        await asyncio.sleep(1)

async def main():
    config = load_config(config_path='config.json')
    started_event.set()
    tasks = []
    for dataset_name in config:
        if dataset_name == "parking_metadata":
            continue
        dataset_config = config[dataset_name]
        task = asyncio.create_task(process_dataset(dataset_name, dataset_config))
        tasks.append(task)

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())