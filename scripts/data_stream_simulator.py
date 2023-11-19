import os
import json
import asyncio
from confluent_kafka import Producer
from dotenv import load_dotenv

from utils import load_config, connect_to_mysql, handle_non_serializable_types, publish_message
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

kafka_config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'enable.idempotence': 'true',
}

producer = Producer(kafka_config)

async def read_offset_from_database(dataset_name, default_offset='0'):
    # Returns the last offset processed for the given dataset
    # Stores and reads the offset from a table.
    if dataset_name == "weather":
        # Set default timestamp for weather dataset before try-except
        default_offset = '1970-01-01 00:00:01'
    
    query = "SELECT last_offset FROM streamed_offsets WHERE dataset_name = %s"
    try:
        pool = await connect_to_mysql()    
        async with pool.acquire() as conn, conn.cursor() as cursor:
            await cursor.execute(query, (dataset_name,))
            result = await cursor.fetchone()
            return result[0] if result else default_offset
    except Exception as e:
        print(f"Error reading offset from database: {e}")
        return default_offset

async def update_offset_in_database(dataset_name, primary_key, new_offset):
    # Updates the last offset streamed for the given dataset
    # Stores the offset in the table 
    current_offset = await read_offset_from_database(dataset_name)
    try:
        pool = await connect_to_mysql()    
        
        async with pool.acquire() as conn, conn.cursor() as cursor:
            if current_offset in ('0', '1970-01-01 00:00:01'):
                query = """
                    INSERT INTO streamed_offsets (dataset_name, primary_key, last_offset)
                    VALUES (%s, %s, %s)
                """
                await cursor.execute(query, (dataset_name, primary_key, new_offset))
            else:
                query = """
                    UPDATE streamed_offsets
                    SET last_offset = %s
                    WHERE dataset_name = %s
                """
                await cursor.execute(query, (new_offset, dataset_name))
            await conn.commit()
    except Exception as e:
        logging.error(f"Error updating offset in database: {e}")

async def process_dataset(dataset_name, dataset_config):
    pool = await connect_to_mysql()    
        
    async with pool.acquire() as conn, conn.cursor() as cursor:

        # Read offset from database
        offset = await read_offset_from_database(dataset_name)
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
            await update_offset_in_database(dataset_name, primary_key, new_offset)

        await asyncio.sleep(1)

async def main():
    config = load_config(config_path='config.json')

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