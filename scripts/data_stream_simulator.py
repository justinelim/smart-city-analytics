import os
import json
import asyncio
from aiokafka import AIOKafkaProducer
import time

from dotenv import load_dotenv

from utils import load_config, connect_to_mysql, \
    read_offset, update_offset, update_offset_within_transaction, \
    handle_non_serializable_types, publish_message
import logging


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()


async def process_dataset(producer, dataset_name, dataset_config, pool):
    start_time = time.time()
    logging.info(f"Starting processing for dataset: {dataset_name}")
    offset_table_name = 'streamed_offsets'
    partition = 0  # Assuming single partition
    primary_key = dataset_config['primary_key']

    async with pool.acquire() as conn, conn.cursor() as cursor:
        while True:
            offset = await read_offset(pool, dataset_name, partition, offset_table_name)
            logging.debug(f"Streamed Offset: {offset} for topic {dataset_name}")
            
            # Use a parameterized query
            query = f"SELECT * FROM {dataset_name} WHERE {primary_key} > %s ORDER BY {primary_key} LIMIT 1"
            await cursor.execute(query, (offset,))
            record = await cursor.fetchone()
            # logging.debug(f"Fetching record with offset: {offset}")
            # await cursor.execute(query, (offset,))
            # record = await cursor.fetchone()
            # logging.debug(f"Fetched record: {record}")


            if not record:
                logging.debug("No more records to fetch.")
                break

            try:
                await conn.begin()  # Start transaction for the record
                serialized_record = tuple(map(handle_non_serializable_types, record))
                serialized_json = json.dumps(serialized_record)
                
                # Send to Kafka
                await producer.send_and_wait(dataset_name, serialized_json.encode('utf-8'))

                # Update offset
                new_offset = record[dataset_config['raw_field_names'].index(primary_key)]
                # await update_offset(pool, dataset_name, partition, new_offset, offset_table_name)
                logging.debug(f"Attempting to update offset to: {new_offset}")

                await update_offset(pool, dataset_name, partition, new_offset, offset_table_name)
                # await update_offset_within_transaction(conn, cursor, dataset_name, partition, new_offset, offset_table_name)

                await conn.commit()  # Commit transaction after successful processing and offset update
            except Exception as e:
                await conn.rollback()  # Rollback the transaction in case of an error
                logging.error(f"Failed to publish message: {e}. Table: {dataset_name}")
                continue  # Skip the current record and continue with the next

            end_time = time.time()
            elapsed_time = end_time - start_time
            logging.info(f"Completed stream simulation for dataset: {dataset_name}. Time taken: {elapsed_time} seconds")


async def main(started_event):
    producer = AIOKafkaProducer(enable_idempotence=True,
                                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
    await producer.start()

    config = load_config(config_path='config.json')
    started_event.set()
    tasks = []
    pool = await connect_to_mysql()

    try:
        for dataset_name in config:
            if dataset_name == "parking_metadata":
                continue
            dataset_config = config[dataset_name]
            task = asyncio.create_task(process_dataset(producer, dataset_name, dataset_config, pool))
            tasks.append(task)

        await asyncio.gather(*tasks)
    finally:
        if pool:
            pool.close()
            await pool.wait_closed()
        await producer.stop()