import os
import json
import asyncio
from aiokafka import AIOKafkaProducer

from dotenv import load_dotenv

from utils import load_config, connect_to_mysql, \
    read_offset, update_offset, \
    handle_non_serializable_types, publish_message
import logging


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()


async def process_dataset(producer, dataset_name, dataset_config, pool):
    offset_table_name = 'streamed_offsets'
    partition = 0  # Assuming single partition
    primary_key = dataset_config['primary_key']

    async with pool.acquire() as conn, conn.cursor() as cursor:
        # Read offset from database
        offset = await read_offset(pool, dataset_name, partition, offset_table_name)
        logging.debug(f"Streamed Offset: {offset} for topic {dataset_name}")
        
        # Use a parameterized query
        query = f"SELECT * FROM {dataset_name} ORDER BY {primary_key} LIMIT 1000 OFFSET %s"
        logging.debug(f"### Query to run: {query}")
        await cursor.execute(query, (offset,))
        records = await cursor.fetchall()
        logging.debug(f"Number of records fetched: {len(records)}")

        for record in records:
            serialized_record = tuple(map(handle_non_serializable_types, record))
            # logging.debug(f"Serialized record: {serialized_record} for table {dataset_name}")
            serialized_json = json.dumps(serialized_record)
            logging.debug(f"Serialized json: {serialized_json} for table {dataset_name}")

            try:
                await producer.send_and_wait(dataset_name, serialized_json.encode('utf-8'))
            except Exception as e:
                logging.error(f"Failed to publish message: {e}. Table: {dataset_name}")
                break
            except KeyboardInterrupt:
                # Graceful shutdown: commit offsets, close connections, etc.
                logging.info("Shutting down gracefully...")
                await producer.stop()
                if pool:
                    pool.close()
                    await pool.wait_closed()
                logging.info("Shutdown complete.")
        
        if records:
            # Update offset only if records were fetched and processed
            last_record = records[-1]
            new_offset = last_record[dataset_config['raw_field_names'].index(primary_key)]
            await update_offset(pool, dataset_name, partition, new_offset, offset_table_name)

async def main(started_event):
    producer = AIOKafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"))
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