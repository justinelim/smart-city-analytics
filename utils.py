import os
from dotenv import load_dotenv
import re
import json
import datetime
from typing import Tuple
import logging
from decimal import Decimal
# from datetime import datetime
import pytz
import aiomysql

load_dotenv()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

mysql_config = {
    'host': os.getenv("SQL_HOST"),
    'user': os.getenv("SQL_USERNAME"),
    'password': os.getenv("SQL_PASSWORD"),
    'db': os.getenv("SQL_DATABASE")
}


async def create_pool(): # async context manager for the pool
    return await aiomysql.create_pool(minsize=1, maxsize=10, **mysql_config)

async def connect_to_mysql():
    pool = await create_pool()
    # The connection is acquired and should be used within an async with statement
    return pool

def load_config(config_path):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config

async def read_primary_key_offset(pool, dataset_name, offset_table_name, default_offset='0'):
    # Returns the last offset processed (e.g. streamed or cleaned) for the given dataset
    # Stores and reads the offset from a table.
    if dataset_name == "weather":
        # Set default timestamp for weather dataset before try-except
        default_offset = '1970-01-01 00:00:01'
    
    query = f"SELECT last_offset FROM {offset_table_name} WHERE dataset_name = %s"
    try:
        async with pool.acquire() as conn, conn.cursor() as cursor:
            await cursor.execute(query, (dataset_name,))
            result = await cursor.fetchone()
            return result[0] if result else default_offset
    except Exception as e:
        print(f"Error reading offset from database: {e}")
        return default_offset

async def update_primary_key_offset(pool, dataset_name, offset_table_name, primary_key, new_offset):
    # Updates the last offset processed (e.g. streamed or cleaned) for the given dataset
    # Stores the offset in the table 
    current_offset = await read_primary_key_offset(pool, dataset_name, offset_table_name)
    try:        
        async with pool.acquire() as conn, conn.cursor() as cursor:
            if current_offset in ('0', '1970-01-01 00:00:01'):
                query = f"""
                    INSERT INTO {offset_table_name} (dataset_name, primary_key, last_offset)
                    VALUES (%s, %s, %s)
                """
                await cursor.execute(query, (dataset_name, primary_key, new_offset))
            else:
                query = f"""
                    UPDATE {offset_table_name}
                    SET last_offset = %s
                    WHERE topic = %s
                """
                await cursor.execute(query, (new_offset, dataset_name))
            await conn.commit()
    except Exception as e:
        logging.error(f"Error updating offset in database: {e}")
    
async def read_offset(pool, topic, partition, offset_table_name, default_offset=0):
    # Returns the last Kafka offset processed for the given topic and partition
    query = f"SELECT last_offset FROM {offset_table_name} WHERE topic = %s AND `partition` = %s"
    try:
        async with pool.acquire() as conn, conn.cursor() as cursor:
            await cursor.execute(query, (topic, partition))
            result = await cursor.fetchone()
            return result[0] if result else default_offset
    except Exception as e:
        print(f"Error reading offset from database: {e}")
        return default_offset


async def update_offset(pool, topic, partition, new_offset, offset_table_name):
    query = f"""
        INSERT INTO {offset_table_name} (topic, `partition`, last_offset)
        VALUES (%s, %s, %s) AS new_values
        ON DUPLICATE KEY UPDATE
        last_offset = new_values.last_offset
    """
    try:
        async with pool.acquire() as conn, conn.cursor() as cursor:
            await cursor.execute(query, (topic, partition, new_offset))
            await conn.commit()
    except Exception as e:
        logging.error(f"Error updating offset in database: {e}")

async def update_offset_within_transaction(conn, cursor, topic, partition, new_offset, offset_table_name):
    query = f"""
        INSERT INTO {offset_table_name} (topic, `partition`, last_offset)
        VALUES (%s, %s, %s) AS new_values
        ON DUPLICATE KEY UPDATE
        last_offset = new_values.last_offset
    """
    try:
        await cursor.execute(query, (topic, partition, new_offset))
    except Exception as e:
        logging.error(f"Error updating offset in database: {e}")
        raise  # Re-raise the exception to handle it in the calling function


def handle_non_serializable_types(obj):
    """
    Convert non-serializable types (like datetime and Decimal) 
    to serializable types.
    """
    try:
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, Decimal):
            return float(obj)
        # Add other type checks and conversions if necessary
        return obj
    except Exception as e:
        logging.error(f"Error handling non-serializable types: {e}")
        return None  # or handle the error as needed


def convert_unix_timestamp(data: list, idx: int) -> list:
    """
    Convert a UNIX timestamp to a human-readable date-time string.

    :param data: List of data fields, one of which is a UNIX timestamp.
    :param idx: Index of the field in the list which is the UNIX timestamp.
    :return: Modified list with the timestamp converted to a human-readable string.
    """
    fields = list(data)  # Convert data to a list

    try:
        timestamp = datetime.datetime.fromtimestamp(int(fields[idx]))  # Convert timestamp to datetime object
    except ValueError:
        # Handling specific case if the value at index is not a valid timestamp
        # E.g. case where the value in event_name is comma-separated (e.g. "Rasmus Skov Borring, soloklaver")
        concatenated_value = fields[2] + fields[3]
        fields = fields[:2] + [concatenated_value] + fields[4:]
        timestamp = datetime.datetime.fromtimestamp(int(fields[idx]))

    fields[idx] = timestamp.strftime("%Y-%m-%d %H:%M:%S")  # Convert datetime object to string

    return fields

def publish_message_stream(producer, topic: str, message: list):
    producer.produce(topic=topic, value=message)
    producer.flush()

async def publish_message(producer, topic: str, message: list):
    # Send the message asynchronously
    await producer.send(topic, value=message)
    # Wait for all messages to be sent
    await producer.flush()

def format_date(date, timezone='Asia/Shanghai'):
    date_formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a %d %b %Y %H:%M:%S %z"
    ]
    
    if isinstance(date, str):
        for date_format in date_formats:
            try:
                date = datetime.strptime(date, date_format)
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Time data {date!r} does not match any valid format")
    
    tz = pytz.timezone(timezone)
    if date.tzinfo is None or date.tzinfo.utcoffset(date) is None:
        date = tz.localize(date)
    
    utc_date = date.astimezone(pytz.UTC)
    return utc_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
