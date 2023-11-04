import os
import mysql.connector
from dotenv import load_dotenv
import re
import json
import datetime as dt
from typing import Tuple
import logging
import csv
from decimal import Decimal
# from datetime import datetime
import pytz


load_dotenv()
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

mysql_config = {
    'host': os.getenv("SQL_HOST"),
    'user': os.getenv("SQL_USERNAME"),
    'password': os.getenv("SQL_PASSWORD"),
    'database': os.getenv("SQL_DATABASE")
}

# Create a Connection Pool
conn_pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool",
                                                        pool_size=10,
                                                        **mysql_config)
def connect_to_mysql():
    return conn_pool.get_connection()

def load_config(config_path):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config

def handle_non_serializable_types(obj):
    """
    Convert non-serializable types (like datetime and Decimal) 
    to serializable types.
    """
    try:
        if isinstance(obj, dt.datetime):
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
        :data: incoming message or a list of fields
        :int:
        """
    fields = list(data)  # Convert data to a list

    # fields = data
    # fields = deserialize_kafka_message_bytes(incoming_message)  # fields: list
    try:
        dt = dt.datetime.fromtimestamp(int(fields[idx]))
    except ValueError:
        # Account for the case where the value in event_name is comma-separated (e.g. Rasmus Skov Borring, soloklaver)
        concatenated_value = fields[2] + fields[3]
        fields = fields[:2] + [concatenated_value] + fields[4:]
        dt = dt.datetime.fromtimestamp(int(fields[idx]))

    fields[idx] = dt.strftime("%Y-%m-%d %H:%M:%S")

    return fields

def publish_message(producer, topic: str, message: list):
    producer.produce(topic=topic, value=message)
    producer.flush()


def format_date(date, timezone='Asia/Shanghai'):
    date_formats = [
        "%a, %d %b %Y %H:%M:%S %z",
        "%a %d %b %Y %H:%M:%S %z"
    ]
    
    if isinstance(date, str):
        for date_format in date_formats:
            try:
                date = dt.strptime(date, date_format)
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
