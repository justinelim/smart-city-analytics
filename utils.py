import os
import mysql.connector
from dotenv import load_dotenv
import re
import json
import datetime
from typing import Tuple
import logging
import csv
from decimal import Decimal
import datetime

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
    if isinstance(obj, datetime.datetime):
        return obj.strftime('%Y-%m-%d %H:%M:%S')
    elif isinstance(obj, Decimal):
        return float(obj) 
    # Add other type checks and conversions if necessary
    return obj

def remove_html_for_sql_parsing(message: str, html_field_idx: int) -> Tuple[tuple, str]:
    """
    Removing HTML content to deal with commas that interfere with 
    splitting the message for SQL insertion
    """
        
    html_pattern = re.compile('<(p|h1|h2|strong)[^>]*>.*<\/(p|h1|h2|strong)>\n?|nan', re.S)
    html_content = re.search(html_pattern, message)

    if html_content:
        html_content = html_content.group(0)
        logging.debug(f"HTML_CONTENT FOUND: {html_content}")

    else:
        html_content = ''  # Default value if no HTML content is found
        logging.debug(f"HTML_CONTENT *NOT* FOUND: {html_content}")
        
    # Remove the HTML field from the string
    html_free_message = message.replace(html_content, '')
    # Reconstruct the original Python object i.e. tuple from the modified string
    # Split the message string into a list of fields
    logging.debug(f"HTML_FREE_MESSAGE: {html_free_message}")

    # message_list = html_free_message.split(',')
    message_list = list(csv.reader([html_free_message]))[0]
    logging.debug(f"MESSAGE_LIST: {message_list}")

    # html_free_message_tuple = ast.literal_eval(html_free_message)
    modified_message = message_list[:html_field_idx] + ['<<HTML_CONTENT>>'] + message_list[html_field_idx+1:]
    logging.debug(f"MESSAGE AFTER HTML SUBSTITUTION: {modified_message}")
    modified_message = tuple(modified_message)
    logging.debug(f"LENGTH OF MODIFIED_MESSAGE: {len(modified_message)}")
    return modified_message, html_content

def convert_unix_timestamp(data: list, idx: int) -> list:
    """
        :data: incoming message or a list of fields
        :int:
        """
    fields = list(data)  # Convert data to a list

    # fields = data
    # fields = deserialize_kafka_message_bytes(incoming_message)  # fields: list
    try:
        dt = datetime.datetime.fromtimestamp(int(fields[idx]))
    except ValueError:
        # Account for the case where the value in event_name is comma-separated (e.g. Rasmus Skov Borring, soloklaver)
        concatenated_value = fields[2] + fields[3]
        fields = fields[:2] + [concatenated_value] + fields[4:]
        dt = datetime.datetime.fromtimestamp(int(fields[idx]))

    fields[idx] = dt.strftime("%Y-%m-%d %H:%M:%S")

    return fields

def publish_message(producer, topic: str, message: list):
    # Publish the processed message to a Kafka topic
    # serialized_message = ', '.join(str(field) for field in message)
    # print('MESSAGE_TO_TOPIC', serialized_message)
    # else:
    #     serialized_message = message
    producer.produce(topic=topic, value=message)
    producer.flush()