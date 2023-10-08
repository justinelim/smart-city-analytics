import os
import mysql.connector
from dotenv import load_dotenv
import re
import json
import datetime


load_dotenv()

mysql_config = {
    'host': os.getenv("SQL_HOST"),
    'user': os.getenv("SQL_USERNAME"),
    'password': os.getenv("SQL_PASSWORD"),
    'database': os.getenv("SQL_DATABASE")
}

def connect_to_mysql():
    return mysql.connector.connect(**mysql_config)

def load_config(config_path):
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config

def handle_html(message: str, no_of_fields: int, html_field_idx: int):
    """
    Handle html field.
    :return modified_message: tuple
    """
    
    print("message before html_replace:", message)
    print("message before html_replace type:", type(message))
    
    html_pattern = re.compile('<(p|h1|h2)>.*<\/(p|h1|h2)>', re.S)
    html_field = re.search(html_pattern, message)

    if html_field:
        html_field = html_field.group(0)
    else:
        html_field = ''  # Default value if no HTML field is found
    
    # print('HTML_FIELD', html_field)
    # Remove the HTML field from the string
    html_free_message = message.replace(html_field, '')
    # Reconstruct the original Python object i.e. tuple from the modified string
    # Split the message string into a list of fields
    print("html_free_message:", html_free_message)

    message_list = html_free_message.split(',')
    # html_free_message_tuple = ast.literal_eval(html_free_message)
    modified_message = message_list[:html_field_idx] + ['<<HTML_FIELD>>'] + message_list[html_field_idx+1:]
    print("message after html replace:", modified_message)
    modified_message = tuple(modified_message)
    print('LEN_MODIFIED_MESSAGE', len(modified_message))
    return modified_message, html_field

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