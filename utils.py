import os
import mysql.connector
from dotenv import load_dotenv
import re
import json

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

def handle_html_field(message: str, html_field_idx: int):
    html_pattern = re.compile('<(p|h1|h2)>.*<\/(p|h1|h2)>', re.S)
    html_field = re.search(html_pattern, message)

    if html_field:
        html_field = html_field.group(0)
    else:
        html_field = ''  # Default value if no HTML field is found
    
    print('HTML_FIELD', html_field)
    # Remove the HTML field from the string
    html_free_message = message.replace(html_field, '')
    # Reconstruct the original Python object i.e. tuple from the modified string
    # Split the message string into a list of fields
    message_list = html_free_message.split(',')
    # html_free_message_tuple = ast.literal_eval(html_free_message)
    modified_message = message_list[:html_field_idx] + ['<<HTML_FIELD>>'] + message_list[html_field_idx+1:]
    modified_message = tuple(modified_message)
    placeholders = ', '.join(["%s"] * len(modified_message))
    print('LEN_MODIFIED_MESSAGE', len(modified_message))
    
    return modified_message, placeholders, html_field

def publish_message(producer, topic: str, message: list):
    # Publish the processed message to a Kafka topic
    # serialized_message = ', '.join(str(field) for field in message)
    # print('MESSAGE_TO_TOPIC', serialized_message)
    # else:
    #     serialized_message = message
    producer.produce(topic=topic, value=message)
    # producer.flush()