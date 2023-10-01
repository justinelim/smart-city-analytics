import os
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

mysql_config = {
    'host': os.getenv("SQL_HOST"),
    'user': os.getenv("SQL_USERNAME"),
    'password': os.getenv("SQL_PASSWORD"),
    'database': os.getenv("SQL_DATABASE")
}

def connect_to_mysql():
    return mysql.connector.connect(**mysql_config)

def deserialize_kafka_message_bytes(incoming_message: bytes) -> list:
    # if isinstance(incoming_message, bytes):
    incoming_message_str = str(incoming_message.decode('utf-8'))
    return incoming_message_str.split(',')

def deserialize_kafka_message_str(message: str) -> tuple:
    fields = message.split(',')
    return (int(fields[0]), fields[1], int(fields[2]), int(fields[3]), fields[4], fields[5], fields[6], int(fields[7]), fields[8], fields[9], float(fields[10]), float(fields[11]))
# parking: 0 vehicle_count, 1 update_time, 2 _id, 3 total_spaces, 4 garage_code, 5 stream_time, 6 city, 7 postal_code, 8 street, 9 house_number, 10 latitude, 11 longitude

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