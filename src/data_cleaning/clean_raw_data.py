import sys
from confluent_kafka import Consumer, Producer
import datetime
import re
import ast
import os
from dotenv import load_dotenv
load_dotenv()
# sys.path.append('/Users/justinelim/Documents/GitHub/smart-city-analytics')
from scripts.utils import connect_to_mysql

config = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    'group.id': 'consumer_group_id',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'true',
    'auto.commit.interval.ms': '5000',
}

consumer = Consumer(config)
producer = Producer(config)

topics = [
    # 'cultural_events',  # done (convert Unix timestamp field to human-readable timestamp, ran into a lot of trouble because of the HTML & event_type field values that were comma-separated)
    # 'library_events',  # done (standardize column names in MySQL, required extra processing of HTML field & concatenation issue when persisting in MySQL similar to that faced with cultural_events)
    'parking',  # done (enrich with metadata)
    # 'pollution',  # done
    # 'road_traffic',  # done
    # 'social_events',  # done
    # 'weather',  # done
    ]
consumer.subscribe(topics)

def deserialize_message(incoming_message: bytes) -> list:
    # if isinstance(incoming_message, bytes):
    incoming_message_str = str(incoming_message.decode('utf-8'))
    return incoming_message_str.split(',')
    
def convert_unix_timestamp(incoming_message: bytes, idx: int) -> list:
    fields = deserialize_message(incoming_message)  # fields: list
    try:
        dt = datetime.datetime.fromtimestamp(int(fields[idx]))
    except ValueError:
        # Account for the case where the value in event_name is comma-separated (e.g. Rasmus Skov Borring, soloklaver)
        concatenated_value = fields[2] + fields[3]
        fields = fields[:2] + [concatenated_value] + fields[4:]
        dt = datetime.datetime.fromtimestamp(int(fields[idx]))

    fields[idx] = dt.strftime("%Y-%m-%d %H:%M:%S")

    return fields

def enrich_parking_data(incoming_message, idx):
    fields = deserialize_message(incoming_message)
    print('FIELDS', fields)

    primary_key = fields[idx]  # The primary key column `garagecode` is at index 4 in the incoming message
    print('PRIMARY_KEY', primary_key)

    conn = connect_to_mysql()
    cursor = conn.cursor()
    query = f"SELECT * FROM smart_city.parking_metadata WHERE garagecode = %s"
    
    cursor.execute(query, (primary_key,))
    row = cursor.fetchone()

    if row is not None:
        city = row[1]
        postal_code = row[2]
        street = row[3]
        house_number = row[4]
        latitude = row[5]
        longitude = row[6]

        # Add columns from `parking_metadata` to the incoming_message
        fields.extend([city, postal_code, street, house_number, latitude, longitude])
        enriched_message = tuple(fields)
        return enriched_message
    else:
        return incoming_message
    
    cursor.close()
    conn.close()

def publish_message(message: list, topic: str):
    # Publish the processed message to a Kafka topic
    # if not isinstance(message, bytes):
    serialized_message = ', '.join(str(field) for field in message)
    print('MESSAGE_TO_TOPIC', serialized_message)
    # else:
    #     serialized_message = message
    producer.produce(topic=f'clean_{topic}', value=serialized_message)
    producer.flush()

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

def persist_message_in_mysql(message: list, topic):
    # Persist the processed message in a MySQL database
    conn = connect_to_mysql()
    cursor = conn.cursor()

    if topic == 'cultural_events':
        no_of_fields = 19
        # html_pattern = re.compile('<(p|h1|h2)>.*<\/(p|h1|h2)>')
        # message_str = message.decode()  # Convert the byte string `message` to a regular string
        message_str = ', '.join(message)  # Re-create the message string from the message list
        modified_message, placeholders, html_field = handle_html_field(message_str, 9)
        
        if len(modified_message) > no_of_fields:
            # Concatenate the last columns from index 18 onwards
            last_field = ','.join(modified_message[18:])
            print('LAST_FIELD', last_field)
            # Create a modified message with the concatenated field at index 18
            modified_message = modified_message[:18] + (last_field,)
            placeholders = ', '.join(["%s"] * len(modified_message))  # Update the number of placeholders
        print('MOD_MSG', modified_message)

        query = f"INSERT INTO clean_{topic} VALUES ({placeholders})"
        print('QUERY', query)
        cursor.execute(query, modified_message)  # Exclude the HTML field from the modified_message
        conn.commit()

        # Insert the HTML field separately
        html_query = f"UPDATE clean_{topic} SET event_description = %s WHERE organizer_id = %s"  # Assuming there's an 'id' column
        print('HTML_FIELD', html_field)
        print('ORGANIZER_ID', message[-4])
        cursor.execute(html_query, (html_field, modified_message[-4]))  # Pass the actual HTML field and primary key organizer_id
    
    elif topic == 'library_events':   
        no_of_fields = 20
        message_str = ', '.join(message)  # Re-create the message string from the message list
        modified_message, placeholders, html_field = handle_html_field(message_str, 7)

        if len(modified_message) > no_of_fields:
            # Create a modified message with the values of the field from index 11 to the index before index -8 concatenated
            concatenated_value = ''.join(modified_message[11:-8])

            # Create a new tuple with the modified values
            modified_message = modified_message[:11] + (concatenated_value,) + modified_message[-8:]
            placeholders = ', '.join(["%s"] * len(modified_message))  # Update the number of placeholders

        print('MODIFIED_MESSAGE', modified_message)
        query = f"INSERT INTO clean_{topic} VALUES ({placeholders})"
        print('QUERY', query)
        cursor.execute(query, modified_message)  # Exclude the HTML field from the modified_message
        conn.commit()

        # Insert the HTML field separately
        html_query = f"UPDATE clean_{topic} SET content = %s WHERE id = %s"  # Assuming there's an 'id' column
        cursor.execute(html_query, (html_field, modified_message[-2]))  # Pass the actual HTML field and message primary key index

    else:
        placeholders = ', '.join(["%s"] * len(message))
        query = f"INSERT INTO clean_{topic} VALUES ({placeholders})"
        cursor.execute(query, message)

    conn.commit()
    cursor.close()
    conn.close()


def process_message(message):
    print('MESSAGE_TYPE', type(message))
    incoming_message = message.value()  # incoming_message: bytes
    print('INCOMING_MESSAGE', incoming_message)

    if message.topic() == 'cultural_events':
        print('RAW_INCOMING_TYPE', type(incoming_message))
        incoming_message = convert_unix_timestamp(incoming_message, 5)
    elif message.topic() == 'parking':
        incoming_message = enrich_parking_data(incoming_message, 4)
    else:
        incoming_message = deserialize_message(incoming_message)

    print('PREPROCESSED_MESSAGE', incoming_message)
    publish_message(incoming_message, message.topic())
    persist_message_in_mysql(incoming_message, message.topic())
    

while True:
    messages = consumer.consume(num_messages=10, timeout=1.0)
    for message in messages:
        if message is None:
            continue
        if message.error():
            print(f"Consumer error: {message.error()}")
            continue

        # Process the received message
        process_message(message)

    consumer.commit()

# if __name__ == '__main__':
#     test = '124,2014-05-22 09:39:02,13,130,BUSGADEHUSET,2014-11-03 16:18:44'
#     print(enrich_parking_data(test))