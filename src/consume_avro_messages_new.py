import json
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# Configuration for the Schema Registry
schema_registry_conf = {
    'url': 'http://schema-registry:8081'
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Avro Deserializer
value_deserializer = AvroDeserializer(schema_registry_client)

# Consumer configuration
consumer_conf = {
    'bootstrap.servers': 'broker:9092',
    'group.id': 'test',
    'auto.offset.reset': 'earliest',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': value_deserializer
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['cultural_events_topic'])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print("Received message: {}".format(msg.value()))
finally:
    consumer.close()
