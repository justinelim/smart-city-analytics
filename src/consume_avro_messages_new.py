from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import DeserializingConsumer
from hybrid_deserializer import HybridDeserializer

# Configure the schema registry client
schema_registry_conf = {'url': 'http://schema-registry:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Value deserializer
value_avro_deserializer = AvroDeserializer(schema_registry_client)

# Set up the consumer configuration
consumer_conf = {
    'bootstrap.servers': 'broker:9092',
    'group.id': 'test10',
    'auto.offset.reset': 'earliest',
    'key.deserializer': HybridDeserializer(),  # Using the revised hybrid deserializer
    'value.deserializer': value_avro_deserializer
}

consumer = DeserializingConsumer(consumer_conf)
# consumer.subscribe(['pollution', 'cultural_events'])  # Subscribe to both topics
# consumer.subscribe(['cultural_events'])
# consumer.subscribe(['library_events'])
# consumer.subscribe(['parking_metadata'])
# consumer.subscribe(['social_events'])
consumer.subscribe(['weather'])

# Consume messages
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print(f"Received message: Key: {msg.key()}, Value: {msg.value()}")
except KeyboardInterrupt:
    print("Interrupted by user")
finally:
    consumer.close()
