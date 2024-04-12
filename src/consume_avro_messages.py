from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka import KafkaError


consumer = AvroConsumer({    
    'bootstrap.servers': 'broker:9092',
    'group.id': 'test',
    'schema.registry.url': 'http://schema-registry:8081',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['cultural_events_topic'])

try:
    while True:
        try:
            msg = consumer.poll(10)
        except SerializerError as e:
            print("Message deserialization failed for {}: {}".format(msg, e))
            break
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        
        # Process the message
        print(msg.value())

except KeyboardInterrupt:
    pass

finally:
    consumer.close()