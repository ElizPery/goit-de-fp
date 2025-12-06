from kafka import KafkaConsumer
from configs import kafka_config
import json

# Create Kafka Consumer
consumer = KafkaConsumer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda v: v.decode('utf-8') if v else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_consumer_group_2'
)

# Name of the topic
topic_name = "eli_athlete_event_output"

# Subscription to topic
consumer.subscribe([topic_name])

print(f"Subscribed to topic '{topic_name}'")

# Processing messages from the topic
try:
    for message in consumer:
        print(f"Message: {message.value} from topic {message.topic}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    consumer.close()  # Close consumer