from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

# Create Kafka Client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# Define new topics
for topic in ['eli_athlete_event_results', 'eli_athlete_event_output']:
    topic_name = f'{topic}'
    num_partitions = 2
    replication_factor = 1

    new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

    # Create new topics
    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Check list of existing topics
[print(topic) for topic in admin_client.list_topics() if "eli" in topic]

# Close connection with client
admin_client.close()