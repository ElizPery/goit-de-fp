from kafka import KafkaProducer
from configs import kafka_config, jdbc_config
import json
import uuid
import time
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafka") \
    .getOrCreate()

# 3. Read data form SQL database (athlete_event_results), write to kafka topic
# Read data form SQL database (athlete_event_results)
df_results = spark.read.format('jdbc').options(
    url=jdbc_config["jdbc_url"],
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable=jdbc_config["jdbc_table"],
    user=jdbc_config["jdbc_user"],
    password=jdbc_config["jdbc_password"]) \
    .load()

pandas_df = df_results.toPandas()
data_records = pandas_df.to_dict('records')

# Write data to Kafka topic
# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Name of the topic
my_name = "eli"
topic_name = f'{my_name}_athlete_event_results'

# Send data from the sensor in topic
for record in data_records:
    try:
        producer.send(topic_name, key=str(uuid.uuid4()), value=record)
        producer.flush()  # Waiting, until all messages would be sent
        print(f"Data from {record} sent to topic '{topic_name}' successfully.")
        time.sleep(1)
    except Exception as e:
        print(f"An error occurred: {e}")

producer.close()  # Close producer