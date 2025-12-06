from pyspark.sql import SparkSession
from configs import kafka_config, jdbc_config
import uuid
from pyspark.sql.functions import isnan, when, lit, from_json, col, avg, window, to_json, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import os

# Package to read Kafka from Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.13:4.0.1,org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 pyspark-shell'

# Create Spark session
spark = SparkSession.builder \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .appName("JDBCToKafka") \
    .getOrCreate()

# 1. Read data form SQL database (athlete_bio)
df_bio = spark.read.format('jdbc').options(
    url=jdbc_config["jdbc_url"],
    driver='com.mysql.cj.jdbc.Driver',  # com.mysql.jdbc.Driver
    dbtable="athlete_bio",
    user=jdbc_config["jdbc_user"],
    password=jdbc_config["jdbc_password"]) \
    .load()

# 2. Filter the data where height or weight is empty or nan
# Rewrite empty string to None value
df_pre_cleaned = (df_bio.withColumn("height", when(col("height") == "", lit(None)).otherwise(col("height")))
                  .withColumn("height", when(col("height") == "", lit(None)).otherwise(col("height"))))

# Create additional columns to convert value to double
df_cast = df_pre_cleaned.withColumn("height_numeric", col("height").try_cast("double")) \
            .withColumn("weight_numeric", col("weight").try_cast("double"))

# Filter the data from null or nan values
df_filtered = df_cast.filter(
    col("height_numeric").isNotNull() & ~isnan(col("height_numeric")) &
    col("weight_numeric").isNotNull() & ~isnan(col("weight_numeric"))
)

# Drop additional columns
df_bio_cleaned = df_filtered.drop("height_numeric", "weight_numeric", "country_noc")

# 3. Read data from kafka topic
# Create SparkSession
spark_2 = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

df_results_kafka = spark_2.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username={kafka_config['username']} password={kafka_config['password']};') \
    .option("subscribe", "eli_athlete_event_results") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "100") \
    .load()

# Create schema for Kafka data
schema = StructType([
    StructField("edition", StringType()),
    StructField("edition_id", FloatType()),
    StructField("country_noc", StringType()),
    StructField("sport", StringType()),
    StructField("event", StringType()),
    StructField("result_id", FloatType()),
    StructField("athlete", StringType()),
    StructField("athlete_id", FloatType()),
    StructField("pos", StringType()),
    StructField("medal", StringType()),
    StructField("isTeamSport", StringType()),
])

# Transform JSON format into dataframe
parsed_df = (df_results_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*").withColumn(
    "timestamp",
    current_timestamp()
))

# 4. Join data with result of competition from Kafka with bio data from SQL by athlete_id
joined_df = parsed_df.join(df_bio_cleaned, on="athlete_id", how="inner")

# 5. Find average height and weight for every sport type, medal type, sex and country_noc
df_agg = (joined_df.withWatermark("timestamp", "10 seconds")
    .select("sport", "medal", "sex", "country_noc", "height", "weight", "timestamp")
    .groupBy(window(col("timestamp"), "30 seconds", "10 seconds"), "sport", "medal", "sex", "country_noc").agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
    ).withColumn(
        "timestamp",
        current_timestamp())
    .drop("window"))

# 6. Create stream by using function forEachBatch
# Function to process every part of data
def foreach_batch_function(batch_df, batch_id):
    kafka_df = batch_df.select(
        # Pack the payload into the 'value' column as JSON
        to_json(struct(
            col("sport"),
            col("medal"),
            col("sex"),
            col("country_noc"),
            col("avg_height"),
            col("avg_weight"),
            col("timestamp")
        )).alias("value")).withColumn("key", lit(str(uuid.uuid4())))

    # 6.a Send data to Kafka
    kafka_df \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
        .option("topic", "eli_athlete_event_output") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username={kafka_config['username']} password={kafka_config['password']};') \
        .save()

    # 6.b Send data to MySQL
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_config["jdbc_url"]) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "results_eli") \
        .option("user", jdbc_config["jdbc_user"]) \
        .option("password", jdbc_config["jdbc_password"]) \
        .mode("append") \
        .save()

# Set streaming for process every batch by using function
df_agg \
    .writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start() \
    .awaitTermination()
