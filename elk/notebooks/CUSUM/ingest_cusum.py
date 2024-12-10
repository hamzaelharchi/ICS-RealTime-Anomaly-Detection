from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import json

# Initialize Spark
sc = SparkContext('local')
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

# Kafka topic and bootstrap servers
TOPIC_NAME = 'cusum'  # Topic for anomaly results
BOOTSTRAP_SERVERS = "kafka:29092"

# Read from Kafka with Structured Streaming
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# Convert key and value to strings
df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Function to print rows read from Kafka to the console
def process_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        pandas_df = batch_df.select("value").toPandas()

        # Print each row read
        for _, row in pandas_df.iterrows():
            print(f"Received row: {row['value']}")

# Write stream using foreachBatch and print rows
query = df1 \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

# Keep the streaming query running
query.awaitTermination()
