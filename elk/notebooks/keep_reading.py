from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# Initialize Spark
sc = SparkContext('local')
spark = SparkSession(sc)

# Kafka topic and bootstrap servers
TOPIC_NAME = 'swat'
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

# Write the output to the console
query = df1 \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Keep the streaming query running
query.awaitTermination()
