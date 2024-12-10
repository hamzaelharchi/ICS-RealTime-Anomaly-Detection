import pandas as pd
import numpy as np
import json
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# Initialize Spark
sc = SparkContext('local')
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

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

# Load CUSUM parameters from reference file
def load_cusum_parameters(reference_file):
    data = pd.read_csv(reference_file)
    return {
        row["Feature"]: (row["ReferenceValue"], row["DriftThreshold"], row["DecisionThreshold"])
        for _, row in data.iterrows()
    }

CUSUM_PARAMETERS = load_cusum_parameters("work/CUSUM/reference_params.csv")


# Initialize CUSUM values
positive_cusum = {}
negative_cusum = {}

def detect_anomaly(feature_name, data_point):
    """
    Applies CUSUM on a single data point for real-time anomaly detection.
    """
    global positive_cusum, negative_cusum

    if feature_name not in CUSUM_PARAMETERS:
        print(f"No CUSUM parameters found for feature: {feature_name}")
        return 0

    reference_value, drift_threshold, decision_threshold = CUSUM_PARAMETERS[feature_name]

    # Initialize CUSUM values for new features
    if feature_name not in positive_cusum:
        positive_cusum[feature_name] = 0
        negative_cusum[feature_name] = 0

    # Calculate the positive and negative CUSUM
    positive_cusum[feature_name] = max(0, positive_cusum[feature_name] + data_point - (reference_value + drift_threshold))
    negative_cusum[feature_name] = min(0, negative_cusum[feature_name] + data_point - (reference_value - drift_threshold))

    # Detect anomaly
    if positive_cusum[feature_name] > decision_threshold or abs(negative_cusum[feature_name]) > decision_threshold:
        # Reset CUSUM after detecting an anomaly
        positive_cusum[feature_name] = 0
        negative_cusum[feature_name] = 0
        return 1  # Anomaly detected
    return 0  # No anomaly

def process_stream(batch_df, batch_id):
    """
    Process each batch of streaming data.
    """
    if not batch_df.isEmpty():
        pandas_df = batch_df.select("value").toPandas()
        for _, row in pandas_df.iterrows():
            # Parse the JSON data point
            data_point = json.loads(row["value"])
            
            # Exclude the first and last columns (timestamp and label)
            keys = list(data_point.keys())[1:-1]
            values = list(data_point.values())[1:-1]
            
            # Process each feature
            for feature_name, value in zip(keys, values):
                try:
                    anomaly = detect_anomaly(feature_name, float(value))
                    print(f"Feature: {feature_name}, Anomaly: {anomaly}")
                except ValueError as e:
                    print(f"Skipping feature {feature_name} due to error: {e}")

# Write stream using foreachBatch
query = df1 \
    .writeStream \
    .outputMode("update") \
    .format('console') \
    .foreachBatch(process_stream) \
    .start()

# Keep the streaming query running
query.awaitTermination()
