import pandas as pd
import numpy as np
import json
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from kafka import KafkaProducer

# Initialize Spark
sc = SparkContext('local')
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

# Kafka topic and bootstrap servers
TOPIC_NAME = 'cusum'  # Topic to send CUSUM results
BOOTSTRAP_SERVERS = "kafka:29092"

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8')  # Serialize messages as UTF-8 encoded strings
)

# Read from Kafka with Structured Streaming
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", 'swat')  \
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
        return 0, 0  # No anomaly, no CUSUM

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
        return 1, (positive_cusum[feature_name], negative_cusum[feature_name])  # Anomaly detected
    return 0, (positive_cusum[feature_name], negative_cusum[feature_name])  # No anomaly

def process_stream(batch_df, batch_id):
    """
    Process each batch of streaming data.
    """
    if not batch_df.isEmpty():
        pandas_df = batch_df.select("value").toPandas()
        
        # List to hold CUSUM data for all features
        cusum_data = []
        
        for _, row in pandas_df.iterrows():
            # Parse the JSON data point
            data_point = json.loads(row["value"])

            # Exclude the first and last columns (timestamp and label)
            keys = list(data_point.keys())[1:-1]
            values = list(data_point.values())[1:-1]

            # Process each feature
            anomaly_detected = False
            feature_cusums = {}
            
            for feature_name, value in zip(keys, values):
                try:
                    anomaly, cusum_values = detect_anomaly(feature_name, float(value))
                    feature_cusums[feature_name] = {
                        "positive_cusum": cusum_values[0],
                        "negative_cusum": cusum_values[1],
                        "anomaly": anomaly
                    }
                    anomaly_detected = anomaly_detected or anomaly  # Flag if any anomaly detected
                except ValueError as e:
                    print(f"Skipping feature {feature_name} due to error: {e}")

            # Add timestamp and feature CUSUM data to the batch
            timestamp = data_point.get("timestamp", "unknown")
            cusum_data.append({
                "timestamp": timestamp,
                "features": feature_cusums,
                "anomaly_detected": anomaly_detected
            })

        # Send the CUSUM data for all features to the Kafka topic
        if cusum_data:
            batch_data = json.dumps(cusum_data)
            producer.send(TOPIC_NAME, key="cusum_batch", value=batch_data)
            print(f"Sent CUSUM results: {batch_data}")

# Write stream using foreachBatch and send CUSUM results
query = df1 \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(process_stream) \
    .start()

# Keep the streaming query running
query.awaitTermination()
