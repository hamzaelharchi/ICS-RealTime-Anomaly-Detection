import pandas as pd
import numpy as np
import json
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from kafka import KafkaProducer
import joblib

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
    .option("subscribe", 'swat') \
    .option("startingOffsets", "latest") \
    .load()

# Convert key and value to strings
df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Load pre-trained OneHotEncoder and MinMaxScaler
encoder = joblib.load("work/Preprocessing/onehot_encoder.pkl")  # Replace with your encoder file path
scaler = joblib.load("work/Preprocessing/minmax_scaler.pkl")    # Replace with your scaler file path

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
        return 0, (0, 0)  # No anomaly, no CUSUM

    reference_value, drift_threshold, decision_threshold = CUSUM_PARAMETERS[feature_name]

    if feature_name not in positive_cusum:
        positive_cusum[feature_name] = 0
        negative_cusum[feature_name] = 0

    delta = data_point - reference_value
    positive_cusum[feature_name] = max(0, positive_cusum[feature_name] + delta - drift_threshold)
    negative_cusum[feature_name] = min(0, negative_cusum[feature_name] + delta + drift_threshold)

    anomaly = 0
    if positive_cusum[feature_name] > decision_threshold or abs(negative_cusum[feature_name]) > decision_threshold:
        anomaly = 1

    return anomaly, (positive_cusum[feature_name], negative_cusum[feature_name])

def preprocess_data(pandas_df):
    """
    Preprocess data for streaming using encoder and scaler.
    Ensures that the Timestamp column is retained but not scaled.
    """
    # Identify the Timestamp column
    timestamp_col = "Timestamp"  # First column is the timestamp

    # Define categorical and numerical columns
    categorical_columns = ['MV101', 'P101', 'P102', 'MV201', 'P201',
                           'P202', 'P203', 'P204', 'P205', 'P206', 'MV301',
                           'MV302', 'MV303', 'MV304', 'P301', 'P302', 
                           'P401', 'P402', 'P403', 'P404', 'UV401', 'P501',
                           'P502', 'P601', 'P602', 'P603']

    numerical_columns = [col for col in pandas_df.columns if col not in categorical_columns + [timestamp_col]]

    # Extract the Timestamp column
    timestamp_data = pandas_df[[timestamp_col]]

    # Perform One-Hot Encoding
    encoded_features = encoder.transform(pandas_df[categorical_columns])
    encoded_df = pd.DataFrame(encoded_features, columns=encoder.get_feature_names_out(categorical_columns), index=pandas_df.index)

    # Perform Min-Max Scaling on numerical columns
    scaled_features = scaler.transform(pandas_df[numerical_columns])
    scaled_df = pd.DataFrame(scaled_features, columns=numerical_columns, index=pandas_df.index)

    # Combine scaled and encoded features along with the Timestamp column
    processed_data = pd.concat([timestamp_data, encoded_df, scaled_df], axis=1)

    return processed_data


def process_stream(batch_df, batch_id):
    """
    Process each batch of streaming data.
    """
    if not batch_df.isEmpty():
        pandas_df = batch_df.select("value").toPandas()

        # Parse the JSON values into a DataFrame
        data_points = pd.DataFrame([json.loads(row["value"]) for _, row in pandas_df.iterrows()])

        # Preprocess the data
        processed_data = preprocess_data(data_points)

        # List to hold CUSUM data for all features
        cusum_data = []

        for idx, row in processed_data.iterrows():
            anomaly_detected = False
            feature_cusums = {}

            # Iterate through each feature and detect anomalies
            for feature_name, value in row.items():
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
            cusum_data.append({
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
