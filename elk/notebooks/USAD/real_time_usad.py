import onnxruntime as ort
import numpy as np
import pandas as pd
import json
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
import joblib


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

# Global buffer to accumulate rows
global_buffer = []

# Load ONNX models using ONNX Runtime
encoder_session = ort.InferenceSession("work/USAD/usad_encoder.onnx")
decoder1_session = ort.InferenceSession("work/USAD/usad_decoder1.onnx")
decoder2_session = ort.InferenceSession("work/USAD/usad_decoder2.onnx")
print('Models loaded')

# Load the saved MinMaxScaler
scaler = joblib.load("work/USAD/minmax_scaler.pkl")
print("Scaler loaded")

def reshape_input_data(global_buffer, window_size):
    """
    Reshapes and scales the accumulated global buffer data into the format the model expects,
    i.e., [batch_size, window_size * feature_size].
    """
    # Convert the global buffer to a DataFrame for further processing
    df = pd.DataFrame(global_buffer)

    # Assuming the first column is the timestamp and the last column is the label
    df = df.iloc[:, 1:-1]  # Exclude the first (timestamp) and last (label) columns

    # Ensure there's enough data to form a valid window
    if df.shape[0] < window_size:
        print(f"Not enough data to reshape. Current data size: {df.shape[0]}")
        return None  # Return None if there's insufficient data

    # Select the last `window_size` rows
    df_last_window = df.iloc[-window_size:]

    # Reshape the last `window_size` rows into [1, window_size * feature_size]
    reshaped_input = df_last_window.values.flatten().reshape(1, -1)

    # Apply the scaler to normalize the data
    scaled_input = scaler.transform(reshaped_input)
    # scaled_input = reshaped_input # !!!! change when minmaxscaler loading is working

    return scaled_input.astype(np.float32)

def process_batch(batch_df, batch_id):
    global global_buffer
    window_size = 10
    
    # Convert the current batch to Pandas
    if not batch_df.isEmpty():
        # Parse and accumulate the rows in the global buffer
        pandas_df = batch_df.select("value").toPandas()
        new_rows = [json.loads(row["value"]) for _, row in pandas_df.iterrows()]
        global_buffer.extend(new_rows)

        print(f'Global buffer contains {len(global_buffer)} rows')
        # print(global_buffer)
        # Always keep the last `window_size` rows from the buffer
        if len(global_buffer) > window_size:
            print('Keeping buffer size.')
            global_buffer = global_buffer[-window_size:]  # Keep only the last `window_size` rows

        # Check if the buffer has enough rows for processing
        if len(global_buffer) >= window_size:
            reshaped_input = reshape_input_data(global_buffer, window_size)
            # If reshaped_input is None (not enough data), skip this batch
            if reshaped_input is None:
                print(f"Not enough data for reshaping in batch {batch_id}")
                return  # Skip this batch

            print(f"Shape of input data for batch {batch_id}: {reshaped_input.shape}")
            # Inference on the encoder (latent representation)
            encoder_input_name = encoder_session.get_inputs()[0].name
            encoder_output = encoder_session.run(None, {encoder_input_name: reshaped_input})[0]

            # Inference on the decoder1 (reconstructed input)
            decoder1_input_name = decoder1_session.get_inputs()[0].name
            decoder1_output = decoder1_session.run(None, {decoder1_input_name: encoder_output})[0]

            # Inference on the decoder2 (reconstructed input from encoder output)
            decoder2_input_name = decoder2_session.get_inputs()[0].name
            decoder2_output = decoder2_session.run(None, {decoder2_input_name: encoder_output})[0]

            # Calculate reconstruction error (you can adjust the metric for anomaly detection)
            error1 = np.mean(np.square(reshaped_input - decoder1_output), axis=1)
            error2 = np.mean(np.square(reshaped_input - decoder2_output), axis=1)

            # Detect anomalies based on error threshold (you can set a threshold based on your use case)
            threshold = 0.1  # Adjust based on your specific needs
            anomalies1 = error1 > threshold
            anomalies2 = error2 > threshold

            # Combine the anomalies detected by both decoders
            combined_anomalies = anomalies1 | anomalies2

            # Log or handle the detected anomalies
            print(f"Anomalies detected in batch {batch_id}: {np.sum(combined_anomalies)} anomalies")


# Write stream using foreachBatch
query = df1 \
    .writeStream \
    .outputMode("append") \
    .format('console') \
    .foreachBatch(process_batch) \
    .start()

# Keep the streaming query running
query.awaitTermination()
