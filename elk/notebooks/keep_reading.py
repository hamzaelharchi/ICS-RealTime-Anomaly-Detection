from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
import json
import pandas as pd 
import onnxruntime as ort
import numpy as np

#from tensorflow.keras.models import load_model

##################################


# Example: Mocking `splits` for demonstration
# Replace this with your actual data loading/preparation
splits = [[np.random.rand(10, 69) for _ in range(10)] for _ in range(10)]  # Mock data

# Load ONNX model
print("#" * 30)
ort_session = ort.InferenceSession("./model.onnx")

# Get model input details
input_details = ort_session.get_inputs()
input_name = input_details[0].name

# Extract the input data
window_with_batch = np.expand_dims(splits[9][0], axis=0)  # Shape: (1, 10, 69)

# Ensure the correct data type
window_with_batch = window_with_batch.astype(np.float32)

# Perform inference
outputs = ort_session.run(None, {input_name: window_with_batch})
print("Model output:", outputs)
print("#" * 30)

############################

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
#Loading the model
#autoencoder = load_model("withBatchNorm2_afterSplit_5.h5") 


def process_batch(batch_df, batch_id):
    global global_buffer

    # Convert the current batch to Pandas
    if not batch_df.isEmpty():
        pandas_df = batch_df.select("value").toPandas()

        # Parse the JSON strings into dictionaries
        new_rows = [json.loads(row["value"]) for _, row in pandas_df.iterrows()]

        # Add the new rows to the global buffer
        global_buffer.extend(new_rows)
        print(f'Global buffer contains {len(global_buffer)}')
        # Check if the buffer has 10 or more rows
        if len(global_buffer) >= 2:
            # Extract the first 10 rows for processing
            rows_to_process = global_buffer[:2]

            # Remove these rows from the buffer
            global_buffer = global_buffer[2:]

            # Convert to a DataFrame and pass to the autoencoder
            batch_df_to_process = pd.DataFrame(rows_to_process)
            # print(batch_df_to_process.head())
            
            # removing first and last columns
            autoencoder_input = batch_df_to_process.iloc[:, 1:-1].to_numpy()
            print(autoencoder_input)
            # autoencoder_output = autoencoder.predict(autoencoder_input)
            
            # Log or handle the output
            print("Processed batch with autoencoder. Output:")
            #print(autoencoder_output)

# Write stream using foreachBatch
query = df1 \
    .writeStream \
    .outputMode("append") \
    .format('console') \
    .foreachBatch(process_batch) \
    .start()

# Keep the streaming query running
query.awaitTermination()
