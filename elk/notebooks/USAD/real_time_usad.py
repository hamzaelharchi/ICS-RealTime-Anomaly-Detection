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

# Load the saved MinMaxScaler and encoder
encoder = joblib.load("work/Preprocessing/onehot_encoder.pkl")  # Replace with your encoder file path
scaler = joblib.load("work/Preprocessing/minmax_scaler.pkl")
pca = joblib.load("work/Preprocessing/pca_model_attack.pkl")
print("Scaler and Encoder loaded")


# Categorical columns for preprocessing
categorical_columns = ['MV101', 'P101', 'P102', 'MV201', 'P201',
                       'P202', 'P203', 'P204', 'P205', 'P206', 'MV301',
                       'MV302', 'MV303', 'MV304', 'P301', 'P302', 
                       'P401', 'P402', 'P403', 'P404', 'UV401', 'P501',
                       'P502', 'P601', 'P602', 'P603']


def preprocess_data(row):
    """
    Preprocess a single incoming data row.
    - Parse the row into a dictionary.
    - Separate numerical and categorical features.
    - Apply OneHotEncoding to categorical features.
    - Apply scaling to numerical features.
    """
    try:
        # Convert the row to a DataFrame
        row_df = pd.DataFrame([row])

        # Process categorical columns with OneHotEncoder
        categorical_data = row_df[categorical_columns]
        encoded_categorical_data = encoder.transform(categorical_data)
        
        # Process numerical columns
        numerical_columns = [col for col in row_df.columns if col not in categorical_columns and col != "Timestamp"]
        numerical_data = row_df[numerical_columns]
        scaled_numerical_data = scaler.transform(numerical_data)

        # Combine encoded categorical and scaled numerical data
        processed_data = pd.concat(
            [
                pd.DataFrame(scaled_numerical_data, columns=numerical_columns),
                pd.DataFrame(encoded_categorical_data, columns=encoder.get_feature_names_out(categorical_columns))
            ],
            axis=1
        )

        # print(processed_data) 

        # Apply PCA transformation to the processed data
        processed_data = pca.transform(processed_data)
        processed_data = pd.DataFrame(processed_data)

        # print(processed_data)


        return processed_data.values.flatten().tolist()
    except Exception as e:
        print(f"Error preprocessing row: {e}")
        return None

def reshape_input_data(global_buffer, window_size):
    """
    Reshapes the accumulated global buffer data into the format the model expects,
    i.e., [batch_size, window_size * feature_size].
    """
    df = pd.DataFrame(global_buffer)
    print(df.columns)

    if df.shape[0] < window_size:
        print(f"Not enough data to reshape. Current data size: {df.shape[0]}")
        return None

    df_last_window = df.iloc[-window_size:]
    reshaped_input = df_last_window.values.flatten().reshape(1, -1)

    return reshaped_input.astype(np.float32)

def process_batch(batch_df, batch_id):
    global global_buffer
    window_size = 10

    if not batch_df.isEmpty():
        pandas_df = batch_df.select("value").toPandas()

        for _, row in pandas_df.iterrows():
            new_row = json.loads(row["value"])
            processed_row = preprocess_data(new_row)
            print('processed row', len(processed_row))
            if processed_row is not None:
                global_buffer.append(processed_row)

        print(f'Global buffer contains {len(global_buffer)} rows')

        if len(global_buffer) > window_size:
            global_buffer = global_buffer[-window_size:]

        if len(global_buffer) >= window_size:
            reshaped_input = reshape_input_data(global_buffer, window_size)

            if reshaped_input is None:
                print(f"Skipping batch {batch_id} due to data issues")
                return

            print(f"Shape of input data for batch {batch_id}: {reshaped_input.shape}")

            encoder_output = encoder_session.run(None, {encoder_session.get_inputs()[0].name: reshaped_input})[0]
            decoder1_output = decoder1_session.run(None, {decoder1_session.get_inputs()[0].name: encoder_output})[0]
            decoder2_output = decoder2_session.run(None, {decoder2_session.get_inputs()[0].name: encoder_output})[0]

            error1 = np.mean(np.square(reshaped_input - decoder1_output), axis=1)
            error2 = np.mean(np.square(reshaped_input - decoder2_output), axis=1)

            threshold = 0.1
            anomalies1 = error1 > threshold
            anomalies2 = error2 > threshold
            combined_anomalies = anomalies1 | anomalies2

            print(f"Anomalies detected in batch {batch_id}: {np.sum(combined_anomalies)} anomalies")

query = df1 \
    .writeStream \
    .outputMode("append") \
    .format('console') \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
