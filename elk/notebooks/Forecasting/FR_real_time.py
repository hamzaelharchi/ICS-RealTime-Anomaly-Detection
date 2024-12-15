## Load pre-trained model and scaler
# def load_model_and_scaler():
#     try:
#         # Try loading with joblib first
#         model = joblib.load("work/Forecasting/swat_model_joblib.pkl")
#         scaler = joblib.load("work/Forecasting/scaler.pkl")
#     except:
#         # Fallback to pickle
#         with open("swat_model_pickle.pkl", "rb") as f:
#             model = pickle.load(f)
#         with open("scaler.pkl", "rb") as f:
#             scaler = pickle.load(f)
    
#     return model, scaler

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import joblib
import pickle
import json
import numpy as np
import pandas as pd
from kafka import KafkaProducer
import time
import traceback

# Initialize Spark
sc = SparkContext('local')
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

# Kafka topic and bootstrap servers
TOPIC_NAME = 'swat'
OUTPUT_TOPIC_NAME = 'swat-forecast'
BOOTSTRAP_SERVERS = "kafka:29092"

# Load pre-trained model and scaler
def load_model_and_scaler():
    try:
        # Try loading with joblib first
        model = joblib.load("work/Forecasting/swat_model_joblib.pkl")
        scaler = joblib.load("work/Forecasting/scaler.pkl")
    except:
        # Fallback to pickle
        with open("swat_model_pickle.pkl", "rb") as f:
            model = pickle.load(f)
        with open("scaler.pkl", "rb") as f:
            scaler = pickle.load(f)
    
    return model, scaler

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8')
)

# Load model and scaler
model, scaler = load_model_and_scaler()

# Read from Kafka with Structured Streaming
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON data from Kafka
df_parsed = df.selectExpr("CAST(value AS STRING) as value")

# Function to process each micro-batch
def process_batch(batch_df, batch_id):
    try:
        # Convert Spark DataFrame to Pandas
        rows = batch_df.toPandas()
        
        # Parse JSON for each row
        parsed_rows = rows['value'].apply(json.loads)
        
        # Prepare data for processing
        processed_rows = []
        timestamps = []
        for row_data in parsed_rows:
            # Remove 'Normal/Attack' key if present
            processed_row = {k: v for k, v in row_data.items() if k != 'Normal/Attack'}
            
            # Handle potential timestamp variations
            timestamp = processed_row.get('Timestamp') or processed_row.get('timestamp', '')
            timestamps.append(timestamp)
            
            # Remove timestamp from numeric processing
            if 'Timestamp' in processed_row:
                del processed_row['Timestamp']
            elif 'timestamp' in processed_row:
                del processed_row['timestamp']
            
            processed_rows.append(processed_row)
        
        # Convert to DataFrame
        data_rows = pd.DataFrame(processed_rows)
        
        # Ensure numeric columns (matching the training data columns)
        numeric_columns = [
            'FIT101', 'LIT101', 'MV101', 'P101', 'P102', 'AIT201', 'AIT202', 'AIT203', 
            'FIT201', 'MV201', 'P201', 'P202', 'P203', 'P204', 'P205', 'P206', 
            'DPIT301', 'FIT301', 'LIT301', 'MV301', 'MV302', 'MV303', 'MV304', 
            'P301', 'P302', 'AIT401', 'AIT402', 'FIT401', 'LIT401', 'P401', 'P402', 
            'P403', 'P404', 'UV401', 'AIT501', 'AIT502', 'AIT503', 'AIT504', 
            'FIT501', 'FIT502', 'FIT503', 'FIT504', 'P501', 'P502', 'PIT501', 
            'PIT502', 'PIT503', 'FIT601', 'P601', 'P602', 'P603'
        ]
        
        
        # Select and order columns consistently
        df_numeric = data_rows[numeric_columns]
        
        # Convert to numeric (in case of any string representations)
        df_numeric = df_numeric.apply(pd.to_numeric, errors='coerce').fillna(0)
        
        # Scale the data using the pre-trained scaler
        df_scaled = scaler.transform(df_numeric)
        
        # Predict using the RandomForest model
        predictions = model.predict(df_scaled)
        
        # Create a DataFrame with predictions
        pred_df = pd.DataFrame(predictions, columns=numeric_columns, index=df_numeric.index)
        
        
        # Prepare output data
        for i, (timestamp, row) in enumerate(zip(timestamps, df_numeric.to_dict('records'),
                                                             )):
            # Reconstruct the original output format
            output_data = row.copy()
            output_data['Timestamp'] = timestamp
            
            key = f"row-{i}"
            value = json.dumps(output_data)
            
            print(f"Sending row {i}", value)
            producer.send(OUTPUT_TOPIC_NAME, key=key, value=value)
            time.sleep(0.1)  # Small delay to avoid overwhelming Kafka
    
    except Exception as e:
        print(f"Error processing batch: {e}")
        traceback.print_exc()

# Write stream using foreachBatch
query = df_parsed \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(process_batch) \
    .start()

# Keep the streaming query running
query.awaitTermination()