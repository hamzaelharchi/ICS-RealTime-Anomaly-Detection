from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import onnxruntime as ort
import pickle
import json
import numpy as np
import pandas as pd
from kafka import KafkaProducer
import time
import traceback
from collections import deque

# Initialize Spark
sc = SparkContext('local')
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")

# Kafka configuration
TOPIC_NAME = 'swat'
OUTPUT_TOPIC_NAME = 'swat-forecast'
BOOTSTRAP_SERVERS = "kafka:29092"

# Model configuration
SEQ_LENGTH = 10
DATA_BUFFER_SIZE = SEQ_LENGTH  # Match buffer size to sequence length

# Global buffer for accumulating data points
data_buffer = {
    'full_data': deque(maxlen=DATA_BUFFER_SIZE),
    'numeric_data': deque(maxlen=DATA_BUFFER_SIZE),
    'timestamps': deque(maxlen=DATA_BUFFER_SIZE)
}

# Load ONNX model and scaler
def load_model_and_scaler():
    try:
        # Load ONNX model
        model = ort.InferenceSession("work/Forecasting/swat_model.onnx", 
                                   providers=['CPUExecutionProvider'])
        # Load scaler
        with open("work/Forecasting/scaler.pkl", "rb") as f:
            scaler = pickle.load(f)
        return model, scaler
    except Exception as e:
        print(f"Error loading model and scaler: {e}")
        raise

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8')
)

# Load model and scaler
model, scaler = load_model_and_scaler()

# Numeric columns to process (matching training data)
NUMERIC_COLUMNS = [
    'FIT101', 'LIT101', 'MV101', 'P101', 'P102', 'AIT201', 'AIT202', 'AIT203', 
    'FIT201', 'MV201', 'P201', 'P202', 'P203', 'P204', 'P205', 'P206', 
    'DPIT301', 'FIT301', 'LIT301', 'MV301', 'MV302', 'MV303', 'MV304', 
    'P301', 'P302', 'AIT401', 'AIT402', 'FIT401', 'LIT401', 'P401', 'P402', 
    'P403', 'P404', 'UV401', 'AIT501', 'AIT502', 'AIT503', 'AIT504', 
    'FIT501', 'FIT502', 'FIT503', 'FIT504', 'P501', 'P502', 'PIT501', 
    'PIT502', 'PIT503', 'FIT601', 'P601', 'P602', 'P603'
]

def prepare_sequences(data):
    """Convert buffer data to sequence format for ONNX model"""
    sequences = np.array([data])  # Shape: (1, seq_length, n_features)
    return sequences.astype(np.float32)

def make_prediction(model, sequence, scaler):
    """Make prediction using ONNX model"""
    # Prepare input for ONNX inference
    model_input = {
        'input': sequence
    }
    
    # Run inference
    predictions_normalized = model.run(None, model_input)[0]
    
    # Denormalize predictions
    predictions_denormalized = scaler.inverse_transform(predictions_normalized)
    return predictions_denormalized[0]  # Return first (and only) prediction

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
        
        for row_data in parsed_rows:
            # Remove 'Normal/Attack' column if present
            row_data.pop('Normal/Attack', None)
            
            # Prepare numeric row
            numeric_row = {k: v for k, v in row_data.items() if k in NUMERIC_COLUMNS}
            
            # Order columns consistently
            ordered_row = [numeric_row[col] for col in NUMERIC_COLUMNS]
            
            # Add to buffers
            data_buffer['full_data'].append(row_data)
            data_buffer['numeric_data'].append(ordered_row)
            
            # Check if buffer is full
            if len(data_buffer['numeric_data']) == SEQ_LENGTH:
                # Convert buffer to numpy array
                sequence_data = np.array(list(data_buffer['numeric_data']))
                
                # Scale the sequence
                sequence_scaled = scaler.transform(sequence_data)
                
                # Prepare sequence for ONNX model
                model_input = prepare_sequences(sequence_scaled)
                
                # Make prediction
                prediction = make_prediction(model, model_input, scaler)
                
                # Prepare output data (use the latest full data as template)
                output_data = data_buffer['full_data'][-1].copy()
                
                # Update numeric values with predictions
                for i, col in enumerate(NUMERIC_COLUMNS):
                    output_data[col] = float(prediction[i])
                
                # Send prediction
                key = f"prediction-{batch_id}"
                value = json.dumps(output_data)
                
                print(f"Sending prediction", value)
                producer.send(OUTPUT_TOPIC_NAME, key=key, value=value)
                
                # Remove oldest entry to slide the window
                data_buffer['numeric_data'].popleft()
                data_buffer['full_data'].popleft()
    
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