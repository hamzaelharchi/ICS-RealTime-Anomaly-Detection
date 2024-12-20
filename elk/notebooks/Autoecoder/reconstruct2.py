from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, udf, array
import numpy as np
import pickle
from kafka import KafkaProducer
import json
import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import pandas as pd
import joblib
from pyspark.sql.functions import to_json, struct
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit,row_number
from pyspark.context import SparkContext
from datetime import datetime
 
 
# Initialize Spark
sc = SparkContext('local')
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")
 
  



  # Define the format of the original timestamp
original_format = '%d/%m/%Y %I:%M:%S %p'



 
# Kafka topic and bootstrap servers
TOPIC_NAME = 'swat'
BOOTSTRAP_SERVERS = "kafka:29092"
OUTPUT_TOPIC_NAME = 'reconstructed-ae2' 


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8'))
 
# Define schema
schema = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("FIT101", StringType(), True),
    StructField("LIT101", StringType(), True),
    StructField("MV101", StringType(), True),
    StructField("P101", StringType(), True),
    StructField("P102", StringType(), True),
    StructField("AIT201", StringType(), True),
    StructField("AIT202", StringType(), True),
    StructField("AIT203", StringType(), True),
    StructField("FIT201", StringType(), True),
    StructField("MV201", StringType(), True),
    StructField("P201", StringType(), True),
    StructField("P202", StringType(), True),
    StructField("P203", StringType(), True),
    StructField("P204", StringType(), True),
    StructField("P205", StringType(), True),
    StructField("P206", StringType(), True),
    StructField("DPIT301", StringType(), True),
    StructField("FIT301", StringType(), True),
    StructField("LIT301", StringType(), True),
    StructField("MV301", StringType(), True),
    StructField("MV302", StringType(), True),
    StructField("MV303", StringType(), True),
    StructField("MV304", StringType(), True),
    StructField("P301", StringType(), True),
    StructField("P302", StringType(), True),
    StructField("AIT401", StringType(), True),
    StructField("AIT402", StringType(), True),
    StructField("FIT401", StringType(), True),
    StructField("LIT401", StringType(), True),
    StructField("P401", StringType(), True),
    StructField("P402", StringType(), True),
    StructField("P403", StringType(), True),
    StructField("P404", StringType(), True),
    StructField("UV401", StringType(), True),
    StructField("AIT501", StringType(), True),
    StructField("AIT502", StringType(), True),
    StructField("AIT503", StringType(), True),
    StructField("AIT504", StringType(), True),
    StructField("FIT501", StringType(), True),
    StructField("FIT502", StringType(), True),
    StructField("FIT503", StringType(), True),
    StructField("FIT504", StringType(), True),
    StructField("P501", StringType(), True),
    StructField("P502", StringType(), True),
    StructField("PIT501", StringType(), True),
    StructField("PIT502", StringType(), True),
    StructField("PIT503", StringType(), True),
    StructField("FIT601", StringType(), True),
    StructField("P601", StringType(), True),
    StructField("P602", StringType(), True),
    StructField("P603", StringType(), True),
    StructField("Normal/Attack", StringType(), True)
])
 
# Read from Kafka
 
inputStream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()
 
# Parse the JSON data
inputStream = inputStream.withColumn("value", inputStream["value"].cast(StringType()))
inputStream = inputStream.withColumn("data", from_json(col("value"), schema)).select("data.*")
schema_mapping = {"Timestamp": "string", "FIT101": "double", "LIT101": "double", "MV101": "double", "P101": "double", "P102": "double",
                  "AIT201": "double", "AIT202": "double", "AIT203": "double", "FIT201": "double", "MV201": "double", "P201": "double",
                  "P202": "double", "P203": "double", "P204": "double", "P205": "double", "P206": "double", "DPIT301": "double", "FIT301": "double",
                  "LIT301": "double", "MV301": "double", "MV302": "double", "MV303": "double", "MV304": "double", "P301": "double", "P302": "double",
                  "AIT401": "double", "AIT402": "double", "FIT401": "double", "LIT401": "double", "P401": "double", "P402": "double", "P403": "double",
                  "P404": "double", "UV401": "double", "AIT501": "double", "AIT502": "double", "AIT503": "double", "AIT504": "double", "FIT501": "double",
                  "FIT502": "double", "FIT503": "double", "FIT504": "double", "P501": "double", "P502": "double", "PIT501": "double", "PIT502": "double",
                  "PIT503": "double", "FIT601": "double", "P601": "double", "P602": "double", "P603": "double", "Normal/Attack": "string"}
 
for col_name, col_type in schema_mapping.items():
    inputStream = inputStream.withColumn(col_name, inputStream[col_name].cast(col_type))

# Load the scaler object
scaler = joblib.load('work/Autoecoder/transformers/scaler.pickle')
# Actuator names for dummy encoding
actuators_NAMES = ['P101', 'P102', 'P201', 'P202', 'P204', 'P205', 'P206', 'MV301',
                   'MV303', 'MV304', 'P301', 'P401', 'P403', 'P404', 'P502', 'P601', 'P602', 'P603']


def dataEngineering(normal_data, actuators_NAMES):
    # Keep a copy of the Timestamp column
    timestamps = normal_data['Timestamp']
    
    # Drop the Timestamp column temporarily for preprocessing
    normal_data = normal_data.drop(columns=['Timestamp'])
    
    # Remove the last column (assumed to be the 'Normal/Attack' column)
    normal_data = normal_data.iloc[:, :-1]
    
    to_drop = ['P402', 'P203', 'FIT501', 'FIT504', 'P501', 'FIT502', 'AIT502',
               'FIT201', 'MV101', 'PIT501', 'PIT503', 'AIT504', 'MV201',
               'MV302', 'FIT503', 'P302', 'FIT301', 'UV401']
    
    # Drop unwanted columns
    normal_data.drop(columns=to_drop, inplace=True)
    
    # Filter actuator names still in the dataset
    actuators_NAMES = [col for col in actuators_NAMES if col in normal_data.columns]
    
    # Separate sensors and actuators
    sensors = normal_data.drop(columns=actuators_NAMES)
    sens_cols = sensors.columns
    
    actuators = normal_data[actuators_NAMES]
    
    # Scale the sensor data
    sensors = scaler.transform(sensors)
    sensors = pd.DataFrame(sensors, columns=sens_cols)
    actuators_dummies = actuators.copy()
    for actuator in actuators_NAMES:
        actuators_dummies[actuator] = pd.Categorical(actuators_dummies[actuator], categories=[0, 1, 2])
        actuators_dummies = pd.get_dummies(actuators_dummies, columns=[actuator], dtype=int)
    # Ensure index consistency
    sensors.index = actuators_dummies.index
    
    # Concatenate sensors and actuators
    allData = pd.concat([sensors, actuators_dummies], axis=1)
    
    # Add the Timestamp back
  #  allData['Timestamp'] = timestamps.values
    timesteps = timestamps
    
    return timesteps, allData
 
 # Global buffer to store rows
BUFFER = []
BUFFER_SIZE =300
timestamps_buffer = []  # Store the corresponding timestamps
# Load the ONNX model
import onnxruntime as ort

model_path = "work/Autoecoder/BWmodel.onnx"
onnx_session = ort.InferenceSession(model_path)

# Input and output names for the ONNX model
input_name = onnx_session.get_inputs()[0].name
output_name = onnx_session.get_outputs()[0].name

# Function to process each batch and send to Kafka
def process_and_send_to_kafka(batch_df, batch_id):
    global BUFFER  # Access the global buffer
    global timestamps_buffer
    
    if not batch_df.isEmpty():  # Check if the batch is not empty
        print(f"Processing batch {batch_id} with {batch_df.count()} records.")
        
        # Convert to Pandas for easier processing
        pandas_df = batch_df.toPandas()
        
        # Apply data engineering pipeline
        timesteps, preprocessed_data = dataEngineering(pandas_df, actuators_NAMES)
        # Accumulate rows in the buffer
        print(len(timesteps))
        print(len(preprocessed_data))
        # Accumulate rows and timestamps
        for (_, row), timestamp in zip(preprocessed_data.iterrows() ,timesteps) :
            BUFFER.append(row)  # Append preprocessed row
            timestamps_buffer.append(timestamp)  # Append corresponding timestamp

            
            # Check if the buffer has 10 rows
            if len(BUFFER) == BUFFER_SIZE:
                # Convert buffer to NumPy array and reshape for the model
                batch_data = np.array(BUFFER, dtype=np.float32).reshape(1, BUFFER_SIZE, -1)
                
                print(f"Buffer full. Length of timestamps_buffer: {len(timestamps_buffer)}")
 
                
                print(f"Buffer full. Processing {BUFFER_SIZE} rows...")

                # Run the ONNX model
                try:
                    reconstructed = onnx_session.run([output_name], {input_name: batch_data})
                    print(f"Shape of ONNX model output: {reconstructed[0].shape}")
                    # Squeeze the output to remove singleton dimensions
                    # Explicitly reshape the flattened output 
                    num_rows = BUFFER_SIZE  # 10 rows
                    num_features = reconstructed[0].size // num_rows  # Calculate features per row
                
                    reconstructed_squeezed = np.squeeze(reconstructed[0])
                    print(f"Reshaped ONNX model output: {reconstructed_squeezed.shape}")
                    # Convert reconstructed output back to a DataFrame
                    reconstructed_df = pd.DataFrame(reconstructed_squeezed, columns=preprocessed_data.columns)
                    print(f"Number of rows in preprocessed_data: {len(preprocessed_data)}")
                    print(f"Number of rows in reconstructed_df: {len(reconstructed_df)}")
                    print(f"Number of elements in timesteps: {len(timestamps_buffer)}")
                    columns = preprocessed_data.columns

                    for original_row, (idx, reconstructed_row), time in zip(BUFFER, reconstructed_df.iterrows(), timestamps_buffer):
                        # Cast original_row to float32 if needed
                        original_row = original_row.astype(np.float32)  # Cast to float32
                        
                        # Convert NumPy array (original_row) to a dictionary
                        original_dict = {col: float(val) for col, val in zip(columns, original_row)}
                        
                        # Convert reconstructed_row (DataFrame row) to dictionary
                        reconstructed_dict = reconstructed_row.to_dict()
                        
                        # Debugging prints
                        print("Original Row (Dictionary):", original_dict)
                        print("Reconstructed Row (Dictionary):", reconstructed_dict)
                        print("Timestamp:", time)

                        # Combine the original timestamp with the reconstruction results
                        # combined_dict = {
                        #     "Timestamp": reconstructed_dict["Timestamp"]  # Attach Timestamp
                        # }
                        combined_dict={}
                        reconstruction_errors = []
                        for feature in original_dict.keys():
                            if feature != "Timestamp":
                                original_value = original_dict[feature]
                                reconstructed_value = reconstructed_dict[feature]
                                combined_dict[f"{feature}_original"] = original_value
                                combined_dict[f"{feature}_reconstructed"] = reconstructed_value
                                reconstruction_errors.append(original_value - reconstructed_value)
                        
                        reconstruction_error = np.mean(np.abs(reconstruction_errors))
                        combined_dict["anomalyFlagged"] = 1 if reconstruction_error > 0.11 else 0 
                        combined_dict["recosntruction_error"] = reconstruction_error
                        # Parse the original timestamp string to a datetime object
                        # Remove leading and trailing whitespace
                        time = time.strip()
                        timestamp_obj = datetime.strptime(time, original_format)

                        # Convert the datetime object to ISO 8601 format (UTC)

                        iso_format = timestamp_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
                        combined_dict["timestamp"] = iso_format
                        #Send to Kafka
                        print(combined_dict)
                        producer.send(
                            OUTPUT_TOPIC_NAME, 
                            value=json.dumps(combined_dict)
                        )
                        print("a Reconstructed datapt sent to Kafka.")


                except Exception as e:
                    print(f"Error during ONNX model inference: {e}")
                finally:
                    BUFFER = []
                    timestamps_buffer = []

                

# Process the streaming data
query = inputStream.writeStream \
    .foreachBatch(process_and_send_to_kafka) \
    .outputMode("update") \
    .start()

query.awaitTermination()