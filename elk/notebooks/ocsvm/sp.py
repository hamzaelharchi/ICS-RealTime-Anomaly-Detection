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
 
 
 
# Initialize Spark
sc = SparkContext('local')
spark = SparkSession(sc)
spark.sparkContext.setLogLevel("ERROR")
 
 
# Initialize Spark
 
#spark = SparkSession.builder.master("local[*]").appName("Stream").getOrCreate()
 
# Kafka topic and bootstrap servers
TOPIC_NAME = 'swat'
BOOTSTRAP_SERVERS = "kafka:29092"
OUTPUT_TOPIC_NAME = 'swat-ocsvm'



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
scaler = joblib.load('work/transformers/scaler.pickle')
# Actuator names for dummy encoding
actuators_NAMES = ['P101', 'P102', 'P201', 'P202', 'P204', 'P205', 'P206', 'MV301',
                   'MV303', 'MV304', 'P301', 'P401', 'P403', 'P404', 'P502', 'P601', 'P602', 'P603']
 
def dataEngineering(normal_data,actuators_NAMES):
 
    normal_data.set_index('Timestamp', inplace=True)
 
    # Remove the last column
    normal_data = normal_data.iloc[:, :-1]
 
    to_drop = ['P402' ,'P203','FIT501','FIT504','P501' ,'FIT502','AIT502' ,'FIT201' ,'MV101','PIT501','PIT503' ,'AIT504' ,'MV201' ,'MV302' ,'FIT503','P302' ,'FIT301' ,'UV401']
 
           
    remained_cols= [i for i in normal_data.columns if i not in to_drop]
 
    # Drop highly correlated features
    normal_data.drop(columns=to_drop, inplace=True)
 
    # Filter actuator names that are still in the dataset
    actuators_NAMES = [col for col in actuators_NAMES if col in normal_data.columns]
 
    # Separate sensors and actuators
    sensors = normal_data.drop(columns=actuators_NAMES)
    sens_cols = sensors.columns
    print(len(sens_cols))
    actuators = normal_data[actuators_NAMES]
   
 
    sensors = scaler.transform(sensors)
 
    # Convert normalized data back to a DataFrame
    sensors = pd.DataFrame(sensors, columns=sens_cols)
    actuators_dummies = actuators.copy()
    for actuator in actuators_NAMES:
        actuators_dummies[actuator] = pd.Categorical(actuators_dummies[actuator], categories=[0, 1, 2])
        actuators_dummies = pd.get_dummies(actuators_dummies, columns=[actuator], dtype=int)
    # Ensure index consistency
    sensors.index = actuators_dummies.index
    # Concatenate sensors and actuators
    allData = pd.concat([sensors,actuators_dummies],axis=1)
    return allData
 
 
 
 
 

svm =  joblib.load('work/models/ocsvm.pickle')
 
# Function to process each batch and send to Kafka
def process_and_send_to_kafka(batch_df, batch_id):
    if not batch_df.isEmpty():  # Check if the batch is not empty
        print(f"Processing batch {batch_id} with {batch_df.count()} records.")
        # Convert to Pandas for actuator dummy encoding
        pandas_df = batch_df.toPandas()
        data = dataEngineering(pandas_df,actuators_NAMES)
        #spark_preprocess_df = spark.createDataFrame(data)
        #spark_preprocess_df.show()
        x = data.values
        predictions = svm.predict(np.array(x))
        print(predictions)
        #Predict
        print('This is the predictions :')
     
        # original DataFrame+ Prediction
        df = add_anomaly_prediction_column(batch_df, predictions,'prediction')
        for i, row in df.iterrows():
            key = f"row-{i}"
            value = json.dumps(row.to_dict())
            print(f"send {i}", value)
            producer.send(OUTPUT_TOPIC_NAME, key=key, value=value)
            time.sleep(5)

        df_with_predictions = spark.createDataFrame(df)
        df_with_predictions.show()
        
     
 
"""
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8')
)
""" 
 
def add_anomaly_prediction_column(df, predictions, col_name='anomaly_prediction'):
     # Add a unique index to the DataFrame based on 'Timestamp'
    window_spec = Window.orderBy("Timestamp")
    df_with_index = df.withColumn("index", row_number().over(window_spec))
 
    # Convert the numpy array to a list
    predictions_list = predictions.tolist()
 
    # Convert -1 to 1 and 1 to 0 in the predictions
    converted_predictions = [1 if val == -1 else 0 for val in predictions_list]
 
    # Create a Pandas DataFrame from the given DataFrame
    pandas_df = df_with_index.toPandas()
 
    # Add the converted predictions as a new column to the Pandas DataFrame
    pandas_df[col_name] = None
    for i, row in pandas_df.iterrows():
        idx = i % len(converted_predictions)
        pandas_df.at[i, col_name] = converted_predictions[idx]
        
    # Convert the Pandas DataFrame back to a Spark DataFrame
    #df_with_predictions = spark.createDataFrame(pandas_df)
    return pandas_df
 
 
# Process the streaming data
query = inputStream.writeStream \
    .foreachBatch(process_and_send_to_kafka) \
    .outputMode("update") \
    .start()
 
query.awaitTermination()
 