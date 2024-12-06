
######encoding
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, udf, array
import numpy as np
import pickle
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline


# Initialize Spark
spark = SparkSession.builder.master("local[*]").appName("KafkaStreamProcessing").getOrCreate()

# Kafka topic and bootstrap servers
TOPIC_NAME = 'swat'
BOOTSTRAP_SERVERS = "kafka:29092"

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

# Load scaler
scaler = pickle.load(open('work/transformers/scaler.pickle', 'rb')) 
encoder = pickle.load(open('work/transformers/encoder.pickle', 'rb')) 
scaler_broadcast = spark.sparkContext.broadcast(scaler)
encoder_brodcast = spark.sparkContext.broadcast(encoder)



# Read from Kafka
inputStream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON data
inputStream = inputStream.withColumn("value", inputStream["value"].cast(StringType()))
inputStream= inputStream.withColumn("data", from_json(col("value"), schema)).select("data.*")


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

# Drop constant columns
constant_columns = ['P202', 'P401', 'P404', 'P502', 'P601', 'P603']
inputStream = inputStream.select([col for col in inputStream.columns if col not in constant_columns])

# Normalize numerical columns
numerical_columns =  ['FIT101', 'LIT101', 'AIT201', 'AIT202', 'AIT203', 'FIT201', 'DPIT301', 'FIT301', 'LIT301', 'AIT401', 'AIT402', 'FIT401', 'LIT401', 
                      'AIT501', 'AIT502', 'AIT503', 'AIT504', 'FIT501', 'FIT502', 'FIT503', 'FIT504', 'PIT501', 'PIT502', 'PIT503', 'FIT601']
#hot encoder
categorical_features = ['MV101', 'P101', 'P102', 'MV201', 'P201', 'P203', 'P204', 'P205', 'P206', 'MV301', 'MV302', 'MV303', 'MV304', 'P301', 'P302',
                         'P402', 'P403', 'UV401', 'P501', 'P602']






# Define a UDF to normalize numerical columns
def normalize_values(*cols):
    scaler = scaler_broadcast.value
   
    data = np.array(cols).reshape(1, -1)  # Convert to 2D array
    normalized_data = scaler.transform(data).flatten() 
   
     # Normalize and flatten back
    return normalized_data.tolist()

# Register the UDF
normalize_udf = udf(lambda *cols: normalize_values(*cols), ArrayType(DoubleType()))

# Apply the UDF to numerical columns
inputStream = inputStream.withColumn(
    "normalized_numericals",
    normalize_udf(*[col(c) for c in numerical_columns])
)

# Split normalized values back into separate columns
for i, col_name in enumerate(numerical_columns):
    inputStream = inputStream.withColumn(col_name, col("normalized_numericals")[i])

# Drop the temporary 'normalized_numericals' column
inputStream = inputStream.drop("normalized_numericals")




#######Encoding
def one_hot_encode(*cols):
    encoder = encoder_brodcast .value  # Use the broadcasted encoder
    data = np.array(cols).reshape(1, -1)  # Convert to 2D array
    one_hot_encoded = encoder.transform(data).toarray()  # Apply one-hot encoding
    return one_hot_encoded.tolist()[0]  

one_hot_encode_udf = udf(lambda *cols: one_hot_encode(*cols), ArrayType(DoubleType()))
# Apply the UDF to numerical columns
# Apply one-hot encoding to categorical columns
inputStream = inputStream.withColumn(
    "one_hot_encoded",
    one_hot_encode_udf(*[col(c) for c in categorical_features])
)



# Split one-hot encoded values back into separate columns
for i in range(len(categorical_features) * len(encoder.categories_)):
     inputStream = inputStream.withColumn(f"one_hot_{i}", col("one_hot_encoded")[i])

# Drop the temporary 'one_hot_encoded' column
inputStream = inputStream.drop("one_hot_encoded")    




# At this point, the 'features' column in inputStream contains the combined numerical and one-hot encoded features

"""
# Apply StringIndexer and OneHotEncoder for categorical features
indexers = [StringIndexer(inputCol=col_name, outputCol=col_name + "_index") for col_name in categorical_features]
encoders = [OneHotEncoder(inputCol=col_name + "_index", outputCol=col_name + "_onehot") for col_name in categorical_features]

# Combine indexers and encoders into a pipeline
pipeline = Pipeline(stages=indexers + encoders)
inputStream = pipeline.fit(inputStream).transform(inputStream)
"""















# Start query
query =  inputStream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "./checkpoints_swat_stream") \
    .start()

query.awaitTermination()



















