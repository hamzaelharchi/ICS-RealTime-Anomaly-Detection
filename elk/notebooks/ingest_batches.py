import time
import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json  # For handling JSON serialization

# Initialize Kafka Admin Client
admin_client = KafkaAdminClient(bootstrap_servers="kafka:29092")
existing_topics = admin_client.list_topics()
print(f"Existing topics: {existing_topics}")


# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers="kafka:29092",
    key_serializer=lambda k: k.encode('utf-8'),
    value_serializer=lambda v: v.encode('utf-8')  # Serialize messages as UTF-8 encoded strings
)

TOPIC_NAME = 'devices'

# Check if topic exists, create if not
if TOPIC_NAME not in existing_topics:
    topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)]
    admin_client.create_topics(new_topics=topic_list, validate_only=False)

# Read CSV into a pandas DataFrame
df = pd.read_csv('./work/Attack.csv')  # Replace 'Normal.csv' with your actual CSV file path

# Initialize variables for batching
batch_size = 10
batch = []

# Loop through each row of the CSV
for index, row in df.iterrows():
    # Add the current row as a dictionary to the batch
    batch.append(row.to_dict())

    # Check if batch size is reached
    if len(batch) == batch_size:
        # Prepare the batch key and value
        key = f"batch-{index // batch_size}"  # Unique key for the batch
        value = json.dumps(batch)  # Serialize the batch (list of dictionaries) as JSON string

        # Send the batch to Kafka
        producer.send(TOPIC_NAME, key=key, value=value)
        print(f"Sent batch key: {key}, batch value: {value}")

        # Clear the batch after sending
        batch = []

        # Optional: Wait before sending the next batch
        time.sleep(2)

# Send any remaining rows in the last batch
if batch:
    key = f"batch-{index // batch_size + 1}"  # Unique key for the last batch
    value = json.dumps(batch)
    producer.send(TOPIC_NAME, key=key, value=value)
    print(f"Sent final batch key: {key}, batch value: {value}")

# Close the producer after sending all messages
producer.close()
