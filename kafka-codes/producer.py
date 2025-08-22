# ./kafka-codes/producer.py

import json
import time
from kafka import KafkaProducer

# The Kafka service is reachable within the Docker network at 'kafka:9092'
bootstrap_servers = 'localhost:9092'
topic_name = 'input-topic'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Connected to Kafka at {bootstrap_servers}. Sending messages to topic '{topic_name}'...")

# Send 100 messages
for i in range(100):
    message = {'user_id': f'user_{i}', 'message': f'Hello Flink! Message #{i}'}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.1)  # Sleep for 100ms

# Ensure all messages are sent
producer.flush()
producer.close()
print("Finished sending messages.")