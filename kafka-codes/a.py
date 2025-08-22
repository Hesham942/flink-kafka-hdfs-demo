from kafka import KafkaProducer
import pandas as pd
import json
import time

def read_csv_and_send(file_path, topic_name, producer):
    df = pd.read_csv(file_path)
    for _, row in df.iterrows():
        message = row.to_json()
        producer.send(topic_name, value=json.loads(message))
        time.sleep(1)  # Simulate streaming

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

read_csv_and_send('data.csv', 'train', producer)
producer.close()
