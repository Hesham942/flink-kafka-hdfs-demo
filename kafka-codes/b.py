from kafka import KafkaConsumer
from hdfs import InsecureClient
import json
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_hdfs_client(max_retries=10):
    """
    Attempt to establish an HDFS connection with retries
    """
    for attempt in range(max_retries):
        try:
            hdfs_client = InsecureClient('http://namenode:9870', user='root')
            hdfs_client.list('/')
            logger.info("Successfully connected to HDFS")
            return hdfs_client
        except Exception as e:
            logger.warning(f"HDFS connection attempt {attempt + 1} failed: {e}")
            time.sleep(10)
    raise ConnectionError("Could not establish HDFS connection after multiple attempts")

def get_kafka_consumer(max_retries=10):
    """
    Attempt to create Kafka consumer with retries
    """
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                'train',
                bootstrap_servers='kafka:9092',
                auto_offset_reset='earliest',
                group_id='fraud-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                request_timeout_ms=30000,
                session_timeout_ms=25000
            )
            logger.info("Successfully connected to Kafka")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka connection attempt {attempt + 1} failed: {e}")
            time.sleep(10)
    raise ConnectionError("Could not establish Kafka connection after multiple attempts")

def write_to_hdfs(hdfs_client, data):
    """
    Write batch data to a unique file in HDFS
    """
    try:
        # Ensure the directory exists
        hdfs_client.makedirs('/opt/codes/fraud-data')
        
        # Generate a unique filename
        unique_filename = f'/opt/codes/fraud-data/fraud-batch-{int(time.time())}.json'
        
        # Write data to HDFS
        with hdfs_client.write(unique_filename, encoding='utf-8', overwrite=True) as writer:
            writer.write(json.dumps(data))
        logger.info(f"Successfully wrote batch to {unique_filename}")
    except Exception as e:
        logger.error(f"Error writing to HDFS: {e}")

def main():
    """
    Main function to run Kafka to HDFS data transfer
    """
    try:
        hdfs_client = get_hdfs_client()
        consumer = get_kafka_consumer()

        buffer = []
        batch_size = 6  # Adjust as needed
        
        for message in consumer:
            try:
                logger.info(f"Received message: {message.value}")
                buffer.append(message.value)
                
                if len(buffer) >= batch_size:
                    logger.info(f"Writing batch to HDFS: {len(buffer)} messages")
                    write_to_hdfs(hdfs_client, buffer)
                    buffer = []  # Clear the buffer
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
    except ConnectionError as conn_err:
        logger.error(f"Connection error: {conn_err}")
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
        logger.info("Kafka consumer closed")

if __name__ == "__main__":
    time.sleep(30)  # Allow services to initialize
    main()
