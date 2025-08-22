# ./jobs/kafka_to_hdfs_job.py

import os
import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.common import WatermarkStrategy
from pyflink.common.typeinfo import Types

def run_kafka_to_hdfs_job():
    """
    A Flink job that reads from Kafka and writes to HDFS.
    """
    # 1. Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Define the JAR files required for the connectors.
    # These paths are inside the Flink container.
    # Your docker-compose mounts the Kafka connector JAR.
    # The Hadoop FileSystem connector is part of the Flink distribution in newer versions
    # but we add the JAR explicitly for robustness.
    kafka_jar_path = "file:///opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar"
    
    # The Hadoop FileSystem connector is needed for HDFS.
    # It seems missing from your job/task manager mounts, so ensure it's in the Flink lib folder.
    # Your jupyter service mounts it, let's assume it's available in the main lib.
    # A safe fallback is to add it via env.add_jars if needed.
    # env.add_jars(kafka_jar_path) # You can add JARs this way if needed.
    
    # 2. Create a Kafka Source
    # The service name 'kafka' is used as the hostname because we are in the same Docker network.
    # The HDFS namenode URI comes from your hadoop.env file.
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("input-topic") \
        .set_group_id("flink-hdfs-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 3. Create a data stream from the Kafka source
    data_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="Kafka Source"
    )

    # You can add transformations here if you want.
    # For example, to uppercase the messages:
    transformed_stream = data_stream.map(lambda msg: msg.upper(), output_type=Types.STRING())
    
    # 4. Create an HDFS File Sink
    hdfs_output_path = "hdfs://namenode:9000/flink-output-kafka"
    
    file_sink = FileSink.for_row_format(
            base_path=hdfs_output_path,
            encoder=Encoder.simple_string_encoder()
        ) \
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("data")
            .with_part_suffix(".txt")
            .build()
        ) \
        .with_rolling_policy(
            RollingPolicy.default_rolling_policy(rollover_interval=60*1000, part_size= 1024 * 1024 * 5, inactivity_interval=5*1000)
        ) \
        .build()

    # 5. Connect the stream to the sink
    # If you applied a transformation, use 'transformed_stream' here instead.
    transformed_stream.sink_to(file_sink)

    # 6. Execute the job
    env.execute("Kafka to HDFS Demo")

if __name__ == '__main__':
    run_kafka_to_hdfs_job()