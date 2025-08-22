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

    env = StreamExecutionEnvironment.get_execution_environment()
    
    
    kafka_jar_path = "file:///opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.19.jar"
    

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("input-topic") \
        .set_group_id("flink-hdfs-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()


    data_stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="Kafka Source"
    )



    transformed_stream = data_stream.map(lambda msg: msg.upper(), output_type=Types.STRING())
    

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


    transformed_stream.sink_to(file_sink)


    env.execute("Kafka to HDFS Demo")

if __name__ == '__main__':
    run_kafka_to_hdfs_job()
