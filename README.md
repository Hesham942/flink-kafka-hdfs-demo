# Flink, Kafka, and HDFS Demo Project

This project demonstrates a data pipeline that reads from a Kafka topic, processes the data with Apache Flink, and writes the output to HDFS. The entire environment is orchestrated using Docker Compose.

---

## Configuration ⚙️

This project simplifies HDFS connectivity by setting the Hadoop configuration path directly within Flink's main configuration file.

Inside the **`flink-conf/config.yaml`** file, the following lines have been added:

```yaml
fs:
  hdfs:
    hadoop-conf: /opt/hadoop/etc/hadoop
```

This tells Flink exactly where to find the Hadoop configuration files (`core-site.xml`, `hdfs-site.xml`) inside the container. By doing this, Flink automatically knows how to connect to the HDFS NameNode, removing the need to set environment variables like `HADOOP_CONF_DIR` for every job submission.

---

## Setup and Prerequisites

Before running the project, you must download the required Flink connector JAR files. Due to their size, they are not included in this repository.

1.  **Create the `jobs` directory** if it doesn't already exist.
2.  **Download the following JARs** and place them inside the `jobs` directory:

    * **Flink Connector for Kafka:**
        * **File:** `flink-sql-connector-kafka-3.3.0-1.19.jar`
        * **Download Link:** [Maven Central](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.19/flink-sql-connector-kafka-3.3.0-1.19.jar)

    * **Flink Shaded Hadoop JAR:**
        * **File:** `flink-shaded-hadoop-3-uber-3.1.1.7.2.8.0-224-9.0.jar`
        * **Download Link:** [Cloudera Repository](https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/flink/flink-shaded-hadoop-3-uber/3.1.1.7.2.8.0-224-9.0/flink-shaded-hadoop-3-uber-3.1.1.7.2.8.0-224-9.0.jar)

    * **Flink Connector for Files**
        * **File:** `flink-connector-files-1.19.0.jar`
        * **Download Link:** [Maven Central](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-files/1.19.0/flink-connector-files-1.19.0.jar)

After downloading, your `./jobs` directory should look something like this:
```
/proj
|-- /jobs
|   |-- flink-sql-connector-kafka-3.3.0-1.19.jar
|   |-- flink-shaded-hadoop-3-uber-....jar
|   |-- flink-connector-files-1.19.0.jar
|   |-- kafka_to_hdfs_job.py
|   |-- ...
|-- docker-compose.yml
|-- ...
```

---

## How to Run
1.  **Start the environment:**
    ```bash
    docker-compose up -d
    ```
2.  **Run the Kafka producer:**
    ```bash
    docker-compose exec kafka python3 /app/scripts/producer.py
    ```
3.  **Submit the Flink job:**
    ```bash
    docker-compose exec jobmanager flink run /opt/flink/usrlib/kafka_to_hdfs_job.py
    ```
