
## README
This reposiotry demonstartes the proof of concept in action with Kafka in Kraft mode and processing using Pyflink.

UseCase: Find Medical Condition of patients realtime and insights. Kaggle Healthcare Dataset (the csv file) is as an input. 

## Architecture
![Healthcar-Kafka-Flink-POC](https://github.com/user-attachments/assets/c0aa84bf-26a3-4b44-a962-ca7c6aa0e229)

Steps to install ..

Step-0: Install Python IDE (PyCharm) -- community free edition

Step-1: Install OpenJDK(Java11) and set JAVA_HOME, PATH and CLASSPATH

Step-2: Install Python 3.12 version and create a virtual environment for poc

Step-3: Follow the instructions in https://kafka.apache.org/quickstart to install Kafka with Kraft mode

Step-4: Install the following python packages using pip in the newly created virtual environment

        apache-flink, kafka-python, confluent-kafka

Step-5: Download the following flink connectors to read from Kafka topic and write into mysql databese

        flink-sql-connector-kafka-3.1.0-1.18.jar 
        mysql-connector-java-8.0.30.jar
        flink-connector-jdbc-3.2.0-1.19.jar

Step-6: Download Grafana to create Medical Insights Dashboard

Follow the modules

### 01_read_csv_healthdata_process.py

This program demonstrates reading Healthcare csv file in flink/batch_mode and process it using flink table-api and in combination with panda. This is outside of the use case. But it gives hands-on experience on processing data using flink/table-api

### 03_read_health_csv_write_to_kafka.py

This module stream patient data (Kaggle CSV) into Kafka Topic. Before running make sure the topic - patient-health is created with partition=10. This can be done by executing kafka prvoded script.  

### 04_tumbling_kafka_flink_sql.py

This is the heart of flink processing demonstarting its flagship features of watermarking and windowing. The flink documentation is outstanding, it is laid out in such a way wasy to understand (https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/overview/)

The flink-sql-connector for kafka is used to create a dynamic source table by timestamping the processed time for each reach record. The processed time is used to eatsblish the window for aggregation, in the example tumbling window is used. Created Tumbling Window Aggregate function - TUMBLE(proctime, INTERVAL '15' SECONDS) on Medical Condition, Count of Patients, Window Start and End times

The aggregated output is written into relation database (mysql) using flink-jdbc and mysql-sink connectors. Note: create database and table inside mysql using the ddl provide.

03 and 04 scripts can be run in parallel. 

As an extra step, Grafana dashboard can be created to visualize the data in realtime by hooking it to mysql.
  
![Grafana](https://github.com/user-attachments/assets/7bf817c9-701b-402c-aed9-38d929cb2699)


