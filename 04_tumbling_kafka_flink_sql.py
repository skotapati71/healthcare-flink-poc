from pyflink.table import EnvironmentSettings, StreamTableEnvironment, ScalarFunction, DataTypes
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.udf import udf

@udf(input_types=[DataTypes.INT()], result_type=DataTypes.STRING())
def age_range (age):
        if age < 50:
            return '<50Years'
        else:
            return '>50Years'

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_streaming_mode()
    table_env = StreamTableEnvironment.create(env, settings)

    table_env.create_temporary_function("AGE_RANGE",age_range)

    env.add_jars(
        "file:///home/sreenivas//work/poc/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")
    env.add_jars(
        "file:///home/sreenivas/work/poc/lib/mysql-connector-java-8.0.30.jar")
    env.add_jars(
        "file:///home/sreenivas/work/poc/lib/flink-connector-jdbc-3.2.0-1.19.jar")

    # create kafka input_data table
    source_sql = """
        CREATE TABLE patient_health (
            name STRING,
            age INT,
            gender STRING,
            bloodType STRING,
            medicalCondition STRING,
            admissionDate STRING,
            doctor STRING,
            hospital STRING,
            insuranceProvider STRING,
            BilledAmount STRING,
            roomNumber STRING,
            admissionType STRING,
            dischargeDate STRING,
            medication STRING,
            testResults STRING,
            healthGroupId STRING,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'patient-health',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'test-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """
    table_env.execute_sql(source_sql)

    # process a tumbling window aggregate of count of patient by medicalCondition and age
    # tumbling_sql ="""
    #     SELECT medicalCondition as medical_condition, AGE_RANGE(age) as age_range, COUNT(*) AS patient_count,
    #     TUMBLE_START(proctime, INTERVAL '3' SECONDS) AS window_start,
    #     TUMBLE_END(proctime, INTERVAL '3' SECONDS) AS window_end
    #     FROM patient_health
    #     GROUP BY medicalCondition, AGE_RANGE(age), TUMBLE(proctime, INTERVAL '3' SECONDS)
    # """

    tumbling_sql ="""
        SELECT medicalCondition as medical_condition, AGE_RANGE(age) as age_range, COUNT(*) AS patient_count,
        TUMBLE_START(proctime, INTERVAL '3' SECONDS) AS window_start,
        TUMBLE_END(proctime, INTERVAL '3' SECONDS) AS window_end
        FROM patient_health WHERE testResults = 'Normal'
        GROUP BY medicalCondition, AGE_RANGE(age), TUMBLE(proctime, INTERVAL '3' SECONDS)
    """

    tumbling_w = table_env.sql_query(tumbling_sql)

    # create print sink table
    kafka_sink_ddl ="""
        CREATE TABLE medical_insights_kafka_sink (
            medical_condition STRING,
            age_range STRING,
            patient_count BIGINT,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'medical-insights',
            'properties.bootstrap.servers' = 'localhost:9092',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        )
    """
    table_env.execute_sql(kafka_sink_ddl)

    mysql_sink_ddl ="""
        CREATE TABLE medical_insights_mysql_sink (
            medical_condition STRING,
            age_range STRING,
            patient_count BIGINT,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://localhost:3306/test_patient_db',
            'table-name' = 'medical_insights',
            'driver' = 'com.mysql.jdbc.Driver',
            'username' = 'testsqluser',
            'password' = 'Test.sql.pwd.1'
        )
    """
    table_env.execute_sql(mysql_sink_ddl)


    # explain
    # print(tumbling_w.explain())

    # sink_ddl ="""
    #     CREATE TABLE medical_insights_sink (
    #         medicalCondition STRING,
    #         age_range STRING,
    #         patientCount BIGINT,
    #         window_start TIMESTAMP(3),
    #         window_end TIMESTAMP(3)
    #     ) WITH (
    #         'connector' = 'print'
    #     )
    # """

    # add multiple sinks

    tumbling_w.execute_insert("medical_insights_mysql_sink").wait()


if __name__ == "__main__" :
    main()
