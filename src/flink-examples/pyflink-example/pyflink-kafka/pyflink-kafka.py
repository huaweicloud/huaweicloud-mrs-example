import os
import logging
import sys

from pyflink.common import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaProducer, FlinkKafkaConsumer

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableEnvironment, EnvironmentSettings


def read_sql(file_path):
    if not os.path.isfile(file_path):
        raise TypeError(file_path + " does not exist")

    all_the_text = open(file_path).read()
    return all_the_text


def exec_sql():
    # 提交前修改sql路径
    file_path = "insertData2kafka.sql"
    sql = read_sql(file_path)
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    statement_set = t_env.create_statement_set()
    sqlArr = sql.split(";")
    for sqlStr in sqlArr:
        sqlStr = sqlStr.strip()
        if sqlStr.lower().startswith("create"):
            print("---------create---------------")
            print(sqlStr)
            t_env.execute_sql(sqlStr)
        if sqlStr.lower().startswith("insert"):
            print("---------insert---------------")
            print(sqlStr)
            statement_set.add_insert_sql(sqlStr)

    statement_set.execute()


def read_write_kafka():
    # find kafka connector jars
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    specific_jars = "file:///opt/client/Flink/flink/lib/flink-connector-kafka-1.15.0-h0.cbu.mrs.321.r13.jar"
    # the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
    env.add_jars(specific_jars)

    kafka_properties = {'bootstrap.servers': '192.168.20.162:21005', 'group.id': 'test_group'}

    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(type_info=Types.ROW([Types.INT(), Types.STRING()])).build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='test_source_topic',
        deserialization_schema=deserialization_schema,
        properties=kafka_properties)

    print("---------read ---------------")
    ds = env.add_source(kafka_consumer)

    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW([Types.INT(), Types.STRING()])).build()
    kafka_producer = FlinkKafkaProducer(
        topic='test_sink_topic',
        serialization_schema=serialization_schema,
        producer_config=kafka_properties)
    print("--------write------------------")
    ds.add_sink(kafka_producer)
    env.execute("pyflink kafka test")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    print("------------------insert data to kafka----------------")
    exec_sql()
    print("------------------read_write_kafka----------------")
    read_write_kafka()
