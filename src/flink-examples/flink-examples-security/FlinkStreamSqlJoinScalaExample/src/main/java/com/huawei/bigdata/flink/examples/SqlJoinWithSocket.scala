package com.huawei.bigdata.flink.examples

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.tuple.{Tuple2, Tuple3}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.table.api.Expressions.$
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.types.Row

object SqlJoinWithSocket {
  def main(args: Array[String]): Unit = {
    var hostname: String = null
    var port = 0
    System.out.println("use command as: ")
    System.out.println("flink run --class com.huawei.bigdata.flink.examples.SqlJoinWithSocket /opt/test.jar --topic" + " topic-test -bootstrap.servers xxxx.xxx.xxx.xxx:21005 --hostname xxx.xxx.xxx.xxx --port xxx")
    System.out.println("flink run --class com.huawei.bigdata.flink.examples.SqlJoinWithSocket /opt/test.jar --topic" + " topic-test -bootstrap.servers xxxx.xxx.xxx.xxx:21007 --security.protocol SASL_PLAINTEXT" + " --sasl.kerberos.service.name kafka  --hostname xxx.xxx.xxx.xxx --port xxx")
    System.out.println("******************************************************************************************")
    System.out.println("<topic> is the kafka topic name")
    System.out.println("<bootstrap.servers> is the ip:port list of brokers")
    System.out.println("******************************************************************************************")
    try {
      val params = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname")
      else "localhost"
      port = params.getInt("port")
    } catch {
      case e: Exception =>
        System.err.println("No port specified. Please run 'FlinkStreamSqlJoinExample " + "--hostname <hostname> --port <port>', where hostname (localhost by default) " + "and port is the address of the text server")
        System.err.println("To start a simple text server, run 'netcat -l -p <port>' and " + "type the input text into the command line")
        return
    }

    val fsSettings = EnvironmentSettings.newInstance.inStreamingMode.build
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env, fsSettings)

    env.getConfig.setAutoWatermarkInterval(200)
    env.setParallelism(1)
    val paraTool = ParameterTool.fromArgs(args)

    val kafkaStream = env.addSource(new FlinkKafkaConsumer[String](paraTool.get("topic"), new SimpleStringSchema, paraTool.getProperties)).map(new MapFunction[String, Tuple3[String, String, String]]() {
      @throws[Exception]
      override def map(st: String): Tuple3[String, String, String] = {
        val word = st.split(",")
        new Tuple3[String, String, String](word(0), word(1), word(2))
      }
    })

    tableEnv.createTemporaryView("Table1", kafkaStream, $("name"), $("age"), $("gender"), $("proctime").proctime)

    val socketStream = env.socketTextStream(hostname, port, "\n").map(new MapFunction[String, Tuple2[String, String]]() {
      @throws[Exception]
      override def map(st: String): Tuple2[String, String] = {
        val words = st.split("\\s")
        if (words.length < 2) return new Tuple2[String, String]
        new Tuple2[String, String](words(0), words(1))
      }
    })

    tableEnv.createTemporaryView("Table2", socketStream, $("name"), $("job"), $("proctime").proctime)

    val result = tableEnv.sqlQuery("SELECT t1.name, t1.age, t1.gender, t2.job, t2.proctime as shiptime\n" + "FROM Table1 AS t1\n" + "JOIN Table2 AS t2\n" + "ON t1.name = t2.name\n" + "AND t1.proctime BETWEEN t2.proctime - INTERVAL '1' SECOND AND t2.proctime + INTERVAL" + " '1' SECOND")

    tableEnv.toAppendStream(result, classOf[Row]).print
    env.execute
  }

}
