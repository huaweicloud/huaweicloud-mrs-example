package com.huawei.bigdata.flink.examples

import java.util.{HashSet, Set}
import java.util.concurrent.TimeUnit

import com.huawei.jredis.client.SslSocketFactoryUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.ResultFuture
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Read stream data and join from configure table from redis.
 */
object FlinkConfigtableScalaExample {

  private val MAX_ATTEMPTS = 2

  private val TIMEOUT = 60000

  def main(args: Array[String]) {
    // print comment for command to use run flink
    System.out.println("use command as: \n" +
      "./bin/flink run --class com.huawei.bigdata.flink.examples.FlinkConfigtableScalaExample" +
      " -m yarn-cluster -yt /opt/config -yn 3 -yjm 1024 -ytm 1024 " +
      "/opt/FlinkConfigtableScalaExample.jar --dataPath config/data.txt" +
      "******************************************************************************************\n" +
      "Especially you may write following content into config filePath, as in config/read.properties: \n" +
      "ReadFields=username,age,company,workLocation,educational,workYear,phone,nativeLocation,school\n" +
      "Redis_IP_Port=SZV1000064084:22400,SZV1000064082:22400,SZV1000064085:22400\n" +
      "******************************************************************************************")

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // get configure and read data and transform to OriginalRecord
    val dataPath = ParameterTool.fromArgs(args).get("dataPath", "config/data.txt")
    val originalStream = env.readTextFile(dataPath)
      .map(it => getRecord(it)).assignTimestampsAndWatermarks(new Record2TimestampExtractor).disableChaining()


    // read from redis and join to the whole user information
    val resultStream = AsyncDataStream.unorderedWait(
      originalStream,
      2,
      TimeUnit.MINUTES,
      5) {
      (input, resultFuture: ResultFuture[UserRecord]) =>
        Future {
          // get configure from config/read.properties, you must put this with commands:
          // ./bin/yarn-session.sh -t /opt/config -n 3 -jm 1024 -tm 1024 or
          // ./bin/flink run -m yarn-cluster -yt /opt/config -yn 3 -yjm 1024 -ytm 1024 /opt/test.jar
          val configPath = "config/read.properties"
          val fields = ParameterTool.fromPropertiesFile(configPath).get("ReadFields")
          val hostPort = ParameterTool.fromPropertiesFile(configPath).get("Redis_IP_Port")
          val ssl = ParameterTool.fromPropertiesFile(configPath).getBoolean("Redis_ssl_on", false)

          // create jedisCluster client
          val hosts: Set[HostAndPort]  = new HashSet[HostAndPort]()
          hostPort.split(",").foreach(it => {
            val hostAndPort = genHostAndPort(it)
            if (hostAndPort != null) {
              hosts.add(hostAndPort);
            }
          })
          val poolConfig = new JedisPoolConfig
          val socketFactory = SslSocketFactoryUtil.createTrustALLSslSocketFactory

          val client = new JedisCluster(hosts, TIMEOUT, TIMEOUT, MAX_ATTEMPTS, "", "", poolConfig, ssl, socketFactory, null, null, null)

          if (client.getClusterNodes.size() <= 0) {
            System.out.println("JedisCluster init failed, getClusterNodes: " + client.getClusterNodes.size())
          }
          // set key string, if you key is more than one column, build your key string with columns
          val key = input.name
          if (!client.exists(key)) {
            System.out.println("test-------cannot find data to key:  " + key)
            resultFuture.complete(Seq(new UserRecord(
              input.name,
              0,
              "null",
              "null",
              "null",
              0,
              "null",
              "null",
              "null",
              input.gender,
              input.shoppingTime)))
          } else {
            val values = client.hmget(key, fields.split(","):_*)
            System.out.println("test-------key: " + key + "  get some fields:  " + values.toString)
            resultFuture.complete(Seq(new UserRecord(
              values.get(0),
              Integer.parseInt(values.get(1)),
              values.get(2),
              values.get(3),
              values.get(4),
              Integer.parseInt(values.get(5)),
              values.get(6),
              values.get(7),
              values.get(8),
              input.gender,
              input.shoppingTime)))
          }
          client.close()
        } (ExecutionContext.global)
    }

    // data transform
     resultStream.filter(_.gender == "female")
      .keyBy("name")
      .window(TumblingEventTimeWindows.of(Time.seconds(30)))
      .reduce((e1, e2) => UserRecord(e1.name, e2.age, e2.company, e2.workLocation, e2.educational, e2.workYear,
        e2.phone, e2.nativeLocation, e2.school, e2.gender, e1.shoppingTime + e2.shoppingTime))
      .filter(_.shoppingTime > 120).print()

    // execute program
    env.execute("FlinkConfigtable scala")
  }

  private def genHostAndPort(ipAndPort: String): HostAndPort = {
    val lastIdx = ipAndPort.lastIndexOf(":")
    if (lastIdx == -1) return null
    val ip = ipAndPort.substring(0, lastIdx)
    val port = ipAndPort.substring(lastIdx + 1)
    new HostAndPort(ip, port.toInt)
  }

  // get enums of record
  def getRecord(line: String): OriginalRecord = {
    val elems = line.split(",")
    assert(elems.length == 3)
    val name = elems(0)
    val gender = elems(1)
    val time = elems(2).toInt
    OriginalRecord(name, gender, time)
  }

  // the scheme of record read from txt
  case class OriginalRecord(name: String, gender: String, shoppingTime: Int)

  case class UserRecord(name: String, age: Int, company: String, workLocation: String, educational: String, workYear: Int,
                        phone: String, nativeLocation: String, school: String, gender: String, shoppingTime: Int)


  // class to set watermark and timestamp
  private class Record2TimestampExtractor extends AssignerWithPunctuatedWatermarks[OriginalRecord] {

    // add tag in the data of datastream elements
    override def extractTimestamp(element: OriginalRecord, previousTimestamp: Long): Long = {
      System.currentTimeMillis()
    }

    // give the watermark to trigger the window to execute, and use the value to check if the window elements is ready
    def checkAndGetNextWatermark(lastElement: OriginalRecord,
                                 extractedTimestamp: Long): Watermark = {
      new Watermark(extractedTimestamp - 1)
    }
  }
}



