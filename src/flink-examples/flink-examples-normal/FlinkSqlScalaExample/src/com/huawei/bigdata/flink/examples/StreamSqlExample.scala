/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.bigdata.flink.examples

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala._

object StreamSqlExample {

  def main(args: Array[String]): Unit = {

    println("use command as: ")
    println("bin/flink run --class com.huawei.bigdata.flink.examples.StreamSqlExample " +
      "<path of StreamSqlExample jar> ")
    println("*********************************************************************************")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.enableCheckpointing(100000, CheckpointingMode.EXACTLY_ONCE);
    env.setParallelism(2)

    val ds1: DataStream[(Long, String, String, Int)] = env.addSource(new SEventSource)
    val ds2: DataStream[(Long, String, String, Int)] = env.addSource(new SEventSource)
    tEnv.registerTable("SEvent1", ds1.toTable(tEnv, 'id, 'name, 'info, 'cnt))
    tEnv.registerTable("SEvent2", ds2.toTable(tEnv, 'id, 'name, 'info, 'cnt))

    val tmpDs = tEnv.sql(
      """
        | SELECT * FROM SEvent1 where id > 7
        | UNION ALL
        | SELECT * FROM SEvent2 where id > 7
      """.stripMargin
    ).toAppendStream[(Long, String, String, Int)]

    tEnv.registerTable("TmpStream", tmpDs.toTable(tEnv, 'id, 'name, 'info, 'cnt, 'proctime.proctime))

    tEnv.sql(
      """
        |SELECT
        |  SUM(cnt)
        | FROM TmpStream
        | GROUP BY TUMBLE(proctime, INTERVAL '10' SECOND)
      """.stripMargin).toAppendStream[Integer].print()

    env.execute()
  }

}

class SEventSource extends RichSourceFunction[(Long, String, String, Int)]{

  private var count = 0L
  private var isRunning = true
  private val alphabet = "abcdefg"

  override def run(sourceContext: SourceFunction.SourceContext[(Long, String, String, Int)]): Unit = {
    while(isRunning) {
      for (i <- 0 until 10) {
        sourceContext.collect((i, "hello-" + count, alphabet, 1))
        count += 1L
      }
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
