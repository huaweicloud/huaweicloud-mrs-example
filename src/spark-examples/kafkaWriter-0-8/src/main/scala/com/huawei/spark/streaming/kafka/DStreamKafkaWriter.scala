/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.spark.streaming.kafka

import java.util.Properties

import scala.reflect.ClassTag

import kafka.producer.KeyedMessage

import org.apache.spark.streaming.dstream.DStream

class DStreamKafkaWriter[T: ClassTag](@transient val dstream: DStream[T]) extends KafkaWriter[T] {

  /**
   * To write data from a DStream to Kafka, call this function after creating the DStream. Once
   * the DStream is passed into this function, all data coming from the DStream is written out to
   * Kafka. The properties instance takes the configuration required to connect to the Kafka
   * brokers in the standard Kafka format. The serializerFunc is a function that converts each
   * element of the RDD to a Kafka [[KeyedMessage]]. This closure should be serializable - so it
   * should use only instances of Serializables.
   * @param producerConfig The configuration that can be used to connect to Kafka
   * @param serializerFunc The function to convert the data from the stream into Kafka
   *                       [[KeyedMessage]]s.
   * @tparam K The type of the key
   * @tparam V The type of the value
   *
   */
  override def writeToKafka[K, V](producerConfig: Properties,
    serializerFunc: T => KeyedMessage[K, V]): Unit = {
    dstream.foreachRDD { rdd =>
      val rddWriter = new RDDKafkaWriter[T](rdd)
      rddWriter.writeToKafka(producerConfig, serializerFunc)
    }
  }

  /**
   * To write data from a DStream to Kafka by single and single
   * @param producerConfig The configuration that can be used to connect to Kafka
   * @param serializerFunc The function to convert the data from the stream into Kafka
   *                       [[KeyedMessage]]s.
   * @tparam K The type of the key
   * @tparam V The type of the value
   *
   */
  override def writeToKafkaBySingle[K, V](producerConfig: Properties,
    serializerFunc: T => KeyedMessage[K, V]): Unit = {
    dstream.foreachRDD { rdd =>
      val rddWriter = new RDDKafkaWriter[T](rdd)
      rddWriter.writeToKafkaBySingle(producerConfig, serializerFunc)
    }
  }
}
