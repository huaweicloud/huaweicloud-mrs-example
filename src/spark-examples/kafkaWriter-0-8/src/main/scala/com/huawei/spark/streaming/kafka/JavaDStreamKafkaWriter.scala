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

import org.apache.spark.api.java.function.Function
import org.apache.spark.streaming.api.java.JavaDStream

class JavaDStreamKafkaWriter[T](dstream: JavaDStream[T])(implicit val classTag: ClassTag[T]) {
  private val dstreamWriter = new DStreamKafkaWriter[T](dstream.dstream)

  def writeToKafka[K, V](
    producerConfig: Properties,
    function1: Function[T, KeyedMessage[K, V]]): Unit = {
    dstreamWriter.writeToKafka(producerConfig, t => function1.call(t))
  }
}

object JavaDStreamKafkaWriterFactory {

  def fromJavaDStream[T](dstream: JavaDStream[T]): JavaDStreamKafkaWriter[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    new JavaDStreamKafkaWriter[T](dstream)
  }
}
