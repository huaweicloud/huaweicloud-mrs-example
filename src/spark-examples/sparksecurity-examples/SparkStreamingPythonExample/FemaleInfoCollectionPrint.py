# -*- coding:utf-8 -*-
# kafka 0.8
import sys
from pyspark.streaming import StreamingContext, kafka
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: FemaleInfoCollectionPrint.py <checkPointDir> <batchTime> <topics> <brokers>")
        exit(-1)

    checkPointDir = sys.argv[1]
    batchTime = sys.argv[2]
    topics = sys.argv[3]
    brokers = sys.argv[4]

    # 初始化SparkContext
    conf = SparkConf().setAppName("DataSightStreamingExample")
    sc = SparkContext(conf = conf)
    ssc = StreamingContext(sc, int(batchTime))

    # 设置Streaming的CheckPoint目录
    ssc.checkpoint(checkPointDir)

    # 获得Kafka的主题和brokers列表
    topics = list(set(topics.split(",")))
    kafkaParams = {"metadata.broker.list": brokers}

    # 通过brokers和topics直接创建kafka stream
    # 1.接收Kafka中数据，生成相应DStream
    lines = kafka.KafkaUtils.createDirectStream(ssc, topics, kafkaParams).map(lambda r:r[1])
	
    # 2.获取每一个行的字段属性
    records = lines.map(lambda line: line.split(",")).map(lambda elems: (elems[0], elems[1], int(elems[2])))

    # 3.筛选女性网民上网时间数据信息
    femaleRecords = records.filter(lambda record1: record1[1] == "female").map(lambda record2: (record2[0], record2[2]))
    upTimeUser = femaleRecords.filter(lambda User: User[1] > 30)

    # 4.筛选连续上网时间超过阈值的用户，并获取结果
    # 默认打印DStream中前10个元素
    upTimeUser.pprint()

    # 5.Streaming系统启动
    ssc.start()
    ssc.awaitTermination()
