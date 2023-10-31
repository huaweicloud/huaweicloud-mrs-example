样例1：
HoodieWriteClientExample：

    1.使用maven打包成hudi-java-examples-1.0.jar并上传至指定目录（以/opt/example/为例）

    2.source client的bigdata_env和Hudi目录下的component_env，kinit有Hive Hadoop权限的用户

    3.使用spark-submit提交作业：
        spark-submit --keytab <user_keytab_path> --principal=<principal_name> --class com.huawei.bigdata.hudi.examples.HoodieWriteClientExample /opt/example/hudi-java-security-examples-1.0.jar  hdfs://hacluster/tmp/example/hoodie_java  hoodie_java

样例2：
HoodieDeltaStreamer：

    1.使用maven打包成hudi-java-examples-1.0.jar并上传至指定目录（以/opt/example/为例）

    2.制造写入Kafka的数据
        spark-submit --keytab <user_keytab_path> --principal=<principal_name> --class com.huawei.bigdata.hudi.examples.ProducerDemo /opt/example/hudi-java-security-examples-1.0.jar xx.xx.xx.xx:xxxx testtopic

    3.将kafka-source.properties上传至指定目录（以/opt/example/为例），修改其中的kafka参数

    4.启动deltastreamer消费kafka写入Hudi
        spark-submit --keytab <user_keytab_path> --principal=<principal_name> --master yarn --driver-memory 1g  --executor-memory 1g --executor-cores 1 --num-executors 2 --conf spark.kryoserializer.buffer.max=128m --driver-class-path /opt/client/Hudi/hudi/conf:/opt/client/Hudi/hudi/lib/*:/opt/example/*:/opt/client/Spark2x/spark/jars/* --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer spark-internal --props file:///opt/example/kafka-source.properties --target-base-path /tmp/huditest/hudi_deltastreamer_example_table --table-type MERGE_ON_READ --target-table hudi_deltastreamer_example_table --source-ordering-field age --source-class org.apache.hudi.utilities.sources.JsonKafkaSource --schemaprovider-class com.huawei.bigdata.hudi.examples.DataSchemaProviderExample --transformer-class com.huawei.bigdata.hudi.examples.TransformerExample --enable-hive-sync  --continuous