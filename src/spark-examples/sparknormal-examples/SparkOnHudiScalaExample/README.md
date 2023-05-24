样例使用说明：

1.使用maven打包成hudi-scala-examples-1.0.jar并上传至指定目录（以/opt/example/为例）

2.source client的bigdata_env和Hudi目录下的component_env，kinit有Hive Hadoop权限的用户

3.使用spark-submit提交作业：

spark-submit --class com.huawei.bigdata.hudi.examples.HoodieDataSourceExample /opt/example/hudi-scala-examples-1.0.jar hdfs://hacluster/tmp/example/hoodie_scala hoodie_scala