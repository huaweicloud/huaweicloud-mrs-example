样例使用说明：

1.将HudiPythonExample.py拷贝至指定目录（以/opt/example/为例）

2.source client的bigdata_env和Hudi目录下的component_env，kinit有Hive Hadoop权限的用户

3.使用spark-submit提交作业：

spark-submit   /opt/example/HudiPythonExample.py hdfs://hacluster/tmp/huditest/example/python  hudi_trips_cow