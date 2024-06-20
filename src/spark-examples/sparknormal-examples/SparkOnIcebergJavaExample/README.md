#Iceberg样例

## Windows本地运行
1. 修改SparkSessionFactory的buildSparkSession方法，指定SparkSession的mater为local
   ```java
        return SparkSession.builder()
                .master("local") //添加这行
                .appName("spark on iceberg example")
                .config("...")
                .config("...")
   ```
2. 运行IcebergSqlExample即可

## 集群运行
1. 编译此模块，生成jar包
2. 把包传到spark的客户端安装目录，如/opt/client/Spark/spark
3. 使用omm用户，通过spark-sutmit提交运行
   export HADOOP_USER_NAME=omm

   sh bin/spark-submit --class com.huawei.bigdata.iceberg.examples.IcebergSqlExample --master local --deploy-mode client ./SparkOnIcebergJavaExample-1.0.jar
   