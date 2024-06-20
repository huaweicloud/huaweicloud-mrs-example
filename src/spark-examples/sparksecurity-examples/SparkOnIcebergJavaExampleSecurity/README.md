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
3. 在manager页面创建用户，下载用户认证凭据，同步修改IcebergSqlExample的kerberosLogin方法中用户和凭据配置。 最后把凭据配置传到客户端节点的对应路径。   
4. 通过spark-sutmit提交运行
   sh bin/spark-submit --class com.huawei.bigdata.iceberg.examples.IcebergSqlExample --master local --deploy-mode client ./SparkOnIcebergJavaExampleSecurity-1.0.jar   