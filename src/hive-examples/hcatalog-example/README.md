hcatalog-example使用说明

1、使用mvn clean install命令编译项目，打包成jar包

2、beeline登录，预先创建t1和t2表

1)创建源头表t1:

create table t1(col1 int);

2)创建目的表t2:

create table t2(col1 int,col2 int);

3、上传jar包到客户端所在机器

4、配置环境变量

```
export HADOOP_HOME=/opt/client/HDFS/hadoop 
export HIVE_HOME=/opt/client/Hive/Beeline 
export HCAT_HOME=$HIVE_HOME/../Hcatalog
```

```
export LIB_JARS=$HCAT_HOME/lib/hive-hcatalog-core-3.1.0-hw-ei-302001-SNAPSHOT.jar,$HCAT_HOME/lib/hive-metastore-3.1.0-hw-ei-302001-SNAPSHOT.jar,$HCAT_HOME/lib/hive-standalone-metastore-3.1.0-hw-ei-302001-SNAPSHOT.jar,$HIVE_HOME/lib/hive-exec-3.1.0-hw-ei-302001-SNAPSHOT.jar,$HCAT_HOME/lib/libfb303-0.9.3.jar,$HCAT_HOME/lib/slf4j-api-1.7.30.jar,$HCAT_HOME/lib/antlr-3.5.2.jar,$HCAT_HOME/lib/jdo-api-3.0.1.jar,$HCAT_HOME/lib/antlr-runtime-3.5.2.jar,$HCAT_HOME/lib/datanucleus-api-jdo-4.2.4.jar,$HCAT_HOME/lib/datanucleus-core-4.1.17.jar,$HCAT_HOME/lib/datanucleus-rdbms-fi-4.1.19-SNAPSHOT.jar,$HCAT_HOME/lib/log4j-api-2.10.0.jar,$HCAT_HOME/lib/log4j-core-2.10.0.jar
```

```
export HADOOP_CLASSPATH=$HCAT_HOME/lib/hive-hcatalog-core-3.1.0-hw-ei-302001-SNAPSHOT.jar:$HCAT_HOME/lib/hive-metastore-3.1.0-hw-ei-302001-SNAPSHOT.jar:$HCAT_HOME/lib/hive-standalone-metastore-3.1.0-hw-ei-302001-SNAPSHOT.jar:$HIVE_HOME/lib/hive-exec-3.1.0-hw-ei-302001-SNAPSHOT.jar:$HCAT_HOME/lib/libfb303-0.9.3.jar:$HADOOP_HOME/etc/hadoop:$HCAT_HOME/conf:$HCAT_HOME/lib/slf4j-api-1.7.30.jar:$HCAT_HOME/lib/antlr-3.5.2.jar:$HCAT_HOME/lib/jdo-api-3.0.1.jar:$HCAT_HOME/lib/antlr-runtime-3.5.2.jar:$HCAT_HOME/lib/datanucleus-api-jdo-4.2.4.jar:$HCAT_HOME/lib/datanucleus-core-4.1.17.jar:$HCAT_HOME/lib/datanucleus-rdbms-fi-4.1.19-SNAPSHOT.jar:$HCAT_HOME/lib/log4j-api-2.10.0.jar:$HCAT_HOME/lib/log4j-core-2.10.0.jar
```

注意事先核对一下实际环境中是否有相关jar包，如果版本号对不上，则需要根据实际情况修改上述命令

5、提交任务       

（1）客户端验证

cd /opt/client   

source bigdata_env

kinit 拥有hive和yarn权限的用户



（2）提交任务

```
yarn --config $HADOOP_HOME/etc/hadoop jar <path_to_jar> <main_class> -libjars $LIB_JARS t1 t2
例：
yarn --config $HADOOP_HOME/etc/hadoop jar /opt/hcatalog-example-1.0-SNAPSHOT.jar com.huawei.bigdata.HCatalogExample -libjars $LIB_JARS t1 t2
```