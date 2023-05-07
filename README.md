# Huawei Cloud MRS example code

The Huawei Cloud MRS example code include HBase, HDFS, Hive, Kafka, Mapreduce, Presto, Spark, Storm. You can get started in minutes using **Maven**.

### Version Mapping:

- Branch  mrs-2.0 is for MRS 2.0.X version

- Branch mrs-1.9 is for MRS 1.9.X version

- Branch mrs-1.8 is for MRS 1.8.X version

  Components mapping:

| Component\MRS version | MRS 1.8.X | MRS 1.9.X | MRS 2.0.X           |
| --------------------- | --------- | --------- | ------------------- |
| Zookeeper             | 3.5.1     | 3.5.1     | 3.5.1               |
| Hadoop                | 2.8.3     | 2.8.3     | 3.1.1               |
| HBase                 | 1.3.1     | 1.3.1     | 2.1.1               |
| OpenTSDB              | 2.3.0     | 2.3.0     | NA                  |
| Tez                   | NA        | 0.9.1     | 0.9.1               |
| Hive                  | 1.3.0     | 2.3.3     | 3.1.0               |
| Hive_Spark            | 1.2.1     | 1.2.1     | 1.2.1               |
| Spark                 | 2.2.1     | 2.2.2     | 2.3.2               |
| Carbon                | 1.6.1     | 1.6.1     | 1.5.1               |
| Presto                | 0.215     | 0.216     | 308                 |
| Kafka                 | 1.1.0     | 1.1.0     | 1.1.0               |
| KafkaManager          | 1.3.3.18  | 1.3.3.1   | 1.3.3.18            |
| Flink                 | 1.7.0     | 1.7.0     | NA(Plan to support) |
| Storm                 | 1.2.1     | 1.2.1     | 1.2.1               |
| Flume                 | 1.6.0     | 1.6.0     | 1.6.0               |
| Hue                   | 3.11.0    | 3.11.0    | 3.11.0              |
| Loader(Sqoop)         | 1.99.7    | 1.99.7    | 1.99.7              |



### Quick Links:

- [Sample Course](https://education.huaweicloud.com:8443/courses/course-v1:HuaweiX+CBUCNXE006+Self-paced/about?isAuth=0&cfrom=hwc), can get the introductory tutorial of MRS.
- [MRS Homepage](https://www.huaweicloud.com/en-us/product/mrs.html), or Chinese language site [MapReduce服务](https://www.huaweicloud.com/product/mrs.html)
- [Deveployer Guide](https://support.huaweicloud.com/devg-mrs/mrs_06_0001.html)
- [MRS Forum](https://bbs.huaweicloud.com/forum/forum-612-1.html)

## Getting Started

For more details, please ref to MRS's [Deveployer Guide](https://support.huaweicloud.com/devg-mrs/mrs_06_0001.html).

### Requirements

To run the examples required:

- Java 1.8+
- Maven 3.0+



#### Specify the Maven Repository

​	Set the repository in your local **setting.xml** of maven

```
<mirror>
    <id>repo2</id>
    <mirrorOf>central</mirrorOf>
    <url>http://repo2.maven.org/maven2/</url>
</mirror>
<profile>
    <id>huaweicloudsdk</id>
    <repositories>
        <repository>
            <id>huaweicloudsdk</id>
            <url>https://repo.huaweicloud.com/repository/maven/huaweicloudsdk/</url>
            <releases><enabled>true</enabled></releases>
            <snapshots><enabled>true</enabled></snapshots>
        </repository>
    </repositories>
</profile>
<activeProfile>huaweicloudsdk</activeProfile>
```

## Building From Source

Once you check out the code from GitHub, you can build it using maven for every child project, eg:

```
cd huaweicloud-mrs-example\src\presto-examples
mvn clean install
```

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)