# Huawei Cloud MRS example code

The Huawei Cloud MRS example code include HBase, HDFS, Hive, Kafka, Mapreduce, Presto, Spark, Storm etc. You can get started in minutes using **Maven**.

### Version Mapping:

MRS_3.0.2 Components mapping:

| Component\MRS version | MRS 3.0.2 |
| --------------------- | --------- |
| Flink                 | 1.10.0    |
| Hive                  | 3.1.0     |
| Tez                   | 0.9.2     |
| Spark                 | 2.4.5     |
| CarbonData            | 2.0.0     |
| Hadoop                | 3.1.1     |
| HBase                 | 2.2.3     |
| ZooKeeper             | 3.5.6     |
| Hue                   | 4.7.0     |
| Oozie                 | 5.1.0     |
| Flume                 | 1.9.0     |
| Kafka                 | 2.4.0     |
| Ranger                | 2.0.0     |
| GeoMesa               | 2.4.0     |
| Storm                 | 1.2.0     |
| Solr                  | 8.4.0     |
| Phoenix               | 5.0.0     |
| ES                    | 7.6.0     |

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

 Set the repository in your local **setting.xml** of maven

```
<mirror>
    <id>repo2</id>
    <mirrorOf>central</mirrorOf>
    <url>http://repo1.maven.org/maven2/</url>
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
cd src\presto-examples
mvn clean install
```

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)