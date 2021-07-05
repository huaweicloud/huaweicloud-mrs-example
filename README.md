# Huawei Cloud MRS example code

The Huawei Cloud MRS example code include HBase, HDFS, Hive, Kafka, Mapreduce, HetuEngine, Spark, Storm etc. You can get started in minutes using **Maven**.

### Version Mapping:

MRS_3.1.1 Components mapping:

| Component\MRS version | MRS 3.1.1 |
| --------------------- | --------- |
| Flink                 | 1.12.2    |
| Hive                  | 3.1.0     |
| Tez                   | 0.9.2     |
| Spark                 | 3.1.1 |
| CarbonData            | 2.2.0  |
| Hudi | 0.8.0 |
| Hadoop                | 3.1.1     |
| HBase                 | 2.2.3     |
| ZooKeeper             | 3.6.3  |
| Hue                   | 4.7.0     |
| Oozie                 | 5.1.0     |
| Flume                 | 1.9.0     |
| Kafka                 | 2.4.0     |
| Ranger                | 2.0.0     |
| Storm                 | 1.2.1    |
| Solr                  | 8.4.0     |
| Phoenix               | 5.0.0     |
| Elasticsearch       | 7.10.2    |
| ClickHouse            | 21.3.4.25 |
| IoTDB                 | 0.12.0    |
| Redis                 | 6.0.12    |
| HetuEngine | 1.2.0 |

### Quick Links:

- [Sample Course](https://education.huaweicloud.com:8443/courses/course-v1:HuaweiX+CBUCNXE006+Self-paced/about?isAuth=0&cfrom=hwc), can get the introductory tutorial of MRS.
- [MRS Homepage](https://www.huaweicloud.com/en-us/product/mrs.html), or Chinese language site [MapReduce服务](https://www.huaweicloud.com/product/mrs.html)
- [Deveployer Guide](https://support.huaweicloud.com/devg-mrs/mrs_06_0001.html)
- [FusionInsight Forum](https://bbs.huaweicloud.com/forum/forum-1103-1.html)
- [MRS Forum](https://bbs.huaweicloud.com/forum/forum-612-1.html)

## Getting Started

For more details, please ref to MRS's [Deveployer Guide](https://support.huaweicloud.com/devg-mrs/mrs_06_0001.html).

### Requirements

To run the examples required:

- Java 1.8+
- Maven 3.0+

#### Specify the Maven Repository

Add the following open source mirror repository address to **mirrors** in the **settings.xml** configuration file.

```
<mirror>
    <id>repo2</id>
    <mirrorOf>central</mirrorOf>
    <url>https://repo1.maven.org/maven2/</url>
</mirror>
```

Add the following mirror repository address to **profiles** in the **settings.xml** configuration file.

```
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
<profile>
    <id>JDK1.8</id>
    <activation>
        <activeByDefault>true</activeByDefault>
        <jdk>1.8</jdk>
    </activation>
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <project.reporting.outputEncoding>UTF-8</project.reporting.outputEncoding>
        <maven.compiler.encoding>UTF-8</maven.compiler.encoding>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <maven.compiler.compilerVersion>1.8</maven.compiler.compilerVersion>
    </properties>
</profile>
```

Add the following mirror repository address to the **activeProfiles** node in the **settings.xml** file.

```
<activeProfile>huaweicloudsdk</activeProfile>
```

## Building From Source

Once you check out the code from GitHub, you can build it using maven for every child project, eg:

```
cd src\hdfs-example-security
mvn clean install
```

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html)
