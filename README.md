# Huawei Cloud MRS example code

The Huawei Cloud MRS example code include HBase, HDFS, Hive, Kafka, Mapreduce, Presto, Spark, Storm. You can get started in minutes using **Maven**.

### MRS component versions:

[MRS component versions](https://support.huaweicloud.com/intl/en-us/productdesc-mrs/mrs_08_0005.html)

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