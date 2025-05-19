## hbase-example

1. 本样例项目操作逻辑包含 ***HBaseSample.java***、***PhoenixSample.java***、***HBaseDualReadSample.java***、***GlobalSecondaryIndexSample***

   其中登录逻辑包含单集群登录 ***TestMain.java*** ，多集群互信场景的多集群登录 ***TestMultiLogin.java***

2. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行 ***source bigdata_env*** 操作后，***kinit username*** 进行认证，需将访问节点配置到运行样例代码的本机 ***hosts*** 文件中。

3. 登录逻辑中
  - 单集群
    
    将用户认证凭据文件放到 ***src/main/resources/conf*** 目录下
    根据 __《HBase开发指南》1.2.1准备开发和运行环境__  章节中所述客户端配置文件解压路径“FusionInsight_Cluster_1_Services_ClientConfig_ConfigFiles\HBase\config”，获取HBase相关配置文件 ***core-site.xml、hbase-site.xml、hdfs-site.xml*** 放到 ***src/main/resources/conf*** 目录下
    
  - 多集群

    将互信场景下的同名用户其中一个集群的认证凭据及其配置文件放入 ***src/main/resources/hadoopDomain*** 目录下，将另一集群的配置文件放入 ***src/main/resources/hadoop1Domain*** 目录下

  - 注意：如果集群为IPv6环境，需要将hbase-site.xml中hbase.zookeeper.quorum配置项对应值的IP修改为集群中ZooKeeper quorumpeer实例所在节点的主机名。

4. 修改 ***TestMain.java*** 文件 USER_NAME = "***hbaseuser***"为实际用户名

5. 修改 ***TestMain.java*** 文件 ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/hadoop.***YourDomainName***"，其中 ***YourDomainName*** 的值可通过在manager界面单击“系统 > 权限 > 域和互信”，然后查看“本端域”参数获取。

6. __Linux环境下__ 运行时：

   修改 TestMain.java中***login ()*** 和Utils.java中 ***createClientConf ()*** 方法中获取路径的逻辑。使用注释中标识Linux环境下的读取方式。

   > eg:修改TestMain.login()
   >
   > ```java
   > String userDir = System.getProperty("user.dir") + File.separator + Utils.CONF_DIRECTORY + File.separator;
   > ```
   > 修改Utils.createClientConf()
   >
   > ```java
   > String userDir = System.getProperty("user.dir") + File.separator + CONF_DIRECTORY + File.separator;
   > ```
   >
   > 修改TestMultiLogin.login()
   >
   > ```java
   > String userdir = System.getProperty("user.dir") + File.separator + confDir + File.separator;
   > ```
   >
   > 修改HBaseDualReadSample.setHbaseDualReadParam()
   > ```java
   > String userDir = System.getProperty("user.dir") + File.separator + Utils.CONF_DIRECTORY + File.separator;
   > ```

   若在 __安装客户端的Linux环境下运行__，需按照 __《HBase开发指南》1.4.2.1安装客户端时编译并运行程序__ 章节，将对应配置文件和认证文件放置到 ***“$BIGDATA_CLIENT_HOME/HBase/hbase/conf”*** 目录。

   若在 __安装客户端的Linux环境下运行双读样例__，需按照 __《HBase开发指南》1.4.2.1安装客户端时编译并运行程序__ 章节，将hbase-dualclient Jar包放置到 ***“$BIGDATA_CLIENT_HOME/HBase/hbase/lib”*** 目录。

   若在 __未安装客户端Linux环境下运行__，需按照 __《HBase开发指南》1.4.2.2未安装客户端时编译并运行程序__ 章节，创建对应目录lib和conf，并分别上传对应依赖Jar包和配置文件及认证文件。
7. 选择需要编译的目标JDK版本，若不指定，默认编译JDK 8版本的jar包；编译时使用-P参数可以指定目标jar包版本，目前支持的有：
   - -P build-with-jdk8
   - -P build-with-jdk17
   - -P build-with-jdk21

   注意：请不要使用低版本JDK编译高版本JDK的jar包

8. 根据 ***pom.xml*** 使用maven构建导包
9. 分别运行 ***TestMain.java*** 和 ***TestMultiLogin.java***
   注意：使用JDK8以上的版本运行前，需要在启动命令或配置中添加以下启动参数：
   --add-modules jdk.unsupported --add-opens java.base/java.nio=ALL-UNNAMED
