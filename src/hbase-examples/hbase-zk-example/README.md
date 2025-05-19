## hbase-zk-example

1. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行 ***source bigdata_env*** 操作后，***kinit username*** 进行认证，需将访问节点配置到运行样例代码的本机 ***hosts*** 文件中。

2. 将用户认证凭据文件放到 ***src/main/resources*** 目录下

   根据 __《HBase开发指南》1.2.1准备开发和运行环境__  章节中所述客户端配置文件解压路径“FusionInsight_Cluster_1_Services_ClientConfig_ConfigFiles\HBase\config”，获取HBase相关配置文件 ***core-site.xml、hbase-site.xml、hdfs-site.xml*** 放到 ***“src/main/resources”*** 目录下

3. 修改 ***TestZKSample.java*** 文件 __testSample()__ 方法中的 ***principal*** 为用户名
   
   修改 ***jaas.conf*** 中 ***Client_new*** 的***principal*** 为用户名、 ***keyTab*** 值为 __运行样例的主机__ 保存user.keytab的绝对路径（注意 区别Windows下与Linux下路径书写）

4. __Linux环境下__ 运行时：

   若在 __安装客户端的Linux环境下运行__，需按照 __《HBase开发指南》1.4.2.1安装客户端时编译并运行程序__ 章节，将对应配置文件和认证文件放置到 ***“$BIGDATA_CLIENT_HOME/HBase/hbase/conf”*** 目录。

   若在 __未安装客户端Linux环境下运行__，需按照 __《HBase开发指南》1.4.2.2未安装客户端时编译并运行程序__ 章节，创建对应目录lib和conf，并分别上传对应依赖Jar包和配置文件的配置文件及认证文件。
5. 选择需要编译的目标JDK版本，若不指定，默认编译JDK 8版本的jar包；编译时使用-P参数可以指定目标jar包版本，目前支持的有：
   - -P build-with-jdk8
   - -P build-with-jdk17
   - -P build-with-jdk21

   注意：请不要使用低版本JDK编译高版本JDK的jar包
6. 根据pom.xml 使用maven构建导包

7. 运行 ***TestZKSample.java***
   注意：使用JDK8以上的版本运行前，需要在启动命令或配置中添加以下启动参数：
   --add-modules jdk.unsupported --add-opens java.base/java.nio=ALL-UNNAMED

***Tips*** :  
A.***connectApacheZK()*** 函数为连接开源zookeeper源使用，若未安装，进行配置，最后报错： ***org.apache.zookeeper.KeeperException$ConnectionLossException*** 为正常现象  
B.当集群中ZooKeeper开启SSL加密通信时，本样例不适用。


