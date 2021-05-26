## hbase-zk-example

1. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行***source bigdata_env***操作后，***kinit username*** 进行认证，需将访问节点配置到运行样例代码的本机***hosts*** 文件中。

2. 将用户认证凭据文件放到***src/main/resources***目录下

   根据__《HBase开发指南》1.2.1准备开发和运行环境__  章节中所述客户端配置文件解压路径“FusionInsight_Cluster_1_Services_ClientConfig_ConfigFiles\HBase\config”，获取HBase相关配置文件***core-site.xml、hbase-site.xml、hdfs-site.xml***放到***“src/main/resources”***目录下

3. 修改***TestZKSample.java*** 文件__testSample()__方法中的***principal***为用户名
   修改***jaas.conf***中***Client_new***的***keyTab***值为__运行样例的主机__保存user.keytab的绝对路径（注意 区别Windows下与Linux下路径书写），***principal***为用户名

4. __Linux环境下__运行时：

   修改***login（）***方法中获取路径的逻辑。使用注释中标识Linux环境下的读取方式。

   > eg:修改TestZKSample.login()
   >
   > ```java
   > String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
   > ```

   若在__安装客户端的Linux环境下运行__，需按照__《HBase开发指南》1.4.2.1安装客户端时编译并运行程序__章节，将对应配置文件和认证文件放置到***“$BIGDATA_CLIENT_HOME/HBase/hbase/conf”***目录。

   若在__未安装客户端Linux环境下运行__，需按照__《HBase开发指南》1.4.2.2未安装客户端时编译并运行程序__章节，创建对应目录lib和conf，并分别上传对应依赖Jar包和配置文件的配置文件及认证文件。

5. 根据pom.xml 使用maven构建导包

6. 运行***TestZKSample.java*** 

***Tips:connectApacheZK()***函数为连接开源zookeeper源使用，若未安装，进行配置，最后报错：***org.apache.zookeeper.KeeperException$ConnectionLossException***为正常现象



