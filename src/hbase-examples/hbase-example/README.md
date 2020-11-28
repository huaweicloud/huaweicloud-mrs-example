## hbase-example

1. 本样例项目操作逻辑包含***HBaseSample.java*** 和 ***PhoenixSample.java***

​        其中登录逻辑包含单集群登录***TestMain.java***，多集群互信场景的多集群登录***TestMultiLogin.java***

2. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行***source bigdata_env***操作后，***kinit username*** 进行认证，需将访问节点配置到运行样例代码的本机***hosts*** 文件中。

3. 单集群登录逻辑中

   > > 将用户认证凭据文件放到***src/main/resources/conf***目录下

   > > 根据__《HBase开发指南》1.2.1准备开发和运行环境__  章节中所述客户端配置文件解压路径“FusionInsight_Cluster_1_Services_ClientConfig_ConfigFiles\HBase\config”，获取HBase相关配置文件***core-site.xml、hbase-site.xml、hdfs-site.xml***放到***src/main/resources/conf***目录下

​      多集群登录逻辑中

> > 将互信场景下的同名用户其中一个集群的认证凭据及其配置文件放入***src/main/resources/hadoopDomain***目录下，将另一集群的配置文件放入***src/main/resources/hadoop1Domain***目录下

4. 修改***login()***  方法里面对应***userName***为用户名

5. __Linux环境下__运行时：

   修改***login（）和 init ()***方法中获取路径的逻辑。使用注释中标识Linux环境下的读取方式。

   > eg:修改TestMain.login()和TestMain.init()
   >
   > ```java
   > String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
   > ```
   > 修改TestMultiLogin.login()
   > ```java
   > String userdir = System.getProperty("user.dir") + File.separator + confDir + File.separator;
   > ```
   > 修改TestMultiLogin.init()
   > ```java
   > String userdir = System.getProperty("user.dir") + File.separator + confDirectoryName + File.separator;
   > ```

   若在__安装客户端的Linux环境下运行__，需按照__《HBase开发指南》1.4.2.1安装客户端时编译并运行程序__章节，将对应配置文件和认证文件放置到***“$BIGDATA_CLIENT_HOME/HBase/hbase/conf”***目录。

   若在__未安装客户端Linux环境下运行__，需按照__《HBase开发指南》1.4.2.2未安装客户端时编译并运行程序__章节，创建对应目录lib和conf，并分别上传对应依赖Jar包和配置文件及认证文件。

6. 根据***pom.xml***使用maven构建导包

7. 分别运行***TestMain.java***和***TestMultiLogin.java***



