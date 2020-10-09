## hbase-example

1. 本样例项目操作逻辑包含***HBaseSample.java*** 和 ***PhoenixSample.java***

​        其中登录逻辑包含单集群登录***TestMain.java***，多集群互信场景的多集群登录***TestMultiLogin.java***

2. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行***source bigdata_env***操作后，***kinit username*** 进行认证，需将访问节点配置到运行样例代码的本机***hosts*** 文件中。

3. 单集群登录逻辑中

   > > 将用户认证凭据文件放到***src/main/resources/conf***目录下

   > > 将客户端中***hbase***目录下***core-site.xml、hbase-site.xml、hdfs-site.xml***放到***src/main/resources/conf***目录下

​      多集群登录逻辑中

> > > 将互信场景下的同名用户其中一个集群的认证凭据及其配置文件放入***src/main/resources/hadoopDomain***目录下，将另一集群的配置文件放入***src/main/resources/hadoop1Domain***目录下

4. 修改代码中对应***userName***为用户名
5. 根据***pom.xml***使用maven构建导包
6. 分别运行***TestMain.java***和***TestMultiLogin.java***



