## hbase-thrift-example

1. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行***source bigdata_env***操作后，***kinit username*** 进行认证。

2. 修改***HBase***集群实例的配置参数***“hbase.thrift.security.qop"***，该值设置值与***"hbase.rpc.protection"***进行一一对应:

   > ***privacy(auth-conf)***/***authentication(auth)***/***integrity(auth-int)*** 保存生效。

3. 将用户认证凭据文件放到***src/main/resources/conf***目录下

   将配置文件放到指定目录下：

   > 进入集群中安装***ThriftServer***实例对应节点，进入对应实例配置目录.
   >
   > eg:***/opt/huawei/Bigdata/FusionInsight_HD_8.0.2/1_8_ThriftServer/etc***
   >
   > 将该目录中***core-site.xml、hbase-site.xml、hdfs-site.xml***放到***src/main/resources/conf***目录下

4. 修改***TestMain.java login()***  方法里面的***username***为用户名
   修改***test.test()***传入参数为欲访问的***ThiftServer***实例所在节点IP地址，并将访问节点IP配置到运行样例代码的本机***hosts*** 文件中。

   修改***THRIFT_PORT***为***hbase-site.xml***中***"hbase.regionserver.thrift.port"***参数对应的***value***

5. 根据pom.xml 使用maven构建导包

6. 运行***TestMain.java*** 



