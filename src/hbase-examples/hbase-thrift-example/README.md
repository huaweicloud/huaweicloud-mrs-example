## hbase-thrift-example

1. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行 ***source bigdata_env*** 操作后，***kinit username*** 进行认证。

2. 在Manager界面修改 ***HBase*** 集群实例的配置参数 ***“hbase.thrift.security.qop"*** ，该值设置值与 ***"hbase.rpc.protection"*** 进行一一对应:

   > ***privacy(auth-conf)*** / ***authentication(auth)*** / ***integrity(auth-int)*** 

   保存，重启配置过期节点服务使更改的配置生效。

3. 将用户认证凭据文件放到 ***src/main/resources/conf*** 目录下。

   该场景下初始化配置需要使用集群中安装ThriftServer的对应节点配置文件 ***core-site.xml、hbase-site.xml、hdfs-site.xml*** ，放到 ***src/main/resources/conf*** 目录下。

   - 方法一：根据 __《HBase开发指南》1.2.3.4访问ThriftServer服务认证__ 章节中所述步骤，获取集群中安装ThriftServer的对应节点配置文件

   - 方法二：若只能通过 __《HBase开发指南》1.2.1准备开发和运行环境__ 章节中所述解压客户端文件的方法获取配置文件，在 ***hbase-site.xml*** 中手动添加以下配置项,其中 ***“hbase.thrift.security.qop"*** 值与步骤2修改保持一致,***“hbase.thrift.keytab.file"*** 中FusionInsight_HD_8.x.x的值与版本保持一致。
      ```xml
      <property>
      <name>hbase.thrift.security.qop</name>
      <value>auth</value>
      </property>
      <property>
      <name>hbase.thrift.kerberos.principal</name>
      <value>thrift/hadoop.hadoop.com@HADOOP.COM</value>
      </property>
      <property>
      <name>hbase.thrift.keytab.file</name>
      <value>/opt/huawei/Bigdata/FusionInsight_HD_8.x.x/install/FusionInsight-HBase-2.4.14/keytabs/HBase/thrift.keytab</value>
      </property>
      ```

4. 修改 ***TestMain.java login()***  方法里面的 ***username*** 为用户名
   
   修改 ***test.test()*** 传入参数为欲访问的 ***ThriftServer*** 实例所在节点IP地址，并将访问节点IP配置到运行样例代码的本机 ***hosts*** 文件中。

   修改 ***THRIFT_PORT*** 为Manager页面中查询得到的配置 ***"hbase.regionserver.thrift.port"*** 参数对应的 ***value***

5. __Linux环境下__ 运行时：

   修改 ***login ()*** 和 ***init ()*** 方法中获取路径的逻辑。使用注释中标识Linux环境下的读取方式。
   
   > eg:修改TestMain.login()和TestMain.init()
   >
   > ```java
   > String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
   > ```
   
   若在 __安装客户端的Linux环境下运行__，需按照 __《HBase开发指南》1.4.2.1安装客户端时编译并运行程序__ 章节，将对应配置文件和认证文件放置到 ***“$BIGDATA_CLIENT_HOME/HBase/hbase/conf”*** 目录。
   
   若在 __未安装客户端Linux环境下运行__，需按照 __《HBase开发指南》1.4.2.2未安装客户端时编译并运行程序__ 章节，创建对应目录lib和conf，并分别上传对应依赖Jar包和配置文件及认证文件。


6. 选择需要编译的目标JDK版本，若不指定，默认编译JDK 8版本的jar包；编译时使用-P参数可以指定目标jar包版本，目前支持的有：
   - -P build-with-jdk8
   - -P build-with-jdk17
   - -P build-with-jdk21

   注意：请不要使用低版本JDK编译高版本JDK的jar包
7. 根据pom.xml 使用maven构建导包

8. 运行 ***TestMain.java***
   注意：使用JDK8以上的版本运行前，需要在启动命令或配置中添加以下启动参数：
   --add-modules jdk.unsupported --add-opens java.base/java.nio=ALL-UNNAMED


