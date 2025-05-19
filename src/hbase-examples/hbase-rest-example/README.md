## hbase-rest-example

1. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行 ***source bigdata_env*** 操作后，***kinit username*** 进行认证。

2. 将用户认证凭据文件放到 ***src/main/resources/conf*** 目录下，若不存在conf目录，请自行创建。

3. 修改 ***HBaseRestTest.java main()*** 方法里面的 ***principal*** 为用户名
   修改 ***restHostName*** 为欲访问的 ***RestServer*** 实例所在节点IP地址，并将访问节点IP配置到运行样例代码的本机 ***hosts*** 文件中。

5. 当运行集群为 __非安全模式__ 时：

   修改 ***main()*** 方法中test.test()的调用，传入参数为 ***nonSecurityModeUrl***，即使用http协议访问节点服务。

   >eg:修改HBaseRestTest.main()
   >```java
   >test.test(nonSecurityModeUrl);
   >```

6. __Linux环境下__ 运行时：
   若在 __安装客户端的Linux环境下运行__，需按照 __《HBase开发指南》1.4.2.1安装客户端时编译并运行程序__ 章节，将对应认证文件放置到 ***“$BIGDATA_CLIENT_HOME/HBase/hbase/conf”*** 目录。

   若在 __未安装客户端Linux环境下运行__，需按照 __《HBase开发指南》1.4.2.2未安装客户端时编译并运行程序__ 章节，创建对应目录lib和conf，并分别上传对应依赖Jar包和认证文件。

7. 选择需要编译的目标JDK版本，若不指定，默认编译JDK 8版本的jar包；编译时使用-P参数可以指定目标jar包版本，目前支持的有：
   - -P build-with-jdk8
   - -P build-with-jdk17
   - -P build-with-jdk21

   注意：请不要使用低版本JDK编译高版本JDK的jar包
8. 根据pom.xml 使用maven构建导包

9. 运行 ***HBaseRestTest.java*** 



