## hbase-rest-example

1. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行***source bigdata_env***操作后，***kinit username*** 进行认证。

2. 将用户认证凭据文件放到***src/main/resources/conf***目录下

3. 修改***HBaseRestTest.java main()***  方法里面的***principal***为用户名
   修改***restHostName***为欲访问的***RestServer***实例所在节点IP地址，并将访问节点IP配置到运行样例代码的本机***hosts*** 文件中。
4. 运行该样例项目前需保证***HBase***集群的***default namespace***中存在***t1***表

> >客户端目录下***source bigdata_env*** ,然后***hbase shell***进入hbase shell
> >
> >***list*** 检查集群中表情况，若无需创建
> >
> >***eg:create 't1','cf1','cf2'***

5. 根据pom.xml 使用maven构建导包

6. 运行***HBaseRestTest.java*** 



