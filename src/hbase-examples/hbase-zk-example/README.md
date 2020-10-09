## hbase-zk-example

1. 样例代码需要使用用户认证凭据，在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行***source bigdata_env***操作后，***kinit username*** 进行认证，需将访问节点配置到运行样例代码的本机***hosts*** 文件中。

2. 将用户认证凭据文件放到***src/main/resources***目录下

   > 将客户端中***hbase***目录下***core-site.xml、hbase-site.xml、hdfs-site.xml***放到***src/main/resources***目录下

3. 修改***TestZKSample.java*** 方法里面的***principal***为用户名
   修改***jaas.conf***中***Client_new***的***keyTab***值为本机user.keytab的绝对路径，***principal***为用户名

4. 根据pom.xml 使用maven构建导包
5. 运行***TestZKSample.java*** 

***Tips:connectApacheZK()***函数为连接开源zookeeper源使用，若未安装，进行配置，最后报错：***org.apache.zookeeper.KeeperException$ConnectionLossException***为正常现象



