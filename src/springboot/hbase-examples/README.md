# HBase/Phoenix对接SpringBoot样例

样例基本功能：使用HBase/Phoenix对接SpringBoot

## 环境准备

* 支持java开发的IDE(例如IDEA)
* maven工具


## 获取用户认证文件与HBase配置文件
1. 样例代码需要使用用户认证凭据，请在manager界面新建用户，为其配置所需权限。（人机用户在第一次登录修改初始密码后才会生效）。并在客户端目录下进行 source bigdata_env 操作后，执行kinit username 进行认证。认证后在manager界面下载该用户的认证凭证。
2. 登录FusionInsight Manager页面，选择“集群 > 概览 > 更多 > 下载客户端”，“选择客户端类型”设置为“仅配置文件”，根据待运行客户端程序节点的节点类型选择正确的平台类型后（x86选择x86_64，ARM选择aarch64）单击“确定”，等待客户端文件包生成后根据浏览器提示下载客户端到本地并解压。
   例如，客户端配置文件压缩包为“FusionInsight_Cluster_1_Services_Client.tar”，解压后得到“FusionInsight_Cluster_1_Services_ClientConfig_ConfigFiles.tar”，继续解压该文件。解压到本地PC的“D:\FusionInsight_Cluster_1_Services_ClientConfig_ConfigFiles”目录下。
3. 进入客户端配置文件解压路径“FusionInsight_Cluster_1_Services_ClientConfig_ConfigFiles\HBase\config”，获取HBase相关配置文件，并放置在同一个目录中，如果集群为IPv6环境，需要将hbase-site.xml中hbase.zookeeper.quorum配置项对应值的IP修改为集群中ZooKeeper quorumpeer实例所在节点的主机名。

## 配置springclient.properties
* principal为所使用用户的用户名
* user.keytab.path为keytab文件路径
* krb5.conf.path为krb5.conf文件路径
* conf.path为HBase配置文件所在目录
* zookeeper.server.principal为zookeeper服务端principal，其格式为“zookeeper/hadoop.系统域名”， 其中系统域名的值可通过在manager界面单击“系统 > 权限 > 域和互信”，然后查看“本端域”参数获取。

## 编译程序
1. 单击IDEA右边Maven窗口的“Reimport All Maven Projects”，进行maven项目依赖import。
2. 配置好springclient.properties后，可以按以下两种方式编译： 
* 方法一：
  选择“Maven > 样例工程名称 > Lifecycle > clean”，双击“clean”运行maven的clean命令。
  选择“Maven > 样例工程名称 > Lifecycle > package”，双击“package”运行maven的package命令。
* 方法二：
  在IDEA的下方Terminal窗口进入“pom.xml”所在目录，手动输入mvn clean package。 

编译时需要选择目标JDK版本，若不指定，默认编译JDK 8版本的jar包；编译时使用-P参数可以指定目标jar包版本，目前支持的有：
- -P build-with-jdk8
- -P build-with-jdk17

注意：请不要使用低版本JDK编译高版本JDK的jar包

## 运行程序
### WINDOWS下
1. 将集群节点配置到运行样例代码的本机 hosts 文件中。
2. 右键“HBaseApplication”文件，选择“Run 'HBaseApplication‘”。
   
### Linux下
1. 创建一个目录作为运行目录，将"target"目录下的hbase-springboot-*.jar放进该路径下，并上传配置文件和用户认证文件到springclient.properties配置的对应路径下。
2. 执行“java -jar hbase-springboot-*.jar”运行程序。

## 发送HTTP请求
在浏览器上访问链接：“http://运行节点ip:8080/hbase/HBaseDemo”和“http://运行节点ip:8080/hbase/PhoenixDemo”，IDEA正常打印日志，请求返回“finish HBase”和“finish Phoenix"