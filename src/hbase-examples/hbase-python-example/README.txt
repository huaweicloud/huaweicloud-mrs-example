Before using python to operate hbase, you need to check your environment as following steps. please install them if they
 don't be installed in your environment:
1. configuration yum repository and Installation dependences:
    yum install automake libtool flex bison pkgconfig gcc-c++ boost-devel zlib-devel openssl-devel python-devel

2.Searching python dependences in website (https://pypi.org/project) and Installing them:
    2.1.thrift
    2.2.pure-sasl/kerberos/gssapi/decorator/krbcontext(normal cluster could skip this step)

3.Commissioning program:
    3.1.Modify the sample code:
        Modify "host" as the hostname of the thriftServer.
        Modify "port" as the port of the thriftServer (default: 9090).
        SecureMode = False if the current cluster is a normal cluster.
        SecureMode = True if it is a secure cluster.
        if your cluster is secure:
            1. create a user in MRS Manager, download it and upload it to your cluster. please refrence:https://support.huaweicloud.com/devg-mrs/mrs_06_0010.html
            2.modify "userPrincipal", such as:"hbaseuser@0387E634_69F0_406F_8633_33E788C1E162.COM"
            3.modify "keytableFilePath", such as: "/opt/client/HBase/python-example/user.keytab"

    3.2. Creating a hbase table:
        source /opt/client/bigdata_env
        kinit -kt /opt/client/HBase/python-example/user.keytab hbaseuser (normal cluster can skip this step)
            Note: If you don't know how to get userPrincal, you can get it by using the command "klit -kt /opt/client/HBase/python-example/user.keytab".
        echo "create 'example','family1','family2'" | hbase shell

    3.2. Running the sample code:
    cd /opt/client/HBase/python-example
    python DemoClient.py
        Note: please open a new window and do not execute the command "source /opt/client/bigdata_env".

Note: Before running the sample code, ensuring that the table "example" with the column family is "family1" and
"family2" already exists your cluster.