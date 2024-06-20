package com.huawei.bigdata.spark.examples;

import com.huawei.hadoop.security.KerberosUtil;
import com.huawei.hadoop.security.LoginUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hive.jdbc.HiveDatabaseMetaData;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class ThriftServerQueriesTest {
    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
        String userPrincipal = "sparkuser";
        String userKeytabPath = "/opt/FIclient/user.keytab";
        String krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf";
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfPath);
        String principalName = KerberosUtil.getKrb5DomainRealm();
        String ZKServerPrincipal = "zookeeper/hadoop." + principalName;

        String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
        String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

        Configuration hadoopConf = new Configuration();
        LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeytabPath);
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal);
        LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

        String securityConfig =
                ";saslQop=auth-conf;auth=KERBEROS;principal=spark2x/hadoop."
                        + principalName
                        + "@"
                        + principalName
                        + ";user.principal=sparkuser;user.keytab=/opt/FIclient/user.keytab;";
        Configuration config = new Configuration();
        config.addResource(new Path(args[0]));
        String zkUrl = config.get("spark.deploy.zookeeper.url");

        String sparkConfPath = args[1];
        Properties fileInfo;
        InputStream fileInputStream = null;
        try {
            fileInfo = new Properties();
            File propertiesFile = new File(sparkConfPath);
            fileInputStream = new FileInputStream(propertiesFile);
            // Load the "spark-defaults.conf" configuration file
            fileInfo.load(fileInputStream);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }

        String zkNamespace;
        zkNamespace = fileInfo.getProperty("spark.thriftserver.zookeeper.namespace");
        if (zkNamespace != null) {
            // Remove redundant characters from configuration items
            zkNamespace = zkNamespace.substring(1);
        }

        String url = "jdbc:hive2://"
            + zkUrl
            + "/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace="
            + zkNamespace
            + securityConfig;

        ArrayList<String> sqlList = new ArrayList<>();
        sqlList.add(
                "CREATE TABLE IF NOT EXISTS CHILD (NAME STRING, AGE INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY"
                    + " ','");
        sqlList.add("LOAD DATA LOCAL INPATH '/home/data' INTO TABLE CHILD");
        sqlList.add("SELECT * FROM child");
        sqlList.add("DROP TABLE child");
        executeSql(url, sqlList);
    }

    static void executeSql(String url, ArrayList<String> sqls) throws ClassNotFoundException, SQLException {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Connection connection = null;
        PreparedStatement statement = null;

        try {
            connection = DriverManager.getConnection(url);
            HiveDatabaseMetaData metaData = (HiveDatabaseMetaData) connection.getMetaData();
            String productName = metaData.getDatabaseProductName();
            String appId = metaData.getDatabaseAppId();
            if (null != productName && "Spark SQL".equals(productName)) {
                System.out.println("Connected to:" + productName);
                System.out.println("Funning with YARN Application = " + appId);
            }

            for (int i = 0; i < sqls.size(); i++) {
                String sql = sqls.get(i);
                System.out.println("---- Begin executing sql: " + sql + " ----");
                statement = connection.prepareStatement(sql);
                ResultSet result = statement.executeQuery();
                ResultSetMetaData resultMetaData = result.getMetaData();
                Integer colNum = resultMetaData.getColumnCount();
                for (int j = 1; j <= colNum; j++) {
                    System.out.print(resultMetaData.getColumnLabel(j) + "\t");
                }
                System.out.println();

                while (result.next()) {
                    for (int j = 1; j <= colNum; j++) {
                        System.out.print(result.getString(j) + "\t");
                    }
                    System.out.println();
                }
                System.out.println("---- Done executing sql: " + sql + " ----");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != statement) {
                statement.close();
            }
            if (null != connection) {
                connection.close();
            }
        }
    }
}
