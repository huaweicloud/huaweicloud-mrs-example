package com.huawei.bigdata.spark.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import com.huawei.hadoop.security.LoginUtil;

public class ThriftServerQueriesTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {

        Configuration hadoopConf = new Configuration();
        if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))){
            //security mode

            final String userPrincipal = "sparkuser";
            final String USER_KEYTAB_FILE = "user.keytab";
            String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            String krbFile = filePath + "krb5.conf";
            String userKeyTableFile = filePath + USER_KEYTAB_FILE;

            String ZKServerPrincipal = "zookeeper/hadoop.hadoop.com";

            String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
            String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeyTableFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal);
            LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf);
        }

        String securityConfig = ";saslQop=auth-conf;auth=KERBEROS;principal=spark/hadoop.hadoop.com@HADOOP.COM" + ";";
        Configuration config = new Configuration();
        config.addResource(new Path(args[0]));
        String zkUrl = config.get("spark.deploy.zookeeper.url");

        String sparkConfPath = args[1];
        Properties fileInfo = null;
        InputStream fileInputStream = null;
        try {
            fileInfo = new Properties();
            File propertiesFile = new File(sparkConfPath);
            fileInputStream = new FileInputStream(propertiesFile);
            //Load the "spark-defaults.conf" configuration file
            fileInfo.load(fileInputStream);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }

        String zkNamespace = null;
        zkNamespace = fileInfo.getProperty("spark.thriftserver.zookeeper.namespace");
        if (zkNamespace != null) {
            //Remove redundant characters from configuration items
            zkNamespace = zkNamespace.substring(1);
        }

        StringBuilder sb = new StringBuilder("jdbc:hive2://ha-cluster/default"
                + securityConfig);
        String url = sb.toString();

        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add("CREATE TABLE IF NOT EXISTS CHILD (NAME STRING, AGE INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
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

