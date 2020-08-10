package com.huawei.bigdata.spark.examples;

import com.huawei.hadoop.security.LoginUtil;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;

/**
 * Example code for connecting spark JDBCServer
 *
 * Before Running:
 * 1. get your hive-site.xml from MRS client to resources folder
 * 2. get your core-site.xml from MRS client to resources folder (only for kerberos enabled cluster)
 * 3. put user.keytab in the conf/user.keytab relative to the running directory and set correct principal in the code (only for kerberos enabled cluster)
 * 4. put some data into /home/data folder on hdfs
 * 5. run using command like shown below
 */

// java -cp /path/to/SparkThriftServerExample-1.0.jar:/opt/client/Spark/spark/jars/* com.huawei.bigdata.spark.examples.ThriftServerQueriesTest
public class ThriftServerQueriesTest {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {

        StringBuilder sb  = new StringBuilder()
                .append("jdbc:hive2://ha-cluster/default;");
        Configuration hadoopConf = new Configuration();
        if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))){
            //security mode

            final String userPrincipal = "sparkuser";
            final String userKeytab = "user.keytab";
            String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            String krbFile = filePath + "krb5.conf";
            String userKeyTableFile = filePath + userKeytab;

            String ZKServerPrincipal = "zookeeper/hadoop.hadoop.com";

            String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
            String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userPrincipal, userKeyTableFile);
            LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZKServerPrincipal);
            LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf);

            String securityConfig = "saslQop=auth-conf;auth=KERBEROS;principal=spark/hadoop.hadoop.com@HADOOP.COM;auth=KERBEROS;user.principal=" +
                    userPrincipal +
                    ";user.keytab=" +
                    userKeyTableFile;
            sb.append(securityConfig);
        }
        String connectUrl = sb.toString();

        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add("CREATE TABLE IF NOT EXISTS CHILD (NAME STRING, AGE INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','");
        sqlList.add("LOAD DATA INPATH '/home/data' INTO TABLE CHILD");
        sqlList.add("SELECT * FROM child");
        sqlList.add("DROP TABLE child");
        executeSql(connectUrl, sqlList);
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

