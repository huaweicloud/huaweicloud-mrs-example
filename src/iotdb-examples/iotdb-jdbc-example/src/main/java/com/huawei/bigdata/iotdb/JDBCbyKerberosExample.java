package com.huawei.bigdata.iotdb;

import com.huawei.iotdb.client.security.IoTDBClientKerberosFactory;

import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.ietf.jgss.GSSException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import javax.security.auth.login.LoginException;

public class JDBCbyKerberosExample {
    private static IoTDBProperties iotdbProps = IoTDBProperties.getInstance();

    private static final String IOTDB_SSL_ENABLE = iotdbProps.getValues("iotdb_ssl_enable", "true");

    private static final String JDBC_URL = iotdbProps.getValues("jdbc_url", "jdbc:iotdb://127.0.0.1:22260/");

    private static final String USER = iotdbProps.getValues("username", "iotdb");

    private static final String IOTDB_SSL_TRUSTSTORE = iotdbProps.getValues("iotdb_ssl_truststore", "truststore文件路径");

    private static final String JAVA_KRB5_CONF = "java.security.krb5.conf";
    /**
     * Location of krb5.conf file
     */
    private static final String KRB5_CONF_DEST = iotdbProps.getValues("krb5_conf_dest", "下载的认证凭据中“krb5.conf”文件的位置");
    /**
     * Location of keytab file
     */
    private static final String KEY_TAB_DEST = iotdbProps.getValues("key_tab_dest", "下载的认证凭据中“user.keytab”文件的位置");
    /**
     * User principal
     */
    private static final String CLIENT_PRINCIPAL = iotdbProps.getValues("client_principal", "对应user.keytab的用户名@HADOOP.COM");

    /**
     * Server principal, 'iotdb_server_kerberos_principal' in iotdb-datanode.properties
     */
    private static final String SERVER_PRINCIPAL = "iotdb/hadoop.hadoop.com@HADOOP.COM";

    /**
     * Get kerberos token as password
     * @return kerberos token
     * @throws LoginException loginException
     * @throws GSSException GSSException
     */
    public static String getAuthToken() throws LoginException, GSSException {
        IoTDBClientKerberosFactory kerberosHandler = IoTDBClientKerberosFactory.getInstance();
        System.setProperty(JAVA_KRB5_CONF, KRB5_CONF_DEST);
        kerberosHandler.loginSubjectFromKeytab(CLIENT_PRINCIPAL, KEY_TAB_DEST);
        byte[] tokens = kerberosHandler.generateServiceToken(SERVER_PRINCIPAL);
        return Base64.getEncoder().encodeToString(tokens);
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
        // set iotdb_ssl_enable
        System.setProperty("iotdb_ssl_enable", IOTDB_SSL_ENABLE);
        if ("true".equals(IOTDB_SSL_ENABLE)) {
            // set truststore.jks path
            System.setProperty("iotdb_ssl_truststore", IOTDB_SSL_TRUSTSTORE);
        }

        try (Connection connection =
                        DriverManager.getConnection(JDBC_URL, USER, getAuthToken());
                Statement statement = connection.createStatement()) {
            // set JDBC fetchSize
            statement.setFetchSize(10000);

            try {
                statement.execute("SET STORAGE GROUP TO root.sg1");
                statement.execute(
                        "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
                statement.execute(
                        "CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
                statement.execute(
                        "CREATE TIMESERIES root.sg1.d1.s3 WITH DATATYPE=INT64, ENCODING=RLE, COMPRESSOR=SNAPPY");
            } catch (IoTDBSQLException e) {
                System.out.println(e.getMessage());
            }
        } catch (GSSException | LoginException e) {
            System.out.println(e.getMessage());
        }
    }
}
