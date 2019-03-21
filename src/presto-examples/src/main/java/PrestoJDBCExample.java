import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PrestoJDBCExample {

    private static Connection connection;
    private static Statement statement;

    /**
     * Only when Kerberos authentication enabled, configurations in presto-examples/conf/presto.properties
     * should be set. More details please refer to https://prestodb.io/docs/0.215/installation/jdbc.html.
     *
     * @param url         jdbc connection url
     * @param krbsEnabled authentication enables or not
     * @throws SQLException
     */
    private static void initConnection(String url, boolean krbsEnabled) throws SQLException {
        if (krbsEnabled) {
            String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            File proFile = new File(filePath + "presto.properties");
            if (proFile.exists()) {
                Properties props = new Properties();
                try {
                    props.load(new FileInputStream(proFile));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                connection = DriverManager.getConnection(url, props);
            }
        } else {
            connection = DriverManager.getConnection(url, "presto", null);
        }
        statement = connection.createStatement();
    }

    /**
     * release statement and connection
     *
     * @throws SQLException
     */
    private static void releaseConnection() throws SQLException {
        statement.close();
        connection.close();
    }

    public static void main(String[] args) throws SQLException {
        try {
            /**
             * Replace example_ip with your cluster presto server ip.
             * By default, Kerberos authentication disabled cluster presto service port is 7520, Kerberos authentication
             * enabled cluster presto service port is 7521
             * The postfix /tpcds/sf1 means to use tpcds catalog and sf1 schema, you can use hive catalog as well
             * If Kerberos authentication enabled, set the second param to ture. see PrestoJDBCExample#initConnection(java
             * .lang.String, boolean).
             */
            initConnection("jdbc:presto://example_ip:7520/tpcds/sf1", false);

            ResultSet resultSet = statement.executeQuery("select * from call_center");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("cc_name") + " : " + resultSet.getString("cc_employees"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection();
        }
    }
}
