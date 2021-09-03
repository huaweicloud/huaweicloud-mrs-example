import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PrestoJDBCExample {
    public static void main(String[] args) {
        /*
         *  Replace example_ip with your cluster presto server ip.
         *  By default, Kerberos authentication disabled cluster presto service port is 7520
         *  The postfix /tpcds/sf1 means to use tpcds catalog and sf1 schema, you can use hive catalog as well.
        */
        final String url = "jdbc:presto://172.16.0.247:7520/tpcds/sf1";
        try (Connection connection = DriverManager.getConnection(url, "presto", null);
                Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select * from call_center");
            while (resultSet.next()) {
                System.out.println(resultSet.getString("cc_name") + " : " + resultSet.getString("cc_employees"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
