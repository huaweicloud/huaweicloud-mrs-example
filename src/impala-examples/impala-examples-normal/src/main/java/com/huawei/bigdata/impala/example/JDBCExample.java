package com.huawei.bigdata.impala.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Simple example for hive jdbc.
 */
public class JDBCExample {
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    private final ClientInfo clientInfo;

    public JDBCExample(ClientInfo clientInfo) {
        this.clientInfo = clientInfo;
    }

    public void run() throws ClassNotFoundException, SQLException {

        //Define impala sql, the sql can not include ";"
        String[] sqls = {
            "CREATE TABLE IF NOT EXISTS employees_info(id INT,name STRING)",
            "SELECT COUNT(*) FROM employees_info", "DROP TABLE employees_info"
        };

        StringBuilder sBuilder = new StringBuilder(
            "jdbc:hive2://").append(clientInfo.getImpalaServer()).append("/default");

        sBuilder.append(";auth=noSasl");
        String url = sBuilder.toString();
        Class.forName(HIVE_DRIVER);
        try (Connection connection = DriverManager.getConnection(url)) {
            execDDL(connection, sqls[0]);
            System.out.println("Create table success!");

            execDML(connection, sqls[1]);

            execDDL(connection, sqls[2]);
            System.out.println("Delete table success!");
        }
    }

    public static void execDDL(Connection connection, String sql)
        throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    public static void execDML(Connection connection, String sql) throws SQLException {
        ResultSetMetaData resultMetaData = null;

        try (PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery()) {

            /*
              Print the column name to console
             */
            resultMetaData = resultSet.getMetaData();
            int columnCount = resultMetaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(resultMetaData.getColumnLabel(i) + '\t');
            }
            System.out.println();

            /*
              Print the query result to console
             */
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(resultSet.getString(i) + '\t');
                }
                System.out.println();
            }
        }
    }

}
