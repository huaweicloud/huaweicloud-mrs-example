package com.huawei.mrs.demo.board.writer;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import com.huawei.mrs.demo.board.util.MysqlParams;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class JavaAbstractMysqlWriter extends ForeachWriter<Row> {

    private String jdbcURL = null;
    private String user = null;
    private String password = null;

    private Connection connection;
    Statement statement;

    JavaAbstractMysqlWriter(String url, String user, String password) {
        this.jdbcURL = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public boolean open(long partitionId, long version) {
        try {
            Class.forName(MysqlParams.DRIVER);
            connection = DriverManager.getConnection(jdbcURL, user, password);
            this.statement = connection.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public void close(Throwable errorOrNull) {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
