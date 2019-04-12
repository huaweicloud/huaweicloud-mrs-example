package com.huawei.mrs.demo.board.writer;

import org.apache.spark.sql.Row;

import java.sql.SQLException;

public class JavaMysqlWriter extends JavaAbstractMysqlWriter {
    private String sqlTemplate = null;

    /**
     * the column name to insert/update
     */
    private String colName = null;

    public JavaMysqlWriter(String url, String user, String password, String sqlTemplate, String colName) {
        super(url, user, password);
        this.sqlTemplate = sqlTemplate;
        this.colName = colName;
    }

    @Override
    public void process(Row value) {
        System.out.println("value -> " + value);
        String sql = String.format(sqlTemplate,
                value.getDouble(value.fieldIndex(colName)),
                value.getDouble(value.fieldIndex(colName))
        );
        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
