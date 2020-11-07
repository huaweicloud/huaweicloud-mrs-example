package com.huawei.mrs.demo.board.util;

public interface MysqlParams {
    String JDBC_URL = "jdbc:mysql://ip:3306";
    String USER = "maxwell";
    String PASSWORD = "";

    String DATABASE_NAME = "demo";
    String TABLE_ORDERS = "orders";
    String COL_PAY_HISTORY = "pay_history";
    String COL_PAY_TODAY = "pay_today";

    String DRIVER = "com.mysql.jdbc.Driver";
}
