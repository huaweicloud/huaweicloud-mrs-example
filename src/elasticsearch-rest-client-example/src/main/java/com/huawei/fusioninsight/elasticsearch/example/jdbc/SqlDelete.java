/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 数据删除样例
 *
 * @since 2020-11-05
 */
public class SqlDelete {
    private static final Logger LOG = LogManager.getLogger(SqlDelete.class);

    /**
     * 删除数据
     *
     * @param connection Sql connection
     */
    public static void deleteData(Connection connection) {
        if (connection == null) {
            LOG.error("Statement is null.");
            return;
        }
        String sql = "DELETE FROM example-sql2 WHERE height=? ";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 25);
            statement.executeQuery();
            LOG.info("Delete data successful.");
        } catch (SQLException e) {
            LOG.error("Delete data failed.");
        }
    }
}