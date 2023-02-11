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
 * JDBC查询数据样例
 *
 * @since 2020-11-05
 */
public class SqlSearch {
    private static final Logger LOG = LogManager.getLogger(SqlSearch.class);

    private static void basicSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT * FROM example-sql1 limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.info("Search failed.");
        }
    }

    private static void fieldSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT user, age FROM example-sql1 limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
        } catch (SQLException e) {
            LOG.info("Search failed.");
        }
    }

    private static void insteadFieldSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT age AS user_age FROM example-sql1 limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void distinctSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT DISTINCT age FROM example-sql1 limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void whereAndLimitSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT user, age FROM example-sql1 WHERE height>? limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 100);
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void groupByAndAliasSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT weight AS user_weight FROM example-sql1 GROUP BY user_weight limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void functionsSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT ADD(weight,10) AS user_weight FROM example-sql1 GROUP BY ADD(weight,10) limit 10";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void havingSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT age, MAX(weight) FROM example-sql1 GROUP BY age HAVING MIN(weight)>? limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 20);
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void orderBySearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT height FROM example-sql1 ORDER BY height DESC limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void joinSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT l.height,l.weight,i.height,i.weight "
                + "FROM example-sql1 l JOIN example-sql2 i ON i.age = l.age limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void subQuerySearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT a.myHeight, a.myWeight FROM "
                + "(SELECT height AS myHeight, weight AS myWeight FROM example-sql1 WHERE age > ? LIMIT 5) AS a";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setInt(1, 25);
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void matchSearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT user, weight, address FROM example-sql1 WHERE MATCH_QUERY(address, 'Holmes') limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    private static void querySearch(Connection connection) {
        if (connection == null) {
            LOG.error("Connection is null.");
            return;
        }
        String sql = "SELECT user, weight, address FROM example-sql1 "
                + "WHERE QUERY('address:Lane OR address:Street') limit 10";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeQuery();
            LOG.info("Search succeed.");
        } catch (SQLException e) {
            LOG.error("Search failed.");
        }
    }

    /**
     * 调用所有查询样例
     *
     * @param connection 初始化的connection
     */
    public static void allSearch(Connection connection) {
        basicSearch(connection);
        fieldSearch(connection);
        insteadFieldSearch(connection);
        distinctSearch(connection);
        whereAndLimitSearch(connection);
        groupByAndAliasSearch(connection);
        functionsSearch(connection);
        havingSearch(connection);
        orderBySearch(connection);
        joinSearch(connection);
        subQuerySearch(connection);
        matchSearch(connection);
        querySearch(connection);
    }
}