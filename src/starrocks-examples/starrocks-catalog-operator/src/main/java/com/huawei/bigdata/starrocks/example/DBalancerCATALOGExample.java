/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 */

package com.huawei.bigdata.starrocks.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class DBalancerCATALOGExample {
	private static final Logger logger = LogManager.getLogger(DBalancerCATALOGExample.class);
	private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d?characterEncoding=utf-8&serverTimezone=Asia/Shanghai";
	private static final String HOST = "xxx";// DBalancer Node Host
	private static final int PORT = 0;   // balancer_tcp_port of DBalancer Node
	// Before running this example, set the environment variables STARROCKS_MY_USER and STARROCKS_MY_PASSWORD in the local environment variables.
	// It is recommended that ciphertext be stored and decrypted during use to ensure security.
	private static final String USER = System.getenv("STARROCKS_MY_USER");
	private static final String PASSWD = System.getenv("STARROCKS_MY_PASSWORD");


	public static void main(String[] args) {
		// Don't add a semicolon at the end.
		String showCatalogSql = "show catalogs";
		String switchCatalogSql = "set catalog default_catalog";
		String queryDatabaseSql = "show databases";
		String useDatabaseSql = "use test_kerbers";
		String queryTableSql = "show tables";
		String querySql = "select * from sr_hdfs_txt";
		logger.info("Start execute starrocks example.");

		logger.info("Start execute starrocks example.");
		try (Connection connection = createConnection()) {
			// query catalog
			logger.info("Start show defaultCatalog.");
			execDDL(connection, showCatalogSql);
			logger.info("Show defaultCatalog successfully.");

			// switch catalog
			logger.info("Start switch Catalog.");
			execDDL(connection, switchCatalogSql);
			logger.info("Switch Catalog successfully.");

			// Query Databases
			logger.info("Start query database.");
			execDDL(connection, queryDatabaseSql);
			logger.info("Query database successfully.");

			// Use Databases
			logger.info("Start use database.");
			execDDL(connection, useDatabaseSql);
			logger.info("Use database successfully.");

			// show tables
			logger.info("Start query table.");
			execDDL(connection, queryTableSql);
			logger.info("Query table successfully.");

			// Query
			logger.info("Start query.");
			query(connection, querySql);
			logger.info("Query successfully.");

		} catch (Exception e) {
			logger.error("Execute starrocks query failed.", e);
		}
		logger.info("starrocks example execution successfully.");
	}

	private static Connection createConnection() throws Exception {
		Connection connection = null;
		try {
			Class.forName(JDBC_DRIVER);
			String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT);
			connection = DriverManager.getConnection(dbUrl, USER, PASSWD);
		} catch (Exception e) {
			logger.error("Init starrocks connection failed.", e);
			throw new Exception(e);
		}
		return connection;
	}

	public static void execDDL(Connection connection, String sql) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.execute();
		} catch (Exception e) {
			logger.error("Execute sql {} failed.", sql, e);
			throw new Exception(e);
		}
	}

	private static void query(Connection connection, String sql) throws Exception {
		try (Statement stmt = connection.createStatement();
			 ResultSet resultSet = stmt.executeQuery(sql)) {

			ResultSetMetaData md = resultSet.getMetaData();
			int columnCount = md.getColumnCount();
			StringBuffer stringBuffer = new StringBuffer();
			logger.info("Start to print query result.");
			for (int i = 1; i <= columnCount; i++) {
				stringBuffer.append(md.getColumnName(i));
				stringBuffer.append("  ");
			}
			logger.info(stringBuffer.toString());

			while (resultSet.next()) {
				stringBuffer = new StringBuffer();
				for (int i = 1; i <= columnCount; i++) {
					stringBuffer.append(resultSet.getObject(i));
					stringBuffer.append("  ");
				}
				logger.info(stringBuffer.toString());
			}

		} catch (Exception e) {
			logger.error("Execute sql {} failed.", sql, e);
			throw new Exception(e);
		}
	}
}
