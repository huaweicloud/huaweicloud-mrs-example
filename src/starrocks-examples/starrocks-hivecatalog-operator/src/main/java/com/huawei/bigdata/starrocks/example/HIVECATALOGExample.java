package com.huawei.bigdata.starrocks.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class HIVECATALOGExample {
	private static final Logger logger = LogManager.getLogger(HIVECATALOGExample.class);
	static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d?characterEncoding=utf-8&serverTimezone=Asia/Shanghai";
	private static final String HOST = "xxx"; // Leader Node host
	private static final int PORT = 0;   // query_port of Leader Node
	// Before running this example, set the environment variables STARROCKS_MY_USER and STARROCKS_MY_PASSWORD in the local environment variables.
	// It is recommended that ciphertext be stored and decrypted during use to ensure security.
	private static final String USER = System.getenv("StarRocks_MY_USER");
	private static final String PASSWD = System.getenv("StarRocks_MY_PASSWORD");


	public static void main(String[] args) {
		// Don't add a semicolon at the end.
		String showCatalogSql = "show catalogs";
		String switchCatalogSql = "set catalog hive_catalog";
		String queryDatabaseSql = "show databases";
		String useDatabaseSql = "use test_kerbers";
		String queryTableSql = "show tables";
		String querySql = "select * from sr_hdfs_txt";
		String createHiveCatalogSql = "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES"  +  " (\n" +
				"type = hive,\n" +
				"hive.metastore.type = hive,\n" +
				"hive.metastore.uris = thrift://Hive MetaStore实例IP地址:MetaStore Thrift端口号,thrift://Hive MetaStore实例IP地址:MetaStore Thrift端口号\n"+
		")";
		logger.info("Start execute starrocks example.");
		try (Connection connection = createConnection()) {
			// create hivecatalog
			logger.info("Start create hiveCatalog.");
			execDDL(connection, createHiveCatalogSql);
			logger.info("Create hiveCatalog successfully.");
			// query catalog
			logger.info("Start show hiveCatalog.");
			execDDL(connection, showCatalogSql);
			logger.info("Show hiveCatalog successfully.");

			// switch catalog
			logger.info("Start switch HiveCatalog.");
			execDDL(connection, switchCatalogSql);
			logger.info("Switch HiveCatalog successfully.");
			//The following operations on databases and tables are hive databases and tables
			// Query HiveDatabases
			logger.info("Start query database.");
			execDDL(connection, queryDatabaseSql);
			logger.info("Query database successfully.");

			// Use HiveDatabases
			logger.info("Start use database.");
			execDDL(connection, useDatabaseSql);
			logger.info("Use database successfully.");

			// show Hivetables
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
		logger.info("StarRocks example execution successfully.");
	}

	private static Connection createConnection() throws Exception {
		Connection connection = null;
		try {
			Class.forName(JDBC_DRIVER);
			String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT);
			connection = DriverManager.getConnection(dbUrl, "root", "");
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
