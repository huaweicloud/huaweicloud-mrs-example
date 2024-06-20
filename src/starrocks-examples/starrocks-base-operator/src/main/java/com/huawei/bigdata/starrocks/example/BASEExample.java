package com.huawei.bigdata.starrocks.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class BASEExample {
	private static final Logger logger = LogManager.getLogger(BASEExample.class);
	static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d?characterEncoding=utf-8&serverTimezone=Asia/Shanghai";
	private static final String HOST = "xxxx"; // Leader Node host
	private static final int PORT = 0;   // query_port of Leader Node
	// Before running this example, set the environment variables STARROCKS_MY_USER and STARROCKS_MY_PASSWORD in the local environment variables.
	// It is recommended that ciphertext be stored and decrypted during use to ensure security.
	private static final String USER = System.getenv("StarRocks_MY_USER");
	private static final String PASSWD = System.getenv("StarRocks_MY_PASSWORD");


	public static void main(String[] args) {
		// Be careful not to add a semicolon at the end.
		String dbName = "demo_db";
		String tableName = "test_tbl";
		String createDatabaseSql = "create database if not exists demo_db";
		String createTableSql = "create table if not exists " + dbName + "." + tableName +  " (\n" +
				"c1 int not null,\n" +
				"c2 int not null,\n" +
				"c3 string not null\n" +
				") engine=olap\n" +
				"unique key(c1, c2)\n" +
				"distributed by hash(c1) buckets 1";
		String insertTableSql = "insert into " + dbName + "." + tableName + " values(?, ?, ?)";
		String querySql = "select * from " + dbName + "." + tableName + " limit 10";
		String dropSql = "drop table " + dbName + "." + tableName;
		String dropDb = "drop database " + dbName;
		logger.info("Start execute starrocks example.");
		try (Connection connection = createConnection()) {
			// Create database
			logger.info("Start create database.");
			execDDL(connection, createDatabaseSql);
			logger.info("Database created successfully.");

			// Create table
			logger.info("Start create table.");
			execDDL(connection, createTableSql);
			logger.info("Table created successfully.");

			// Insert into data to table
			logger.info("Start to insert data into the table.");
			insert(connection, insertTableSql);
			logger.info("Inserting data to the table succeeded.");

			// Query table data
			logger.info("Start to query table data.");
			query(connection, querySql);
			logger.info("Querying table data succeeded.");

			// Delete table
			logger.info("Start to delete the table.");
			execDDL(connection, dropSql);
			logger.info("Table deleted successfully.");

			// delete database
			logger.info("Start to delete the database.");
			execDDL(connection, dropDb);
			logger.info("Database deleted successfully.");

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

	private static void insert(Connection connection, String sql) throws Exception {
		int INSERT_BATCH_SIZE = 10;
		try(PreparedStatement stmt = connection.prepareStatement(sql)) {

			for (int i =0; i < INSERT_BATCH_SIZE; i++) {
				stmt.setInt(1, i);
				stmt.setInt(2, i * 10);
				stmt.setString(3, String.valueOf(i * 100));
				stmt.addBatch();
			}

			stmt.executeBatch();
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
