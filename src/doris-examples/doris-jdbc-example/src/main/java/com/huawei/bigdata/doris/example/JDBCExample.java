package com.huawei.bigdata.doris.example;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCExample {
    private static final Logger logger = LogManager.getLogger(JDBCExample.class);
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d?rewriteBatchedStatements=true";
    private static String HOST = ""; // Leader Node host
    private static long PORT;   // query_port of Leader Node
    // 运行本示例前请先在本地环境变量中设置环境变量DORIS_MY_USER和DORIS_MY_PASSWORD。建议密文存放，使用时解密，确保安全。
    private static String USER = "";
    private static String PASSWD = "";


    public static void main(String[] args) {
        // 注意末尾不要加 分号 ";"
        String dbName = "demo_db";
        String tableName = "test_tbl";
        String createDatabaseSql = "create database if not exists demo_db";
        String createTableSql = "create table if not exists " + dbName + "." + tableName + " (\n" +
                "c1 int not null,\n" +
                "c2 int not null,\n" +
                "c3 string not null\n" +
                ") engine=olap\n" +
                "unique key(c1, c2)\n" +
                "distributed by hash(c1) buckets 1";
        String insertTableSql = "insert into " + dbName + "." + tableName + " values(?, ?, ?);";
        String querySql = "select * from " + dbName + "." + tableName + " limit 10";
        String dropSql = "drop table " + dbName + "." + tableName;

        logger.info("Start execute doris example.");
        try (Connection connection = createConnection()) {
            // 创建数据库
            logger.info("Start create database.");
            execDDL(connection, createDatabaseSql);
            logger.info("Database created successfully.");

            // 创建表
            logger.info("Start create table.");
            execDDL(connection, createTableSql);
            logger.info("Table created successfully.");

            // 插入表数据
            logger.info("Start to insert data into the table.");
            insert(connection, insertTableSql);
            logger.info("Inserting data to the table succeeded.");

            // 查询表数据
            logger.info("Start to query table data.");
            query(connection, querySql);
            logger.info("Querying table data succeeded.");

            // 删除表
            logger.info("Start to delete the table.");
            execDDL(connection, dropSql);
            logger.info("Table deleted successfully.");

        } catch (Exception e) {
            logger.error("Execute doris query failed.", e);
        }
        logger.info("Doris example execution successfully.");
    }

    private static Connection createConnection() throws Exception {
        Connection connection = null;

        try {
            Properties properties = new Properties();
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            InputStream in = JDBCExample.class.getClassLoader().getResourceAsStream("conf.properties");
            // 使用properties对象加载输入流
            properties.load(in);
            //获取key对应的value值
            USER = properties.getProperty("USER");
            PASSWD = properties.getProperty("PASSWD");
            HOST = properties.getProperty("HOST");
            PORT = Long.parseLong(properties.getProperty("QUERY_PORT"));
            Class.forName(properties.getProperty("JDBC_DRIVER"));
            String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT);
            connection = DriverManager.getConnection(dbUrl, USER, PASSWD == null || PASSWD.equals("") ? "" : PASSWD);
        } catch (Exception e) {
            logger.error("Init doris connection failed.", e);
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
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {

            for (int i = 0; i < INSERT_BATCH_SIZE; i++) {
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
