package com.huawei.fusioninsight.doris.example.springboot.restclient.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.springframework.stereotype.Service;


/**
 * 功能描述
 *
 * @since 2024-04-20
 */
@Service
public class DorisStreamLoaderService {

    // FE IP Address
    private final static String HOST = "127.0.0.1";
    // FE port 安全场景使用https_port,普通模式使用 http_port
    private final static int PORT = 29991;

    private final static int JDBC_PORT = 29982;
    // db name
    private final static String DATABASE = "test_2";
    // table name
    private final static String TABLE = "doris_test_sink";

    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d?rewriteBatchedStatements=true";

    private static final String USER = System.getenv("DORIS_MY_USER");
    private static final String PASSWD = System.getenv("DORIS_MY_PASSWORD");

    // 安全场景使用https开头， 普通模式使用http开头
    private final static String loadUrl = String.format("https://%s:%s/api/%s/%s/_stream_load",
            HOST, PORT, DATABASE, TABLE);

    //java 调用 Curl的方法
    public static String execCurl(String[] cmds) {
        ProcessBuilder process = new ProcessBuilder(cmds);
        Process p;
        try {
            p = process.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            return builder.toString();

        } catch (Exception e) {
            System.out.print("error");
        }
        return null;
    }

    public  void initTable(){
        String createDatabaseSql = "create database if not exists "+DATABASE;

        String createTableSql = "create table if not exists " + DATABASE + "." + TABLE +  " (\n" +
                "   `id` int NULL COMMENT \"\",\n" +
                "   `number` int NULL COMMENT \"\",\n" +
                "   `price` DECIMAL(12,2) NULL COMMENT \"\",\n" +
                "   `skuname` varchar(40) NULL COMMENT \"\",\n" +
                "   `skudesc` varchar(200) NULL COMMENT \"\"\n" +
                " ) ENGINE=OLAP\n" +
                " DUPLICATE KEY(`id`)\n" +
                " COMMENT \"商品信息表\"\n" +
                " DISTRIBUTED BY HASH(`id`) BUCKETS 1\n" +
                " PROPERTIES (\n" +
                " \"replication_num\" = \"3\",\n" +
                " \"in_memory\" = \"false\",\n" +
                " \"storage_format\" = \"V2\"\n" +
                " );";
        try (Connection connection = createConnection()) {
            // 创建数据库
            System.out.println("Start create database.");
            execDDL(connection, createDatabaseSql);
            System.out.println("Database created successfully.");
            // 创建表
            System.out.println("Start create table.");
            execDDL(connection, createTableSql);
            System.out.println("Table created successfully.");
        } catch (Exception e) {
            System.out.println("Execute doris operation failed.");
        }
    }

    private static Connection createConnection() throws Exception {
        Connection connection = null;
        try {
            Class.forName(JDBC_DRIVER);
            String dbUrl = String.format(DB_URL_PATTERN, HOST, JDBC_PORT);
            connection = DriverManager.getConnection(dbUrl, USER, PASSWD);
        } catch (Exception e) {
            System.out.println("Init doris connection failed.");
            throw new Exception(e);
        }
        return connection;
    }

    public static void execDDL(Connection connection, String sql) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        } catch (Exception e) {
            System.out.println("Execute sql {} failed.");
            throw new Exception(e);
        }
    }

    //接口调用
    public  String getHttpPost(String csvPath) {

        String[] cmdList = {"curl", "-k", "--location-trusted", "-u" + USER + ":" + PASSWD, "-H", "expect:100-continue", "-H", "column_separator:,", "-T",
                csvPath,
                loadUrl};

        //命令的空格在jva数组里单个的,必须分开写，不能有空格,
        String responseMsg = execCurl(cmdList);
        System.out.println("curl" + responseMsg);

        return responseMsg;
    }
}
