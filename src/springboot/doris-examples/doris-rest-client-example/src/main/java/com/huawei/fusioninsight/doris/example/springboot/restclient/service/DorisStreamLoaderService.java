package com.huawei.fusioninsight.doris.example.springboot.restclient.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import org.springframework.stereotype.Service;


/**
 * 功能描述
 *
 * @since 2024-04-20
 */
@Service
public class DorisStreamLoaderService {
    // db name
    private final static String DATABASE = "test_2";
    // table name
    private final static String TABLE = "doris_test_sink";

    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d?rewriteBatchedStatements=true";

    // FE IP Address
    private static String HOST = "";
    // FE port 安全场景使用https_port,普通模式使用 http_port
    private static long PORT = 29991;

    private static long QUERY_PORT = 29982;
    // db name

    private static String USER = "";
    private static String PASSWD = "";

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
            Properties properties = new Properties();
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            InputStream in = DorisStreamLoaderService.class.getClassLoader().getResourceAsStream("conf.properties");
            // 使用properties对象加载输入流
            properties.load(in);
            //获取key对应的value值
            USER = properties.getProperty("USER");
            PASSWD = properties.getProperty("PASSWD");
            HOST = properties.getProperty("HOST");
            QUERY_PORT = Long.parseLong(properties.getProperty("QUERY_PORT"));
            Class.forName(properties.getProperty("JDBC_DRIVER"));
            String dbUrl = String.format(DB_URL_PATTERN, HOST, QUERY_PORT);
            connection = DriverManager.getConnection(dbUrl, USER, PASSWD == null || PASSWD.equals("") ? "" : PASSWD);
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
    public  String getHttpPost(String csvPath) throws IOException {
        // 安全场景使用https开头， 普通模式使用http开头
        Properties properties = new Properties();
        // 使用ClassLoader加载properties配置文件生成对应的输入流
        InputStream in = DorisStreamLoaderService.class.getClassLoader().getResourceAsStream("conf.properties");
        // 使用properties对象加载输入流
        properties.load(in);
        HOST = properties.getProperty("HOST");
        PORT = Long.parseLong(properties.getProperty("PORT"));
        USER = properties.getProperty("USER");
        PASSWD = properties.getProperty("PASSWD");
        String loadUrl = String.format("https://%s:%s/api/%s/%s/_stream_load",
                HOST, PORT, DATABASE, TABLE);
        String[] cmdList = {"curl", "-k", "--location-trusted", "-u" + USER + ":" + PASSWD, "-H", "expect:100-continue", "-H", "column_separator:,", "-T",
                csvPath,
                loadUrl};

        //命令的空格在jva数组里单个的,必须分开写，不能有空格,
        String responseMsg = execCurl(cmdList);
        System.out.println("curl" + responseMsg);

        return responseMsg;
    }
}
