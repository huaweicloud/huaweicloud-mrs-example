package com.example.stream.load.dorisstreamloadexample;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huawei.us.common.random.UsSecureRandom;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolException;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * 功能描述
 *
 * @since 2024-04-20
 */
public class DorisSecurityStreamLoader {
    private static final Logger logger = LogManager.getLogger(DorisSecurityStreamLoader.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // FE IP Address
    private static String HOST = "";
    // FE port 安全场景使用https_port
    private static long PORT = 29991;

    private static long QUERY_PORT = 29982;
    // db name
    private final static String DATABASE = "test_2";
    // table name
    private final static String TABLE = "doris_test_sink";

    private static final String DB_URL_PATTERN = "jdbc:mysql://%s:%d?rewriteBatchedStatements=true";

    private static String USER = "";
    private static String PASSWD = "";
    private static String JDBC_DRIVER = "";

    public static void initConf() throws IOException {
        Properties properties = new Properties();
        // 使用ClassLoader加载properties配置文件生成对应的输入流
        InputStream in = DorisSecurityStreamLoader.class.getClassLoader().getResourceAsStream("conf.properties");
        // 使用properties对象加载输入流
        properties.load(in);
        //获取key对应的value值
        USER = properties.getProperty("USER");
        PASSWD = properties.getProperty("PASSWD");
        HOST = properties.getProperty("HOST");
        PORT = Long.parseLong(properties.getProperty("PORT"));
        QUERY_PORT = Long.parseLong(properties.getProperty("QUERY_PORT"));
        JDBC_DRIVER = properties.getProperty("JDBC_DRIVER");
    }

    public static void initTable() {
        String createDatabaseSql = "create database if not exists " + DATABASE;

        String createTableSql = "create table if not exists " + DATABASE + "." + TABLE + " (\n" +
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
            logger.info("Start create database.");
            execDDL(connection, createDatabaseSql);
            logger.info("Database created successfully.");
            // 创建表
            logger.info("Start create table.");
            execDDL(connection, createTableSql);
            logger.info("Table created successfully.");
        } catch (Exception e) {
            logger.info("Execute doris operation failed.");
        }
    }

    private static Connection createConnection() throws Exception {
        Class.forName(JDBC_DRIVER);
        String dbUrl = String.format(DB_URL_PATTERN, HOST, QUERY_PORT);
        return DriverManager.getConnection(dbUrl, USER, PASSWD == null || PASSWD.equals("") ? "" : PASSWD);
    }

    public static void execDDL(Connection connection, String sql) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        } catch (Exception e) {
            logger.info("Execute sql failed.", e);
            throw new Exception(e);
        }
    }

    public static CloseableHttpClient getHttpsClient()
            throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        SSLContext sslContext = new SSLContextBuilder().setProtocol("TLSv1.2")
                .loadTrustMaterial(null, new TrustStrategy() {
                    public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                        return true;
                    }
                }).setSecureRandom(UsSecureRandom.getInstance()).build();
        SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContext,
                NoopHostnameVerifier.INSTANCE);
        return HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();
    }

    public static HttpPut getHttpsPut(String url, String label) {
        HttpPut httpPut = new HttpPut(url);
        String authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", USER, PASSWD)
                .getBytes(StandardCharsets.UTF_8));
        httpPut.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + new String(authEncoding));
        httpPut.setHeader(HttpHeaders.EXPECT, "100-continue");
        httpPut.setHeader("Content-Type", "text/plain; charset=UTF-8");
        httpPut.setHeader("label", label);
        httpPut.setHeader("column_separator", "\t");
        httpPut.setHeader("line_delimiter", "\n");
        httpPut.setHeader("format", "csv");
        httpPut.setHeader("max_filter_ratio", "1.0");

        return httpPut;
    }

    public static void testHttpsClient() throws Exception {
        CloseableHttpClient httpClient = getHttpsClient();

        String url = String.format("https://%s:%s/api/%s/%s/_stream_load",
                HOST, PORT, DATABASE, TABLE);
        String label = "label_stream_load_" + UUID.randomUUID().toString();
        HttpPut feHttpPut = getHttpsPut(url, label);
        logger.info("Start execute doris stream load.url: {}", url);
        CloseableHttpResponse feResponse = httpClient.execute(feHttpPut);
        int statusCode = feResponse.getStatusLine().getStatusCode();
        if (statusCode != 307) {
            logger.error("status is not TEMPORARY_REDIRECT 307, status: ", statusCode);
            return;
        }
        String beLocation = feResponse.getFirstHeader("Location").getValue();
        HttpPut beHttpPut = getHttpsPut(beLocation, label);
        // data
        StringBuilder sb = new StringBuilder();
        // 10001,12,13.3,test1,his is attest
        sb.append(10001).append("\t");
        sb.append(12).append("\t");
        sb.append(13.3).append("\t");
        sb.append("his is attest").append("\t");
        sb.append("test1").append("\n");
        ByteArrayEntity entity = new ByteArrayEntity(sb.toString().getBytes(Charset.forName("UTF-8")),
                ContentType.create("text/plain", "UTF-8"));
        beHttpPut.setEntity(entity);
        CloseableHttpResponse beResponse = httpClient.execute(beHttpPut);
        statusCode = beResponse.getStatusLine().getStatusCode();
        HttpEntity httpEntity = beResponse.getEntity();
        if (statusCode == 200 && httpEntity != null) {
            String loadResult = EntityUtils.toString(httpEntity);
            logger.info("Stream load job result: {}", loadResult);
            StreamLoadRespContent respContent =
                    OBJECT_MAPPER.readValue(loadResult, StreamLoadRespContent.class);
            if (!"Success".contains(respContent.getStatus())) {
                String errMsg =
                        String.format(
                                "Stream load job error: %s, see more in %s",
                                respContent.getMessage(), respContent.getErrorURL());
                logger.warn(errMsg);
            }
        } else {
            String errMsg = httpEntity == null ? "" : EntityUtils.toString(httpEntity);
            logger.warn("Failed to load with label: {}, error code: {}, msg: {}", label, statusCode, errMsg);
        }
    }

    public static void main(String[] args) throws Exception {
        initConf();
        initTable();
        testHttpsClient();
    }
}
