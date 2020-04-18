package com.huawei.bigdata.opentsdb.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OpentsdbExample {

  private final static Log LOG = LogFactory.getLog(OpentsdbExample.class.getName());
  private static String OPENTSDB_PROTOCOL = null;
  private static String OPENTSDB_HOSTNAME = null;
  private static String OPENTSDB_PORT = null;
  private static String BASE_URL = null;
  private static String PUT_URL = null;
  private static String QUERY_URL = null;
  

  public static void main(String[] args) {

    init();

    OpentsdbExample opentsdbExample = new OpentsdbExample();
    // import data.
    opentsdbExample.putData("/api/put/?sync&sync_timeout=60000");
    // query data.
    opentsdbExample.queryData("/api/query");
    // delete data.
    opentsdbExample.deleteData("/api/query");

  }

  private static void init() {
    ClassLoader classLoader = OpentsdbExample.class.getClassLoader();
    String file = classLoader.getResource("opentsdb.properties").getPath();
    File proFile = new File(file);
    if (proFile.exists()) {
      Properties props = new Properties();
      try {
        props.load(new FileInputStream(proFile));
      } catch (IOException e) {
        LOG.error("Failed to read property file.", e);
      }
      //get properties.
      OPENTSDB_HOSTNAME = props.getProperty("tsd_hostname");
      OPENTSDB_PORT = props.getProperty("tsd_port");
      OPENTSDB_PROTOCOL = props.getProperty("tsd_protocol");
      if ("".equals(OPENTSDB_PROTOCOL) || OPENTSDB_PROTOCOL == null) {
        OPENTSDB_PROTOCOL = "https";
      }
      if ("".equals(OPENTSDB_PORT) || OPENTSDB_PORT == null) {
      	OPENTSDB_PORT = "4242";
      }
      // append base url. http://tsd_hostname:tsd_prot/
      StringBuilder sbuilder = new StringBuilder();
      sbuilder.append(OPENTSDB_PROTOCOL)
        .append("://")
        .append(OPENTSDB_HOSTNAME)
        .append(":")
        .append(OPENTSDB_PORT);

      BASE_URL = sbuilder.toString();
    } else {
      LOG.error("property file not exists, the property file is: " + file);
    }
  }

  public static void addTimeout(HttpRequestBase req) {
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(5000)
      .setConnectionRequestTimeout(10000).setSocketTimeout(60000).build();
    req.setConfig(requestConfig);
  }

  //put data.
  private void putData(String dataPoint) {
    PUT_URL = BASE_URL + dataPoint;
    LOG.info("start to put data in opentsdb, the url is " + PUT_URL);
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpPost httpPost = new HttpPost(PUT_URL);
      // 请求需要设置超时时间
      addTimeout(httpPost);
      String weatherData = genWeatherData();
      StringEntity entity = new StringEntity(weatherData, "ISO-8859-1");
      entity.setContentType("application/json");
      httpPost.setEntity(entity);
      HttpResponse response = httpClient.execute(httpPost);

      int statusCode = response.getStatusLine().getStatusCode();
      LOG.info("Status Code : " + statusCode);
      if (statusCode != HttpStatus.SC_NO_CONTENT) {
        LOG.info("Request failed! " + response.getStatusLine());
      }
      LOG.info("put data to opentsdb successfully.");
    } catch (IOException e) {
      LOG.error("Failed to put data.", e);
    }
  }
  
  //query data
  private void queryData(String dataPoint) {
    QUERY_URL = BASE_URL + dataPoint;
    LOG.info("start to query data in opentsdb, the url is " + QUERY_URL);
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpPost httpPost = new HttpPost(QUERY_URL);
      // 请求需要设置超时时间
      addTimeout(httpPost);
      String queryRequest = genQueryReq();
      StringEntity entity = new StringEntity(queryRequest, "utf-8");
      entity.setContentType("application/json");
      httpPost.setEntity(entity);
      HttpResponse response = httpClient.execute(httpPost);

      int statusCode = response.getStatusLine().getStatusCode();
      LOG.info("Status Code : " + statusCode);
      if (statusCode != HttpStatus.SC_OK) {
        LOG.info("Request failed! " + response.getStatusLine());
      }

      String body = EntityUtils.toString(response.getEntity(), "utf-8");
      LOG.info("Response content : " + body);
      LOG.info("query data to opentsdb successfully.");
    } catch (IOException e) {
      LOG.error("Failed to query data.", e);
    }
  }
  
  //delete data.
  public void deleteData(String dataPoint) {
    QUERY_URL = BASE_URL + dataPoint;
    LOG.info("start to delete data in opentsdb, the url is " + QUERY_URL);
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpPost httpPost = new HttpPost(QUERY_URL);
      addTimeout(httpPost);
      String deleteRequest = genDeleteReq();
      StringEntity entity = new StringEntity(deleteRequest, "utf-8");
      entity.setContentType("application/json");
      httpPost.setEntity(entity);
      HttpResponse response = httpClient.execute(httpPost);

      int statusCode = response.getStatusLine().getStatusCode();
      LOG.info("Status Code : " + statusCode);
      if (statusCode != HttpStatus.SC_OK) {
        LOG.info("Request failed! " + response.getStatusLine());
      }
      LOG.info("delete data to opentsdb successfully.");
    } catch (IOException e) {
      LOG.error("Failed to delete data.", e);
    }
  }

  String genDeleteReq() {
    Query query = new Query();
    query.start = 1498838400L;
    query.end = 1498921200L;
    query.queries = ImmutableList.of(new SubQuery("city.temp", "sum"),
      new SubQuery("city.hum", "sum"));
    query.delete = true;

    Gson gson = new Gson();
    return gson.toJson(query);
  }
  
  String genQueryReq() {
    Query query = new Query();
    query.start = 1498838400L;
    query.end = 1498921200L;
    query.queries = ImmutableList.of(new SubQuery("city.temp", "sum"),
      new SubQuery("city.hum", "sum"));

    Gson gson = new Gson();
    return gson.toJson(query);
  }

  static class DataPoint {
    public String metric;
    public Long timestamp;
    public Double value;
    public Map<String, String> tags;
    public DataPoint(String metric, Long timestamp, Double value, Map<String, String> tags) {
      this.metric = metric;
      this.timestamp = timestamp;
      this.value = value;
      this.tags = tags;
    }
  }

  static class Query {
    public Long start;
    public Long end;
    public boolean delete = false;
    public List<SubQuery> queries;
  }

  static class SubQuery {
    public String metric;
    public String aggregator;

    public SubQuery(String metric, String aggregator) {
      this.metric = metric;
      this.aggregator = aggregator;
    }
  }

  private String genWeatherData() {
    List<DataPoint> dataPoints = new ArrayList<DataPoint>();
    Map<String, String> tags = ImmutableMap.of("city", "Shenzhen", "region", "Longgang");

    // Data of air temperature
    dataPoints.add(new DataPoint("city.temp", 1498838400L, 28.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498842000L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498845600L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498849200L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498852800L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498856400L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498860000L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498863600L, 27.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498867200L, 29.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498870800L, 30.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498874400L, 32.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498878000L, 32.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498881600L, 33.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498885200L, 33.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498888800L, 32.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498892400L, 32.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498896000L, 31.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498899600L, 30.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498903200L, 30.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498906800L, 29.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498910400L, 29.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498914000L, 29.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498917600L, 28.0, tags));
    dataPoints.add(new DataPoint("city.temp", 1498921200L, 28.0, tags));

    // Data of humidity
    dataPoints.add(new DataPoint("city.hum", 1498838400L, 54.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498842000L, 53.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498845600L, 52.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498849200L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498852800L, 50.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498856400L, 49.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498860000L, 48.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498863600L, 46.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498867200L, 46.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498870800L, 48.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498874400L, 48.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498878000L, 49.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498881600L, 49.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498885200L, 50.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498888800L, 50.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498892400L, 50.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498896000L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498899600L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498903200L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498906800L, 51.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498910400L, 52.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498914000L, 53.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498917600L, 54.0, tags));
    dataPoints.add(new DataPoint("city.hum", 1498921200L, 54.0, tags));

    Gson gson = new Gson();
    return gson.toJson(dataPoints);
  }
  
  
}
