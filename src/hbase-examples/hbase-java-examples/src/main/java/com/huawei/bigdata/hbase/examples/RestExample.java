package com.huawei.bigdata.hbase.examples;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteAdmin;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.rest.client.Response;
import org.apache.hadoop.hbase.rest.model.NamespacesInstanceModel;
import org.apache.hadoop.hbase.rest.provider.JacksonProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.map.ObjectMapper;

// RestExample Example of using the REST API
public class RestExample {

    private final static Log LOG = LogFactory.getLog(RestExample.class.getName());

    private static Configuration conf = null;
    private TableName tableName = null;
    private String tName = null;
    private String node = null;
    private boolean isEnableSSL = false;
    private Cluster cluster = null;
    private Client client = null;
    private static JAXBContext context = null;
    private static String NAMESPACES = "/namespaces/";

    public RestExample(Configuration conf, String path, boolean ssl) throws IOException {
        this.conf = conf;
        this.node = path;
        this.isEnableSSL = ssl;
        tName = "hbase_rest_table";
        this.tableName = TableName.valueOf(tName);
    }

    public static void main(String[] args) {


    }

    /**
     * need to configure parameters: HBase --> Service Configuration -->
     * Type(All) --> Role(RESTServer) --> Security
     * <p>
     * hbase.rest.authentication.type kerberos hbase.rest.ssl.enabled true
     *
     * @throws MalformedURLException
     * @throws IOException
     */
    public void isUseSSL() throws Exception {

        try {
            LOG.info("please make sure that you are using "
              + (isEnableSSL ? "security" : "NoSecurity") + " cluster.");
            UserGroupInformation ugi = UserGroupInformation.getLoginUser();
            ugi.doAs(new PrivilegedAction<Boolean>() {
                @Override
                public Boolean run() {
                    testRestCreateTable();
                    testRestPut();
                    testRestGet();
                    testRestScanData();
                    testSingleColumnValueFilter();
                    testFilterList();
                    testRestDelete();
                    dropRestTable();
                    getRequestByText();
                    createdAndAlterNamespacesByXML();
                    createdAndAlterNamespacesByJSON();
                    createdAndAlterNamespacesByProtobuf();
                    return null;
                }
            });
        } catch (Exception e) {
            LOG.error("failed to execute the examples because ", e);
        }
    }

    private void initClient() {
        cluster = new Cluster();
        cluster.add(node);
        client = new Client(cluster, isEnableSSL);
    }

    private String listNamespaces() {
        String namespacePath = "/namespaces/";
        Response response;
        try {
            initClient();
            response = client.get(namespacePath);
            String nss = Bytes.toString(response.getBody());
            LOG.info("list namespace: " + nss);
        } catch (IOException e) {
            LOG.error("failed to list namespaces ", e);
        }

        return null;
    }

    private static NamespacesInstanceModel buildTestModel() throws IOException {
        NamespacesInstanceModel model = new NamespacesInstanceModel();
        model.addProperty("key1", "value1");
        model.addProperty("key2", "value2");
        model.addProperty("key3", "value3");
        return model;
    }

    private void getRequestByText() {
        Response response;
        try {
            initClient();
            response = client.get(NAMESPACES);
            LOG.info(Bytes.toString(response.getBody()));
        } catch (IOException e) {
            LOG.error("failed to get info because ", e);
        }
    }

    private static byte[] toXML(NamespacesInstanceModel model) throws JAXBException {
        StringWriter writer = new StringWriter();
        context = JAXBContext.newInstance(NamespacesInstanceModel.class);
        context.createMarshaller().marshal(model, writer);
        return Bytes.toBytes(writer.toString());
    }

    @SuppressWarnings("unchecked")
    private static <T> T fromXML(byte[] content) throws JAXBException {
        return (T) context.createUnmarshaller().unmarshal(new ByteArrayInputStream(content));
    }

    private void getNamespaceProperties(Map<String, String> namespaceProps) {
        for (String key : namespaceProps.keySet()) {
            LOG.info(namespaceProps.get(key));
        }
    }

    private void createdAndAlterNamespacesByXML() {
        Response response;
        String namespacePath = "test4XML";
        try {
            initClient();
            //create namespace
            NamespacesInstanceModel model = buildTestModel();

            //create namespace
            client.post(NAMESPACES + namespacePath, Constants.MIMETYPE_XML, toXML(model));

            //alter namespace
            client.put(NAMESPACES + namespacePath, Constants.MIMETYPE_XML, toXML(model));

            // get namespace properties
            response = client.get(NAMESPACES + namespacePath, Constants.MIMETYPE_XML);
            NamespacesInstanceModel model1 = fromXML(response.getBody());
            getNamespaceProperties(model1.getProperties());

            //list namespaces
            listNamespaces();

            //delete namespace
            response = client.delete(NAMESPACES + namespacePath);
        } catch (IOException | JAXBException e) {
            LOG.error("failed to use POST request to create namespaces by XML", e);
        }
    }


    private void createdAndAlterNamespacesByJSON() {

        ObjectMapper jsonMapper = new JacksonProvider().locateMapper(NamespacesInstanceModel.class, MediaType.APPLICATION_JSON_TYPE);

        Response response;
        String namespacePath = "test4JSON";
        String jsonString;
        try {
            initClient();
            //create model
            NamespacesInstanceModel model = buildTestModel();

            //create namespace
            jsonString = jsonMapper.writeValueAsString(model);
            client.post(NAMESPACES + namespacePath, Constants.MIMETYPE_JSON, Bytes.toBytes(jsonString));

            //alter namespace
            jsonString = jsonMapper.writeValueAsString(model);
            client.put(NAMESPACES + namespacePath, Constants.MIMETYPE_JSON, Bytes.toBytes(jsonString));

            //get namespace properties
            response = client.get(NAMESPACES + namespacePath, Constants.MIMETYPE_JSON);
            model = jsonMapper.readValue(response.getBody(), NamespacesInstanceModel.class);
            getNamespaceProperties(model.getProperties());

            //list namespaces
            listNamespaces();

            //delete namespace
            client.delete(NAMESPACES + namespacePath);
        } catch (IOException e) {
            LOG.error("failed to use PUT request to create namespaces by JSON", e);
        }
    }

    private void createdAndAlterNamespacesByProtobuf() {
        Response response;
        String namespacePath = "test4Protobuf";
        try {
            initClient();
            //create model
            NamespacesInstanceModel model = buildTestModel();

            //create namespace
            client.post(NAMESPACES + namespacePath, Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());

            //alter namespace
            response = client.put(NAMESPACES + namespacePath, Constants.MIMETYPE_PROTOBUF, model.createProtobufOutput());
            NamespacesInstanceModel nsModel = (NamespacesInstanceModel) model.getObjectFromMessage(response.getBody());
            for (Entry<String, String> nsProperty : nsModel.getProperties().entrySet()) {
                LOG.info(nsProperty);
            }

            //list namespaces
            listNamespaces();

            //delete namespace
            client.delete(NAMESPACES + namespacePath);
        } catch (IOException e) {
            LOG.error("failed to use PUT request to create namespaces by Protobuf", e);
        }
    }


    /**
     * Create user info table
     */
    private void testRestCreateTable() {

        // co RestExample-3-Table Create a remote table instance, wrapping the
        // REST access into a familiar interface.
        LOG.info("Entering testCreateRestTable.");

        // Specify the table descriptor.
        HTableDescriptor htd = new HTableDescriptor(tableName);

        // Set the column family name to info.
        HColumnDescriptor hcd = new HColumnDescriptor("info");

        // Set data encoding methods. HBase provides DIFF,FAST_DIFF,PREFIX
        // and PREFIX_TREE
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

        // Set compression methods, HBase provides two default compression
        // methods:GZ and SNAPPY
        // GZ has the highest compression rate,but low compression and
        // decompression effeciency,fit for cold data
        // SNAPPY has low compression rate, but high compression and
        // decompression effeciency,fit for hot data.
        // it is advised to use SANPPY
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);

        htd.addFamily(hcd);

        RemoteAdmin restAdmin = null;
        initClient();
        // Instantiate an RemoterestAdmin object.
        restAdmin = new RemoteAdmin(client, conf);
        try {
            if (!restAdmin.isTableAvailable(tableName.toString())) {
                LOG.info("Creating RestTable...");
                restAdmin.createTable(htd);
                LOG.info("restTable created successfully.");
            } else {
                LOG.warn("table already exists");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        LOG.info("Exiting testCreateRestTable.");
    }

    /**
     * Insert data
     */
    public void testRestPut() {
        LOG.info("Entering testRestPut.");

        // Specify the column family name.
        byte[] familyName = Bytes.toBytes("info");
        // Specify the column name.
        byte[][] qualifiers = {Bytes.toBytes("name"), Bytes.toBytes("gender"), Bytes.toBytes("age"),
                Bytes.toBytes("address")};

        RemoteHTable rTable = null;
        try {
            initClient();
            // Instantiate an HTable object.
            rTable = new RemoteHTable(client, tName);
            List<Put> puts = new ArrayList<Put>();
            // Instantiate a Put object.
            Put put = new Put(Bytes.toBytes("012005000201"));
            put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Yang Yiwen"));
            put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
            put.addColumn(familyName, qualifiers[2], Bytes.toBytes("30"));
            put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Kaixian, Chongqing"));
            puts.add(put);

            put = new Put(Bytes.toBytes("012005000202"));
            put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Xu Bing"));
            put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
            put.addColumn(familyName, qualifiers[2], Bytes.toBytes("26"));
            put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Weinan, Shaanxi"));
            puts.add(put);

            // Submit a put request.
            rTable.put(puts);

            LOG.info("Put successfully.");
        } catch (IOException e) {
            LOG.error("Put failed ", e);
        }
        LOG.info("Exiting testRestPut.");
    }

    /**
     * Get Data
     */
    public void testRestGet() {
        LOG.info("Entering testRestGet.");

        // Specify the column family name.
        byte[] familyName = Bytes.toBytes("info");
        // Specify the column name.
        byte[][] qualifier = {Bytes.toBytes("name"), Bytes.toBytes("address")};
        // Specify RowKey.
        byte[] rowKey = Bytes.toBytes("012005000201");

        RemoteHTable rTable = null;
        try {
            initClient();
            // Create the Configuration instance.
            rTable = new RemoteHTable(client, tName);

            // Instantiate a Get object.
            Get get = new Get(rowKey);

            // Set the column family name and column name.
            get.addColumn(familyName, qualifier[0]);
            get.addColumn(familyName, qualifier[1]);

            // Submit a get request.
            Result result = rTable.get(get);

            // Print query results.
            for (Cell cell : result.rawCells()) {
                LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
                        + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                        + Bytes.toString(CellUtil.cloneValue(cell)));
            }
            LOG.info("Get data successfully.");
        } catch (IOException e) {
            LOG.error("Get data failed ", e);
        }
        LOG.info("Exiting testRestGet.");
    }

    public void testRestScanData() {
        LOG.info("Entering testRestScanData.");

        RemoteHTable rTable = null;
        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;
        try {
            initClient();
            // Create the Configuration instance.
            rTable = new RemoteHTable(client, tName);

            // Instantiate a Get object.
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // Set the cache size.
            scan.setCaching(1000);

            // Submit a scan request.
            rScanner = rTable.getScanner(scan);

            // Print query results.
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Scan data successfully.");
        } catch (IOException e) {
            LOG.error("Scan data failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
        }
        LOG.info("Exiting testRestScanData.");
    }

    public void testSingleColumnValueFilter() {
        LOG.info("Entering testSingleColumnValueFilter.");

        RemoteHTable rTable = null;

        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;

        try {
            initClient();
            // Create the Configuration instance.
            rTable = new RemoteHTable(client, tName);

            // Instantiate a Get object.
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // Set the filter criteria.
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
                    CompareOp.EQUAL, Bytes.toBytes("Xu Bing"));

            scan.setFilter(filter);

            // Submit a scan request.
            rScanner = rTable.getScanner(scan);

            // Print query results.
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Single column value filter successfully.");
        } catch (IOException e) {
            LOG.error("Single column value filter failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
        }
        LOG.info("Exiting testSingleColumnValueFilter.");
    }

    public void testFilterList() {
        LOG.info("Entering testFilterList.");

        RemoteHTable rTable = null;

        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;

        try {
            initClient();
            // Create the Configuration instance.
            rTable = new RemoteHTable(client, tName);

            // Instantiate a Get object.
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // Instantiate a FilterList object in which filters have "and"
            // relationship with each other.
            FilterList list = new FilterList(Operator.MUST_PASS_ALL);
            // Obtain data with age of greater than or equal to 20.
            list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
                    CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(new Long(20))));
            // Obtain data with age of less than or equal to 29.
            list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
                    CompareOp.LESS_OR_EQUAL, Bytes.toBytes(new Long(29))));

            scan.setFilter(list);

            // Submit a scan request.
            rScanner = rTable.getScanner(scan);
            // Print query results.
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + ","
                            + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Filter list successfully.");
        } catch (IOException e) {
            LOG.error("Filter list failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
        }
        LOG.info("Exiting testFilterList.");
    }

    /**
     * deleting data
     */
    public void testRestDelete() {
        LOG.info("Entering testRestDelete.");

        byte[] rowKey = Bytes.toBytes("012005000201");

        RemoteHTable rTable = null;
        try {
            initClient();
            // Instantiate an HTable object.
            rTable = new RemoteHTable(client, tName);

            // Instantiate an Delete object.
            Delete delete = new Delete(rowKey);

            // Submit a delete request.
            rTable.delete(delete);

            LOG.info("Delete table successfully.");
        } catch (IOException e) {
            LOG.error("Delete table failed ", e);
        }
        LOG.info("Exiting testRestDelete.");
    }

    /**
     * Delete user table
     */
    public void dropRestTable() {
        LOG.info("Entering dropRestTable.");

        RemoteAdmin restAdmin = null;
        try {
            initClient();
            restAdmin = new RemoteAdmin(client, conf);
            if (restAdmin.isTableAvailable(tName)) {
                // Delete table.
                restAdmin.deleteTable(tName);
            }
            LOG.info("Drop table successfully.");
        } catch (IOException e) {
            LOG.error("Drop table failed ", e);
        }
        LOG.info("Exiting dropRestTable.");
    }
}
