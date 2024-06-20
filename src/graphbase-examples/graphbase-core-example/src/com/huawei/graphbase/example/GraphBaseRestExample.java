package com.huawei.graphbase.example;

import com.huawei.graphbase.rest.RestApi;
import com.huawei.graphbase.rest.entity.EdgeLabel;
import com.huawei.graphbase.rest.entity.PropertyKey;
import com.huawei.graphbase.rest.request.AddEdgeReqObj;
import com.huawei.graphbase.rest.request.AddVertexReqObj;
import com.huawei.graphbase.rest.request.EdgeSearchReqObj;
import com.huawei.graphbase.rest.request.EdgeSearchRspObj;
import com.huawei.graphbase.rest.request.GraphIndexReqObj;
import com.huawei.graphbase.rest.request.KeyTextType;
import com.huawei.graphbase.rest.request.LineSearchReqObj;
import com.huawei.graphbase.rest.request.PathSearchReqObj;
import com.huawei.graphbase.rest.request.PropertyFilter;
import com.huawei.graphbase.rest.request.PropertyKeySort;
import com.huawei.graphbase.rest.request.PropertyReqObj;
import com.huawei.graphbase.rest.request.Task;
import com.huawei.graphbase.rest.request.VertexFilter;
import com.huawei.graphbase.rest.request.VertexSearchReqObj;
import com.huawei.graphbase.rest.request.VertexSearchRspObj;
import com.huawei.graphbase.rest.security.GraphHttpClient;
import com.huawei.graphbase.rest.security.HttpAuthInfo;
import com.huawei.graphbase.rest.util.DataType;
import com.huawei.graphbase.rest.util.ElementCategory;
import com.huawei.graphbase.rest.util.IndexType;
import com.huawei.graphbase.rest.util.PropertyPredicate;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <p>
 * GraphBase Example Based on REST API
 * <br> The social graph in a typical scene:
 * <br>
 * marko,29-----------------------friend-----------------------------josb,32       <br>
 * &nbsp; |++\----------------------------------------------------------------/++++|     <br>
 * &nbsp; |++-\--------------------------------------------------------------/-++++|     <br>
 * &nbsp; |    150xxxx0000------------------------------------------152xxxx2222    |     <br>
 * &nbsp; |+++------------------------------------------------------------------+++|     <br>
 * knows------------------------------------------------------------------knows    <br>
 * &nbsp; |+++------------------------------------------------------------------+++|     <br>
 * &nbsp; |    137xxxx7777----------------call----------------------136xxxx6666    |     <br>
 * &nbsp; |+++-/--------------------------------------------------------------\-+++|     <br>
 * &nbsp; |++-/+--------------------------------------------------------------+\-++|     <br>
 * vadas,27 ------------------------------------------------------------ blame,30 <br>
 * </p>
 *
 * @author graphbase
 */
public class GraphBaseRestExample {
    private static final Logger LOG = LoggerFactory.getLogger(GraphBaseRestExample.class);

    private static final String DEFAULT_CONFIG_FILE = "conf" + File.separator + "graphbase.properties";

    private static final String SCHEMA_FILE = "conf" + File.separator + "Person.xml";

    private static final String KEY_TAB = "conf" + File.separator + "user.keytab";

    public static void main(String[] args) throws Exception {

        GraphHttpClient client = null;
        InputStream inputStream = null;
        try {

            /***************************** Read Configuration and Build Client ***************************/
            inputStream = new FileInputStream(System.getProperty("user.dir") + File.separator + DEFAULT_CONFIG_FILE);
            Properties p = new Properties();
            p.load(inputStream);
            // Passwords stored in plaintext pose security risks. Store them in ciphertext in configuration files
            // and decrypted during use to ensure security.
            HttpAuthInfo httpAuthInfo = HttpAuthInfo.newBuilder()
                .setIp(p.getProperty("ip"))
                .setPort(Integer.valueOf(p.getProperty("port")))
                .setService(RestApi.SERVICE)
                .setUsername(p.getProperty("userName"))
                .setPassword(p.getProperty("password"))
                .setKeytabFile(System.getProperty("user.dir") + File.separator + KEY_TAB)
                .build();

            /*
             * usage: kerberos login based on username and password,
             * note: Password expired after three months
             */
            /*client = GraphHttpClient.newPasswordClient(httpAuthInfo);*/
            /*
             * usage: kerberos login based on username and keytab,
             * note: Password never expired
             */
            client = GraphHttpClient.newKeytabClient(httpAuthInfo);

            /***************************** Create Graph and Build Its Schema ******************************/
            RestApi api = new RestApi(client);
            String graphName = "graphbase";
            /* Create graph */
            api.createGraph(graphName);
            // Create schema in uploading schema.xml way
            /*File file = FileUtils.getFile(System.getProperty("user.dir") + File.separator + SCHEMA_FILE);
             api.addSchema(file, graphName);
             */
            /* Create Schema for Graph
             * Includes: adding vertex labels, edge labels, and property keys, and then building indexes based on properties
             */
            /* add vertex labels */
            api.addVertexLabel("person", graphName);
            api.addVertexLabel("phone", graphName);
            /* query vertex labels */
            api.queryVertexLabel("person", graphName);
            api.queryAllVertexLabel(graphName);
            /* add edge labels */
            EdgeLabel edgeLabel = new EdgeLabel();
            edgeLabel.setName("friend");
            api.addEdgeLabel(edgeLabel, graphName);
            edgeLabel = new EdgeLabel();
            edgeLabel.setName("knows");
            api.addEdgeLabel(edgeLabel, graphName);
            edgeLabel = new EdgeLabel();
            edgeLabel.setName("call");
            api.addEdgeLabel(edgeLabel, graphName);
            edgeLabel = new EdgeLabel();
            edgeLabel.setName("has");
            api.addEdgeLabel(edgeLabel, graphName);
            /* query edge labels */
            api.queryEdgeLabel("friend", graphName);
            api.queryAllEdgeLabel(graphName);
            /* add property keys */
            PropertyKey propertyKey = new PropertyKey();
            propertyKey.setDataType(DataType.String);
            propertyKey.setName("name");
            api.addPropertyKey(propertyKey, graphName);
            propertyKey = new PropertyKey();
            propertyKey.setDataType(DataType.Integer);
            propertyKey.setName("age");
            api.addPropertyKey(propertyKey, graphName);
            propertyKey = new PropertyKey();
            propertyKey.setDataType(DataType.String);
            propertyKey.setName("telephone");
            api.addPropertyKey(propertyKey, graphName);
            propertyKey = new PropertyKey();
            propertyKey.setDataType(DataType.Float);
            propertyKey.setName("weight");
            api.addPropertyKey(propertyKey, graphName);
            /* query property key */
            api.queryPropertyKey("name", graphName);
            /* query the whole property key */
            api.queryAllPropertyKey(graphName);
            /* add index. Wherever practicable, creating schema shall be completed prior to this. */
            addIndex(api, graphName);
            /* query the whole indexes */
            api.queryAllIndex(graphName);

            /***************************** Write Graph Data **************************************************/
            /* add vertex with label 'person' */
            addVertexPerson(api, graphName);
            /* add vertex with label 'phone' */
            addVertexPhone(api, graphName);
            /* get vertex number in entire graph */
            api.countVertex(graphName);
            /* get vertex number in entire graph by vertexLabel */
            api.countVertexByLabel(graphName, "person");
            /* query vertex according to its id */
            String vertexId = getVertexIdByProperty(api, graphName, "person", "name", "marko");
            api.queryVertex(vertexId, graphName);
            /* batch query vertex according to ids */
            String vertexId_marko = getVertexIdByProperty(api, graphName, "person", "name", "marko");
            String vertexId_vadas = getVertexIdByProperty(api, graphName, "person", "name", "vadas");
            String vertexId_josh = getVertexIdByProperty(api, graphName, "person", "name", "josh");
            String vertexId_blame = getVertexIdByProperty(api, graphName, "person", "name", "blame");
            List<String> idList = new ArrayList<String>();
            idList.add(vertexId_marko);
            idList.add(vertexId_vadas);
            idList.add(vertexId_josh);
            idList.add(vertexId_blame);
            api.batchQueryVertex(idList, graphName);
            /* add edges */
            addEdges(api, graphName);
            /* get edge number in entire graph */
            api.countEdge(graphName);
            /* get edge number in entire graph */
            api.countEdgeByLabel(graphName, "knows");
            /* query edge according to its id */
            String edgeId = getEdgeIdByProperty(api, graphName, "call", "weight", "0.6");
            api.queryEdge(edgeId, graphName);

            /***************************** Read Graph Data **************************************************/
            /*  search vertices in entire graph, predicate:[label=person, age>29, limit=3] and the result sorted by age decreasing */
            VertexSearchReqObj vertexSearchReqObj = new VertexSearchReqObj();
            vertexSearchReqObj.setVertexLabel("person");
            List<PropertyFilter> propertyFilterList1 = new ArrayList<>();
            PropertyFilter propertyFilter = new PropertyFilter();
            propertyFilter.setPropertyName("age");
            propertyFilter.setPredicate(PropertyPredicate.GREATER_THAN);
            List<Integer> values = new ArrayList<>();
            values.add(29);
            propertyFilter.setValues(values);
            propertyFilterList1.add(propertyFilter);
            vertexSearchReqObj.setFilterList(propertyFilterList1);
            List<PropertyKeySort> sortList = new ArrayList<>();
            PropertyKeySort propertyKeySort = new PropertyKeySort();
            propertyKeySort.setPropertyKeyName("age");
            propertyKeySort.setSortType("ASC");
            sortList.add(propertyKeySort);
            vertexSearchReqObj.setPropertyKeySortList(sortList);
            vertexSearchReqObj.setLimit(3);
            api.searchVertex(vertexSearchReqObj, graphName);
            /* search edges in entire graph, predicate:[label=knows, weight<=0.6] */
            EdgeSearchReqObj edgeSearchReqObj = new EdgeSearchReqObj();
            edgeSearchReqObj.setEdgeLabel("knows");
            edgeSearchReqObj.setLimit(2);
            List<PropertyFilter> propertyFilterList2 = new ArrayList<>();
            PropertyFilter propertyFilter2 = new PropertyFilter();
            propertyFilter2.setPropertyName("weight");
            propertyFilter2.setPredicate(PropertyPredicate.LESS_THAN_EQUAL);
            List<Float> values2 = new ArrayList();
            values2.add(new Float(0.6));
            propertyFilter2.setValues(values2);
            propertyFilterList2.add(propertyFilter2);
            edgeSearchReqObj.setFilterList(propertyFilterList2);
            api.searchEdge(edgeSearchReqObj, graphName);

            /***************************** Running Graph Algorithms **************************************************/
            /* Expanding line inquiry(lines):
             * predicate：expanding path with start vertex 1
             *   layer 1: the vertex must be 'person' type, but the age not be greater than 29
             *   layer 2: the vertex must be 'phone' type and have telephone 137xxxx4211
             */
            lineSearch(api, graphName);
            /* query all paths
             * find all paths between vertex 1 to vertex 8
             */
            allPathSearch(api, graphName);
            /* find shortest path from vertex 1 to vertex 8 */
            shortestPathSearch(api, graphName);
            /* find all paths between vertex 1 to vertex 8 and
             * every vertex must be 'person' type and the age not be greater than 29
             */
            vertexFilterPathSearch(api, graphName);

            /***************************** Client Kill Task ***********************************************************/
            /* query task with running status */
            List<Task> tasks = api.queryAllTask();
            /* kill the first running task */
            Task task = null;
            if (null != tasks && tasks.size() > 0) {
                task = tasks.get(0);
            }
            if (task != null) {
                api.deleteTask(task.getId(), task.getInstanceIp());
            }

            /***************************** Delete Graph and Running Over **********************************************/
            api.deleteGraph(graphName);

        } catch (Exception e) {
            LOG.info(e.getMessage());
        } finally {
            if (null != inputStream) {
                inputStream.close();
            }
            if (null != client) {
                client.httpClient.close();
            }
        }
    }

    private static String getEdgeIdByProperty(RestApi api, String graphName, String edgeLabel, String name,
        String value) {
        String edgeId = "";
        EdgeSearchReqObj edgeSearchReqObj = new EdgeSearchReqObj();
        edgeSearchReqObj.setEdgeLabel(edgeLabel);
        edgeSearchReqObj.setLimit(1);
        List<PropertyFilter> propertyFilterList = new ArrayList<>();
        PropertyFilter propertyFilter = new PropertyFilter();
        propertyFilter.setPropertyName(name);
        propertyFilter.setPredicate(PropertyPredicate.EQUAL);
        List<Float> values = new ArrayList();
        values.add(new Float(value));
        propertyFilter.setValues(values);
        propertyFilterList.add(propertyFilter);
        edgeSearchReqObj.setFilterList(propertyFilterList);
        EdgeSearchRspObj rspObj = api.searchEdge(edgeSearchReqObj, graphName);
        edgeId = rspObj.getEdgeList().get(0).getEdgeId();
        return edgeId;
    }

    private static void lineSearch(RestApi api, String graphName) {
        LineSearchReqObj lineSearchReqObj = new LineSearchReqObj();
        List<Long> vertexIdList = new ArrayList<Long>();
        String vertexId = getVertexIdByProperty(api, graphName, "person", "name", "marko");
        vertexIdList.add(Long.valueOf(vertexId));
        lineSearchReqObj.setVertexIdList(vertexIdList);
        lineSearchReqObj.setOnlyLastLayer(false);
        lineSearchReqObj.setWithPath(false);
        List<VertexFilter> vertexFilterList = new ArrayList<>();
        VertexFilter vertexFilter = new VertexFilter();
        List<PropertyFilter> propertyFilterList1 = new ArrayList<>();
        PropertyFilter propertyFilter = new PropertyFilter();
        propertyFilter.setPropertyName("age");
        propertyFilter.setPredicate(PropertyPredicate.LESS_THAN_EQUAL);
        List<Integer> values2 = new ArrayList<Integer>();
        values2.add(29);
        propertyFilter.setValues(values2);
        propertyFilterList1.add(propertyFilter);
        vertexFilter.setFilterList(propertyFilterList1);
        vertexFilterList.add(vertexFilter);
        vertexFilter = new VertexFilter();
        List<String> vertexLabelList = new ArrayList<>();
        vertexLabelList.add("phone");
        vertexFilter.setVertexLabelList(vertexLabelList);
        vertexFilterList.add(vertexFilter);
        lineSearchReqObj.setVertexFilterList(vertexFilterList);
        lineSearchReqObj.setLayer(2);
        lineSearchReqObj.setVertexEdgeLimit(10);
        lineSearchReqObj.setLimit(5);
        api.searchLines(lineSearchReqObj, graphName);
    }

    private static void vertexFilterPathSearch(RestApi api, String graphName) {
        PathSearchReqObj pathSearchReqObj = new PathSearchReqObj();
        List<String> vertexIdList = new ArrayList<>();
        vertexIdList.add(getVertexIdByProperty(api, graphName, "person", "name", "marko"));
        vertexIdList.add(getVertexIdByProperty(api, graphName, "person", "name", "blame"));
        pathSearchReqObj.setVertexIdList(vertexIdList);
        VertexFilter vertexFilter = new VertexFilter();
        List<PropertyFilter> propertyFilterList3 = new ArrayList<>();
        PropertyFilter propertyFilter = new PropertyFilter();
        propertyFilter.setPropertyName("age");
        propertyFilter.setPredicate(PropertyPredicate.GREATER_THAN_EQUAL);
        List<Integer> values2 = new ArrayList();
        values2.add(29);
        propertyFilter.setValues(values2);
        propertyFilterList3.add(propertyFilter);
        vertexFilter.setFilterList(propertyFilterList3);
        pathSearchReqObj.setVertexFilter(vertexFilter);
        pathSearchReqObj.setLayer(7);
        api.searchPath(pathSearchReqObj, graphName);
    }

    private static void shortestPathSearch(RestApi api, String graphName) {
        PathSearchReqObj pathSearchReqObj = new PathSearchReqObj();
        List<String> vertexIdList = new ArrayList<>();
        vertexIdList.add(getVertexIdByProperty(api, graphName, "person", "name", "marko"));
        vertexIdList.add(getVertexIdByProperty(api, graphName, "person", "name", "blame"));
        pathSearchReqObj.setVertexIdList(vertexIdList);
        pathSearchReqObj.setLayer(7);
        pathSearchReqObj.setOption("shortest");// all is default
        api.searchPath(pathSearchReqObj, graphName);
    }

    private static void allPathSearch(RestApi api, String graphName) {
        PathSearchReqObj pathSearchReqObj = new PathSearchReqObj();
        List<String> vertexIdList = new ArrayList<>();
        vertexIdList.add(getVertexIdByProperty(api, graphName, "person", "name", "marko"));
        vertexIdList.add(getVertexIdByProperty(api, graphName, "person", "name", "blame"));
        pathSearchReqObj.setVertexIdList(vertexIdList);
        pathSearchReqObj.setLayer(7);
        api.searchPath(pathSearchReqObj, graphName);
    }

    private static void addIndex(RestApi api, String graphName) {
        // add index base on property 'name' with type 'COMPOSITE'
        GraphIndexReqObj graphIndexReqObj = new GraphIndexReqObj();
        graphIndexReqObj.setElementCategory(ElementCategory.VERTEX);
        graphIndexReqObj.setName("name_index");
        graphIndexReqObj.setType(IndexType.COMPOSITE);
        List<KeyTextType> keyTextTypeList1 = new ArrayList<>();
        KeyTextType keyTextType = new KeyTextType();
        keyTextType.setName("name");
        keyTextTypeList1.add(keyTextType);
        graphIndexReqObj.setKeyTextTypeList(keyTextTypeList1);
        api.addGraphIndex(graphIndexReqObj, graphName);

        // add index base on property 'telephone' with type 'COMPOSITE'
        graphIndexReqObj = new GraphIndexReqObj();
        graphIndexReqObj.setElementCategory(ElementCategory.VERTEX);
        graphIndexReqObj.setName("telephone_index");
        graphIndexReqObj.setType(IndexType.COMPOSITE);
        List<KeyTextType> keyTextTypeList3 = new ArrayList<>();
        keyTextType = new KeyTextType();
        keyTextType.setName("telephone");
        keyTextTypeList3.add(keyTextType);
        graphIndexReqObj.setKeyTextTypeList(keyTextTypeList3);
        api.addGraphIndex(graphIndexReqObj, graphName);

    }

    private static void addEdges(RestApi api, String graphName) {
        // create edge with number 9
        AddEdgeReqObj addEdgeReqObj = new AddEdgeReqObj();
        addEdgeReqObj.setEdgeLabel("knows");
        addEdgeReqObj.setOutVertexId(getVertexIdByProperty(api, graphName, "person", "name", "marko"));
        addEdgeReqObj.setInVertexId(getVertexIdByProperty(api, graphName, "person", "name", "vadas"));
        List<PropertyReqObj> propertyReqObjList1 = new ArrayList<>();
        PropertyReqObj propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("weight");
        propertyReqObj.setValue(0.4);
        propertyReqObjList1.add(propertyReqObj);
        addEdgeReqObj.setPropertyList(propertyReqObjList1);
        api.addEdge(addEdgeReqObj, graphName);

        // create edge with number 10
        addEdgeReqObj = new AddEdgeReqObj();
        addEdgeReqObj.setEdgeLabel("has");
        addEdgeReqObj.setOutVertexId(getVertexIdByProperty(api, graphName, "person", "name", "marko"));
        addEdgeReqObj.setInVertexId(getVertexIdByProperty(api, graphName, "phone", "telephone", "150xxxx0000"));
        List<PropertyReqObj> propertyReqObjList2 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("weight");
        propertyReqObj.setValue(0.8);
        propertyReqObjList2.add(propertyReqObj);
        addEdgeReqObj.setPropertyList(propertyReqObjList2);
        api.addEdge(addEdgeReqObj, graphName);

        // create edge with number 11
        addEdgeReqObj = new AddEdgeReqObj();
        addEdgeReqObj.setEdgeLabel("has");
        addEdgeReqObj.setOutVertexId(getVertexIdByProperty(api, graphName, "person", "name", "josh"));
        addEdgeReqObj.setInVertexId(getVertexIdByProperty(api, graphName, "phone", "telephone", "152xxxx2222"));
        List<PropertyReqObj> propertyReqObjList3 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("weight");
        propertyReqObj.setValue(0.8);
        propertyReqObjList3.add(propertyReqObj);
        addEdgeReqObj.setPropertyList(propertyReqObjList3);
        api.addEdge(addEdgeReqObj, graphName);

        // create edge with number 12
        addEdgeReqObj = new AddEdgeReqObj();
        addEdgeReqObj.setEdgeLabel("knows");
        addEdgeReqObj.setOutVertexId(getVertexIdByProperty(api, graphName, "person", "name", "josh"));
        addEdgeReqObj.setInVertexId(getVertexIdByProperty(api, graphName, "person", "name", "blame"));
        List<PropertyReqObj> propertyReqObjList4 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("weight");
        propertyReqObj.setValue(0.4);
        propertyReqObjList4.add(propertyReqObj);
        addEdgeReqObj.setPropertyList(propertyReqObjList4);
        api.addEdge(addEdgeReqObj, graphName);

        // create edge with number 13
        addEdgeReqObj = new AddEdgeReqObj();
        addEdgeReqObj.setEdgeLabel("has");
        addEdgeReqObj.setOutVertexId(getVertexIdByProperty(api, graphName, "person", "name", "blame"));
        addEdgeReqObj.setInVertexId(getVertexIdByProperty(api, graphName, "phone", "telephone", "136xxxx6666"));
        List<PropertyReqObj> propertyReqObjList5 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("weight");
        propertyReqObj.setValue(0.8);
        propertyReqObjList5.add(propertyReqObj);
        addEdgeReqObj.setPropertyList(propertyReqObjList5);
        api.addEdge(addEdgeReqObj, graphName);

        // create edge with number 14, includes two edges with direction out and in
        addEdgeReqObj = new AddEdgeReqObj();
        addEdgeReqObj.setEdgeLabel("call");
        addEdgeReqObj.setOutVertexId(getVertexIdByProperty(api, graphName, "phone", "telephone", "136xxxx6666"));
        addEdgeReqObj.setInVertexId(getVertexIdByProperty(api, graphName, "phone", "telephone", "137xxxx7777"));
        List<PropertyReqObj> propertyReqObjList6 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("weight");
        propertyReqObj.setValue(0.6);
        propertyReqObjList6.add(propertyReqObj);
        addEdgeReqObj.setPropertyList(propertyReqObjList6);
        api.addEdge(addEdgeReqObj, graphName);
        addEdgeReqObj = new AddEdgeReqObj();
        addEdgeReqObj.setEdgeLabel("call");
        addEdgeReqObj.setOutVertexId(getVertexIdByProperty(api, graphName, "phone", "telephone", "137xxxx7777"));
        addEdgeReqObj.setInVertexId(getVertexIdByProperty(api, graphName, "phone", "telephone", "136xxxx6666"));
        List<PropertyReqObj> propertyReqObjList7 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("weight");
        propertyReqObj.setValue(0.6);
        propertyReqObjList7.add(propertyReqObj);
        addEdgeReqObj.setPropertyList(propertyReqObjList7);
        api.addEdge(addEdgeReqObj, graphName);

        // create edge with number 15
        addEdgeReqObj = new AddEdgeReqObj();
        addEdgeReqObj.setEdgeLabel("has");
        addEdgeReqObj.setOutVertexId(getVertexIdByProperty(api, graphName, "person", "name", "vadas"));
        addEdgeReqObj.setInVertexId(getVertexIdByProperty(api, graphName, "phone", "telephone", "137xxxx7777"));
        List<PropertyReqObj> propertyReqObjList8 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("weight");
        propertyReqObj.setValue(0.8);
        propertyReqObjList8.add(propertyReqObj);
        addEdgeReqObj.setPropertyList(propertyReqObjList8);
        api.addEdge(addEdgeReqObj, graphName);

        // create edge with number 16
        addEdgeReqObj = new AddEdgeReqObj();
        addEdgeReqObj.setEdgeLabel("friend");
        addEdgeReqObj.setOutVertexId(getVertexIdByProperty(api, graphName, "person", "name", "marko"));
        addEdgeReqObj.setInVertexId(getVertexIdByProperty(api, graphName, "person", "name", "josh"));
        List<PropertyReqObj> propertyReqObjList9 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("weight");
        propertyReqObj.setValue(1.0);
        propertyReqObjList9.add(propertyReqObj);
        addEdgeReqObj.setPropertyList(propertyReqObjList9);
        api.addEdge(addEdgeReqObj, graphName);

        // Avoid latency for data query latency
        try {
            Thread.sleep(1000L);
        } catch (InterruptedException e) {
        }
    }

    private static String getVertexIdByProperty(RestApi api, String graphName, String vertexLabel,
        String propertyKeyName, String value) {
        String vertexId = "";
        VertexSearchReqObj vertexSearchReqObj = new VertexSearchReqObj();
        vertexSearchReqObj.setVertexLabel(vertexLabel);
        List<PropertyFilter> propertyFilterList = new ArrayList<>();
        PropertyFilter propertyFilter = new PropertyFilter();
        propertyFilter.setPropertyName(propertyKeyName);
        propertyFilter.setPredicate(PropertyPredicate.EQUAL);
        List<String> values = new ArrayList<String>();
        values.add(value);
        propertyFilter.setValues(values);
        propertyFilterList.add(propertyFilter);
        vertexSearchReqObj.setFilterList(propertyFilterList);
        vertexSearchReqObj.setLimit(1);
        VertexSearchRspObj rspObj = api.searchVertex(vertexSearchReqObj, graphName);
        vertexId = rspObj.getVertexList().get(0).getId();
        return vertexId;
    }

    private static void addVertexPhone(RestApi api, String graphName) {
        // create 'phone' vertex with number 2
        AddVertexReqObj addVertexReqObj = new AddVertexReqObj();
        addVertexReqObj.setVertexLabel("phone");
        List<PropertyReqObj> propertyReqObjList1 = new ArrayList<>();
        PropertyReqObj propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("telephone");
        propertyReqObj.setValue("150xxxx0000");
        propertyReqObjList1.add(propertyReqObj);
        addVertexReqObj.setPropertyList(propertyReqObjList1);
        api.addVertex(addVertexReqObj, graphName);

        // create 'phone' vertex with number 3
        addVertexReqObj = new AddVertexReqObj();
        addVertexReqObj.setVertexLabel("phone");
        List<PropertyReqObj> propertyReqObjList2 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("telephone");
        propertyReqObj.setValue("152xxxx2222");
        propertyReqObjList2.add(propertyReqObj);
        addVertexReqObj.setPropertyList(propertyReqObjList2);
        api.addVertex(addVertexReqObj, graphName);

        // create 'phone' vertex with number 6
        addVertexReqObj = new AddVertexReqObj();
        addVertexReqObj.setVertexLabel("phone");
        List<PropertyReqObj> propertyReqObjList3 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("telephone");
        propertyReqObj.setValue("136xxxx6666");
        propertyReqObjList3.add(propertyReqObj);
        addVertexReqObj.setPropertyList(propertyReqObjList3);
        api.addVertex(addVertexReqObj, graphName);

        // create 'phone' vertex with number 7
        addVertexReqObj = new AddVertexReqObj();
        addVertexReqObj.setVertexLabel("phone");
        List<PropertyReqObj> propertyReqObjList4 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("telephone");
        propertyReqObj.setValue("137xxxx7777");
        propertyReqObjList4.add(propertyReqObj);
        addVertexReqObj.setPropertyList(propertyReqObjList4);
        api.addVertex(addVertexReqObj, graphName);
    }

    private static void addVertexPerson(RestApi api, String graphName) {
        // create 'person' vertex with number 1
        AddVertexReqObj addVertexReqObj = new AddVertexReqObj();
        addVertexReqObj.setVertexLabel("person");
        List<PropertyReqObj> propertyReqObjList1 = new ArrayList<>();
        PropertyReqObj propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("name");
        propertyReqObj.setValue("marko");
        propertyReqObjList1.add(propertyReqObj);
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("age");
        propertyReqObj.setValue(29);
        propertyReqObjList1.add(propertyReqObj);
        addVertexReqObj.setPropertyList(propertyReqObjList1);
        api.addVertex(addVertexReqObj, graphName);

        // create 'person' vertex with number 4
        addVertexReqObj = new AddVertexReqObj();
        addVertexReqObj.setVertexLabel("person");
        List<PropertyReqObj> propertyReqObjList2 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("name");
        propertyReqObj.setValue("josh");
        propertyReqObjList2.add(propertyReqObj);
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("age");
        propertyReqObj.setValue(32);
        propertyReqObjList2.add(propertyReqObj);
        addVertexReqObj.setPropertyList(propertyReqObjList2);
        api.addVertex(addVertexReqObj, graphName);

        // create 'person' vertex with number 5
        addVertexReqObj = new AddVertexReqObj();
        addVertexReqObj.setVertexLabel("person");
        List<PropertyReqObj> propertyReqObjList3 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("name");
        propertyReqObj.setValue("vadas");
        propertyReqObjList3.add(propertyReqObj);
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("age");
        propertyReqObj.setValue(27);
        propertyReqObjList3.add(propertyReqObj);
        addVertexReqObj.setPropertyList(propertyReqObjList3);
        api.addVertex(addVertexReqObj, graphName);

        // create 'person' vertex with number 8
        addVertexReqObj = new AddVertexReqObj();
        addVertexReqObj.setVertexLabel("person");
        List<PropertyReqObj> propertyReqObjList4 = new ArrayList<>();
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("name");
        propertyReqObj.setValue("blame");
        propertyReqObjList4.add(propertyReqObj);
        propertyReqObj = new PropertyReqObj();
        propertyReqObj.setName("age");
        propertyReqObj.setValue(30);
        propertyReqObjList4.add(propertyReqObj);
        addVertexReqObj.setPropertyList(propertyReqObjList4);
        api.addVertex(addVertexReqObj, graphName);
    }
}
