package com.huawei.graphbase.gremlin;

import com.huawei.graphbase.gremlin.util.LoginUtil;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GraphTraversalDemo {

    /**
     * find vertices given ids
     *
     * @param g
     * @param ids
     * @return
     */
    public static Iterator<Vertex> vertices(GraphTraversalSource g, List<Long> ids) {
        return g.V(ids);
    }

    /**
     * find vertices who has propety/value
     *
     * @param g
     * @param property
     * @param value
     * @return
     */
    public static Iterator<Vertex> vertices(GraphTraversalSource g, String property, Object value) {
        return g.V().has(property, value);
    }

    /**
     * find vertices given range condition
     *
     * @param g
     * @param property
     * @param start
     * @param end
     * @return
     */
    public static Iterator<Vertex> vertices(GraphTraversalSource g, String property, Object start, Object end) {
        return g.V().has(property, P.between(start, end));
    }

    /**
     * find vertex properties given ids and properties
     *
     * @param g
     * @param ids
     * @param properties
     * @return
     */
    public static Iterator<Map<Object, Object>> verticesProperties(GraphTraversalSource g, List<Long> ids,
        String... properties) {
        return g.V(ids).valueMap(properties);
    }

    /**
     * find edges given ids
     *
     * @param g
     * @param ids
     * @return
     */
    public static Iterator<Edge> edges(GraphTraversalSource g, List<String> ids) {
        return g.E(ids);
    }

    /**
     * find edges who has propety/value
     *
     * @param g
     * @param property
     * @param value
     * @return
     */
    public static Iterator<Edge> edges(GraphTraversalSource g, String property, Object value) {
        return g.E().has(property, value);
    }

    /**
     * find edges given range condition
     *
     * @param g
     * @param property
     * @param start
     * @param end
     * @return
     */
    public static Iterator<Edge> edges(GraphTraversalSource g, String property, Object start, Object end) {
        return g.E().has(property, P.between(start, end));
    }

    /**
     * find edge properties given ids and properties
     *
     * @param g
     * @param ids
     * @return
     */
    public static Iterator<Map<Object, Object>> edgesProperties(GraphTraversalSource g, List<String> ids,
        String... properties) {
        return g.E(ids).valueMap(properties);
    }

    /**
     * kerberos login
     *
     * @throws IOException
     */
    private static void krbLogin() throws IOException {
        String user_principal = "graphbase_username";
        String krb5Path = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "krb5.conf";
        File userkrb5File = FileUtils.getFile(krb5Path);
        if (!userkrb5File.exists()) {
            throw new RuntimeException("userkrb5File(" + userkrb5File.getCanonicalPath() + ") does not exsit.");
        }
        //set krb5 file
        System.setProperty("java.security.krb5.conf", userkrb5File.getCanonicalPath());

        //set ketytab,jaasconf
        String keytab = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "user.keytab";
        LoginUtil.setJaasConf("gremlinclient", user_principal, keytab);
    }

    /**
     * print result
     *
     * @param iterator
     */
    private static void println(Iterator iterator) {
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }

    public static void main(String[] args) throws Exception {
        //kerberos login
        krbLogin();
        //connect gremlinserver
        Cluster cluster = GremlinClient.getInstance().getCluster();
        //grpah instance
        Graph graph = GremlinClient.getInstance().getGraph();
        //graph traversal
        GraphTraversalSource g = graph.traversal().withRemote(DriverRemoteConnection.using(cluster, "图名称"));
        //traversal vertices
        println(g.V().limit(10));
        //traversal edges
        println(g.E().limit(10));

        //close
        GremlinClient.getInstance().close();
    }

}
