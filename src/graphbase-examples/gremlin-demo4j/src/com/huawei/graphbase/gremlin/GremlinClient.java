package com.huawei.graphbase.gremlin;

import com.huawei.graphbase.gremlin.util.FileFinder;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GremlinClient {

    /**
     * gremlin config
     */
    private static final String CONFIG_FILE_PATH = "remote-objects.yaml";

    /**
     * singleton
     */
    private static GremlinClient instance = null;

    /**
     * gremlin server cluster
     */
    private Cluster cluster;

    /**
     * graph instance
     */
    private Graph graph;

    /**
     * private construct
     */
    private GremlinClient() {
        try {
            connectAndCreateClient();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.graph = EmptyGraph.instance();
    }

    /**
     * @return singleton
     */
    public static GremlinClient getInstance() {
        if (null != instance) {
            return instance;
        }
        synchronized (GremlinClient.class) {
            if (null != instance) {
                return instance;
            }
            instance = new GremlinClient();
            return instance;
        }
    }

    /**
     * connect gremlin server, construct client
     */
    private void connectAndCreateClient() throws IOException {
        String keytab = System.getProperty("user.dir") + File.separator + "conf" + File.separator + CONFIG_FILE_PATH;
        InputStream in = null;
        try {
            in = FileFinder.loadConfig(keytab);
            YamlConfiguration configuration = new YamlConfiguration();
            configuration.load(new InputStreamReader(in));
            this.cluster = Cluster.open(configuration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    /**
     * @return gremlin server cluster
     */
    public Cluster getCluster() {
        return cluster;
    }

    /**
     * @return graph instance getter
     */
    public Graph getGraph() {
        return graph;
    }

    /**
     * close
     */
    public void close() throws Exception {
        this.cluster.close();
    }
}
