package com.huawei.graphbase.gremlin;

import com.huawei.graphbase.gremlin.util.LoginUtil;
import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Future;

public class GremlinClusterClientDemo {

    /**
     * gremlin submit gremlin dsl
     *
     * @param client
     * @param gremlin
     * @return
     */
    public static ResultSet submitGremlinDsl(Client client, String gremlin) {
        return client.submit(gremlin);
    }

    /**
     * gremlin asynchronous submit gremlin dsl
     *
     * @param client
     * @param gremlin
     * @return
     */
    public static Future<ResultSet> submitAsyncGremlinDsl(Client client, String gremlin) {
        return client.submitAsync(gremlin);
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
     * @param resultSet
     */
    private static void println(ResultSet resultSet) {
        for (Result result : resultSet) {
            System.out.println(result.getString());
        }
    }

    public static void main(String[] args) throws Exception {
        //kerberos login
        krbLogin();
        // connect gremlinserver
        Cluster cluster = GremlinClient.getInstance().getCluster();
        //graph name
        String graphName = "graphName";
        Client client = cluster.connect().alias(graphName);

        //submit gremlin dsl
        ResultSet resultSet = submitGremlinDsl(client, "g.V().limit(10)");
        println(resultSet);

        //asynchronous submit gremlin dsl
        Future<ResultSet> future = submitAsyncGremlinDsl(client, "g.V().limit(10)");
        println(future.get());

        System.out.println("--------------vertex properties--------------");
        ResultSet vertexP = submitGremlinDsl(client, "g.V().hasLabel('person').valueMap('name','age')");
        println(vertexP);

        System.out.println("--------------edge properties--------------");
        ResultSet edgeP = submitGremlinDsl(client, "g.E().valueMap()");
        println(edgeP);

        ResultSet resultSet2 = submitGremlinDsl(client, "g.V().has('name', 'josh')");
        String josh = String.valueOf(resultSet2.one().getVertex().id());
        System.out.println("--------------move to the outgoing adjacent vertices--------------");
        String dsl0 = "g.V(" + josh + ").out().valueMap()";
        ResultSet resultSet3 = submitGremlinDsl(client, dsl0);
        println(resultSet3);

        System.out.println("--------------move to the incoming adjacent vertices--------------");
        String dsl1 = "g.V(" + josh + ").in().valueMap()";
        ResultSet resultSet4 = submitGremlinDsl(client, dsl1);
        println(resultSet4);

        System.out.println("--------------move to both the incoming and outgoing adjacent vertices--------------");
        String dsl2 = "g.V(" + josh + ").both().valueMap()";
        ResultSet resultSet5 = submitGremlinDsl(client, dsl2);
        println(resultSet5);

        System.out.println("--------------move to the outgoing incident edges--------------");
        String dsl3 = "g.V(" + josh + ").outE().valueMap()";
        ResultSet resultSet6 = submitGremlinDsl(client, dsl3);
        println(resultSet6);

        System.out.println("--------------move to the incoming incident edges--------------");
        String dsl4 = "g.V(" + josh + ").inE().valueMap()";
        ResultSet resultSet7 = submitGremlinDsl(client, dsl4);
        println(resultSet7);

        System.out.println("--------------move to both the incoming and outgoing incident edges--------------");
        String dsl5 = "g.V(" + josh + ").bothE().valueMap()";
        ResultSet resultSet8 = submitGremlinDsl(client, dsl5);
        println(resultSet8);

        System.out.println("--------------path--------------");
        ResultSet resultSet9 = submitGremlinDsl(client, "g.V().out().out().valueMap().path()");
        println(resultSet9);

        System.out.println("--------------simple path--------------");
        ResultSet resultSet10 = submitGremlinDsl(client, "g.V().out().out().simplePath().path()");
        println(resultSet10);

        System.out.println("--------------cyclic path--------------");
        ResultSet resultSet11 = submitGremlinDsl(client, "g.V().out().out().cyclicPath().path()");
        println(resultSet11);

        System.out.println("-------------find all vertices who have an age-property ---------------");
        ResultSet resultSet12 = submitGremlinDsl(client, "g.V().properties().hasKey('age').value()");
        println(resultSet12);

        System.out.println("-------------find all vertices given 'person' label---------------");
        ResultSet resultSet13 = submitGremlinDsl(client, "g.V().hasLabel('person')");
        println(resultSet13);

        System.out.println("-------------qury all vertices ordered by age property ascending---------------");
        ResultSet resultSet14 = submitGremlinDsl(client,
            "g.V().hasLabel('person').order().by('age', incr).valueMap('name','age')");
        println(resultSet14);

        System.out.println("-------------qury all vertices ordered by age property descending---------------");
        ResultSet resultSet15 = submitGremlinDsl(client,
            "g.V().hasLabel('person').order().by('age', decr).valueMap('name','age')");
        println(resultSet15);

        System.out.println("-------------ordered by age property ascending, and query first 2 vertices---------------");
        ResultSet resultSet16 = submitGremlinDsl(client,
            "g.V().hasLabel('person').order().by('age', incr).limit(2).valueMap('name','age')");
        println(resultSet16);

        System.out.println("------------ join ---------------");
        ResultSet resultSet17 = submitGremlinDsl(client,
            "g.V().hasLabel('person').match(__.as('c').values('name').as('name'),__.as('c').out('friend','knows').values('name').as('friend_or_knows')).select('name', 'friend_or_knows')");
        println(resultSet17);

        System.out.println("------------ union---------------");
        ResultSet resultSet18 = submitGremlinDsl(client,
            "g.V().has('name','josh').union(__.in('friend').values('name'),__.out('knows').values('name'))");
        println(resultSet18);

        System.out.println("------------count vertices---------------");
        ResultSet resultSet19 = submitGremlinDsl(client, "g.V().count()");
        println(resultSet19);

        System.out.println("------------count edges---------------");
        ResultSet resultSet20 = submitGremlinDsl(client, "g.E().count()");
        println(resultSet20);

        //close
        GremlinClient.getInstance().close();
    }
}
