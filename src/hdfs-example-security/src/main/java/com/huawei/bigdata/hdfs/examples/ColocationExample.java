package com.huawei.bigdata.hdfs.examples;

import com.huawei.hadoop.oi.colocation.DFSColocationAdmin;
import com.huawei.hadoop.oi.colocation.DFSColocationClient;
import com.huawei.hadoop.security.KerberosUtil;
import com.huawei.hadoop.security.LoginUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class ColocationExample {
    private static final String TESTFILE_TXT = "/testfile.txt";

    private static final String COLOCATION_GROUP_GROUP01 = "gid01";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String PRINCIPAL = "username.client.kerberos.principal";

    private static final String KEYTAB = "username.client.keytab.file";

    private static final String PRNCIPAL_NAME = "hdfsDeveloper";

    private static final String LOGIN_CONTEXT_NAME = "Client";

    private static final String PATH_TO_KEYTAB = System.getProperty("user.dir") + File.separator + "conf"
            + File.separator + "user.keytab";

    private static final String PATH_TO_KRB5_CONF = System.getProperty("user.dir") + File.separator + "conf"
            + File.separator + "krb5.conf";

    private static String zookeeperDefaultServerPrincipal = null;

    private static Configuration conf = new Configuration();

    private static DFSColocationAdmin dfsAdmin;

    private static DFSColocationClient dfs;

    private static void init() throws IOException {
        /*
         * if need to connect zk, please provide jaas info about zk. of course,
         * you can do it as below:
         * System.setProperty("java.security.auth.login.config", confDirPath +
         * "jaas.conf"); Note: if this process will connect more than one zk
         * cluster, the demo may be not proper. you can contact us for more help
         */

        LoginUtil.login(PRNCIPAL_NAME, PATH_TO_KEYTAB, PATH_TO_KRB5_CONF, conf);
        LoginUtil.setJaasConf(LOGIN_CONTEXT_NAME, PRNCIPAL_NAME, PATH_TO_KEYTAB);

        zookeeperDefaultServerPrincipal = "zookeeper/hadoop." + KerberosUtil.getKrb5DomainRealm().toLowerCase();
        LoginUtil.setZookeeperServerPrincipal(ZOOKEEPER_SERVER_PRINCIPAL_KEY, zookeeperDefaultServerPrincipal);
    }

    /**
     * create and write file
     *
     * @throws IOException
     */
    private static void put() throws IOException {
        FSDataOutputStream out = dfs.create(new Path(TESTFILE_TXT), true, COLOCATION_GROUP_GROUP01, "lid01");
        // the data to be written to the hdfs.
        byte[] readBuf = "Hello World".getBytes("UTF-8");
        out.write(readBuf, 0, readBuf.length);
        out.close();
    }

    /**
     * delete file
     *
     * @throws IOException
     */
    @SuppressWarnings("deprecation")
    private static void delete() throws IOException {
        dfs.delete(new Path(TESTFILE_TXT));
    }

    /**
     * create group
     *
     * @throws IOException
     */
    private static void createGroup() throws IOException {
        dfsAdmin.createColocationGroup(COLOCATION_GROUP_GROUP01,
            Arrays.asList(new String[] {"lid01", "lid02", "lid03"}));
    }

    /**
     * delete group
     *
     * @throws IOException
     */
    private static void deleteGroup() throws IOException {
        dfsAdmin.deleteColocationGroup(COLOCATION_GROUP_GROUP01);
    }

    public static void main(String[] args) throws IOException {
        init();
        dfsAdmin = new DFSColocationAdmin(conf);
        dfs = new DFSColocationClient();
        dfs.initialize(URI.create(conf.get("fs.defaultFS")), conf);

        System.out.println("Create Group is running...");
        createGroup();
        System.out.println("Create Group has finished.");

        System.out.println("Put file is running...");
        put();
        System.out.println("Put file has finished.");

        System.out.println("Delete file is running...");
        delete();
        System.out.println("Delete file has finished.");

        System.out.println("Delete Group is running...");
        deleteGroup();
        System.out.println("Delete Group has finished.");

        dfs.close();
        dfsAdmin.close();
    }

}
