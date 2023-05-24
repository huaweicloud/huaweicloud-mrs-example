package com.huawei.bigdata.hdfs.examples;

import com.huawei.hadoop.oi.colocation.DFSColocationAdmin;
import com.huawei.hadoop.oi.colocation.DFSColocationClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

public class ColocationExample {
    private static final String TESTFILE_TXT = "/testfile.txt";

    private static final String COLOCATION_GROUP_GROUP01 = "gid01";

    private final static String USER = "hdfs";

    private static Configuration conf = new Configuration();

    private static DFSColocationAdmin dfsAdmin;

    private static DFSColocationClient dfs;

    private static void init() throws IOException {
        // set user, if user dont set HADOOP_USER_NAME, then use USER.
        if (System.getenv("HADOOP_USER_NAME") == null && System.getProperty("HADOOP_USER_NAME") == null) {
            System.setProperty("HADOOP_USER_NAME", USER);
        }
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
    private static void delete() throws IOException {
        dfs.delete(new Path(TESTFILE_TXT), true);
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
