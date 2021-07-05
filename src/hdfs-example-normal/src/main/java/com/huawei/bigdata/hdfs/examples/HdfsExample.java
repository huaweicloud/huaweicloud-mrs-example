package com.huawei.bigdata.hdfs.examples;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockStoragePolicySpi;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class HdfsExample {
    private final static Log LOG = LogFactory.getLog(HdfsExample.class.getName());

    private static final String STORAGE_POLICY_HOT = "HOT";

    private final static String PATH_TO_HDFS_SITE_XML = HdfsExample.class.getClassLoader()
        .getResource("hdfs-site.xml")
        .getPath();

    private final static String PATH_TO_CORE_SITE_XML = HdfsExample.class.getClassLoader()
        .getResource("core-site.xml")
        .getPath();

    private static Configuration conf = null;

    // private static String PATH_TO_SMALL_SITE_XML =
    // HdfsMain.class.getClassLoader().getResource("smallfs-site.xml").getPath();

    private FileSystem fSystem; /* HDFS file system */

    private String destPath;

    private String fileName;

    public HdfsExample(String path, String fileName) throws IOException {
        this.destPath = path;
        this.fileName = fileName;
        instanceBuild();
    }

    public static void main(String[] args) throws Exception {
        // 完成初始化和认证
        confLoad();

        // 业务示例1：一个普通用例
        HdfsExample hdfsExample = new HdfsExample("/user/hdfsExample", "test.txt");
        hdfsExample.test();

        // 业务示例2：多线程
        final int threadCount = 2;
        for (int threadNum = 0; threadNum < threadCount; threadNum++) {
            HdfsExampleThread exampleThread = new HdfsExampleThread("hdfs_example_" + threadNum);
            exampleThread.start();
        }

        // 业务示例3： 设置存储策略
        // System.out.println("begin to set Storage Policy");
        // hdfsExample.setStoragePolicy(STORAGE_POLICY_HOT);
        // System.out.println("set Storage Policy end");
    }

    /**
     * Add configuration file if the application run on the linux ,then need
     * make the path of the core-site.xml and hdfs-site.xml to in the linux
     * client file
     */
    private static void confLoad() throws IOException {
        conf = new Configuration();
        // conf file
        conf.addResource(new Path(PATH_TO_HDFS_SITE_XML));
        conf.addResource(new Path(PATH_TO_CORE_SITE_XML));
        // conf.addResource(new Path(PATH_TO_SMALL_SITE_XML));
    }

    /**
     * HDFS operator instance
     *
     * @throws IOException
     */
    public void test() throws IOException {
        // create directory
        mkdir();

        // write file
        write();

        // wait for writing complete
        try {
            Thread.sleep(2 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // append file
        append();

        // read file
        read();

        // delete file
        delete();

        // delete directory
        rmdir();
    }

    /**
     * build HDFS instance
     */
    private void instanceBuild() throws IOException {
        // get filesystem
        // 一般情况下，FileSystem对象JVM里唯一，是线程安全的，这个实例可以一直用，不需要立马close。
        // 注意：
        // 若需要长期占用一个FileSystem对象的场景，可以给这个线程专门new一个FileSystem对象，但要注意资源管理，别导致泄露。
        // 在此之前，需要先给conf加上：
        // conf.setBoolean("fs.hdfs.impl.disable.cache",
        // true);//表示重新new一个连接实例，不用缓存中的对象。
        fSystem = FileSystem.get(conf);
    }

    /**
     * delete directory
     *
     * @throws java.io.IOException
     */
    private void rmdir() throws IOException {
        Path destPath = new Path(this.destPath);
        if (!deletePath(destPath)) {
            LOG.error("failed to delete destPath " + this.destPath);
            return;
        }

        LOG.info("success to delete path " + this.destPath);

    }

    /**
     * create directory
     *
     * @throws java.io.IOException
     */
    private void mkdir() throws IOException {
        Path destPath = new Path(this.destPath);
        if (!createPath(destPath)) {
            LOG.error("failed to create destPath " + this.destPath);
            return;
        }

        LOG.info("success to create path " + this.destPath);
    }

    /**
     * set storage policy to path
     *
     * @param policyName Policy Name can be accepted:
     * <li>HOT
     * <li>WARN
     * <li>COLD
     * <li>LAZY_PERSIST
     * <li>ALL_SSD
     * <li>ONE_SSD
     * @throws java.io.IOException
     */
    private void setStoragePolicy(String policyName) throws IOException {
        if (fSystem instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fSystem;
            Path destPath = new Path(this.destPath);
            Boolean flag = false;

            mkdir();
            BlockStoragePolicySpi[] storage = dfs.getStoragePolicies();
            for (BlockStoragePolicySpi bs : storage) {
                if (bs.getName().equals(policyName)) {
                    flag = true;
                }
                LOG.info("StoragePolicy:" + bs.getName());
            }
            if (!flag) {
                policyName = storage[0].getName();
            }
            dfs.setStoragePolicy(destPath, policyName);
            LOG.info("success to set Storage Policy path " + this.destPath);
            rmdir();
        } else {
            LOG.info("SmallFile not support to set Storage Policy !!!");
        }
    }

    /**
     * create file,write file
     *
     * @throws java.io.IOException
     * @throws com.huawei.bigdata.hdfs.examples.ParameterException
     */
    private void write() throws IOException {
        final String content = "hi, I am bigdata. It is successful if you can see me.";
        FSDataOutputStream out = null;
        try {
            out = fSystem.create(new Path(destPath + File.separator + fileName));
            out.write(content.getBytes());
            out.hsync();
            LOG.info("success to write.");
        } finally {
            // make sure the stream is closed finally.
            if (out != null) {
                out.close();
            }
        }
    }

    /**
     * append file content
     *
     * @throws java.io.IOException
     */
    private void append() throws IOException {
        final String content = "I append this content.";
        FSDataOutputStream out = null;
        try {
            out = fSystem.append(new Path(destPath + File.separator + fileName));
            out.write(content.getBytes());
            out.hsync();
            LOG.info("success to append.");
        } finally {
            // make sure the stream is closed finally.
            if (out != null) {
                out.close();
            }
        }
    }

    /**
     * read file
     *
     * @throws java.io.IOException
     */
    private void read() throws IOException {
        String strPath = destPath + File.separator + fileName;
        Path path = new Path(strPath);
        FSDataInputStream in = null;
        BufferedReader reader = null;
        StringBuffer strBuffer = new StringBuffer();

        try {
            in = fSystem.open(path);
            reader = new BufferedReader(new InputStreamReader(in));
            String sTempOneLine;

            // write file
            while ((sTempOneLine = reader.readLine()) != null) {
                strBuffer.append(sTempOneLine);
            }

            LOG.info("result is : " + strBuffer.toString());
            LOG.info("success to read.");

        } finally {
            // make sure the streams are closed finally.
            close(reader);
            close(in);
        }
    }

    /**
     * delete file
     *
     * @throws java.io.IOException
     */
    private void delete() throws IOException {
        Path beDeletedPath = new Path(destPath + File.separator + fileName);
        if (fSystem.delete(beDeletedPath, true)) {
            LOG.info("success to delete the file " + destPath + File.separator + fileName);
        } else {
            LOG.warn("failed to delete the file " + destPath + File.separator + fileName);
        }
    }

    /**
     * close stream
     *
     * @param stream
     * @throws java.io.IOException
     */
    private void close(Closeable stream) throws IOException {
        if (stream != null) {
            stream.close();
        }
    }

    /**
     * create file path
     *
     * @param filePath
     * @return
     * @throws java.io.IOException
     */
    private boolean createPath(final Path filePath) throws IOException {
        if (!fSystem.exists(filePath)) {
            fSystem.mkdirs(filePath);
        }
        return true;
    }

    /**
     * delete file path
     *
     * @param filePath
     * @return
     * @throws java.io.IOException
     */
    private boolean deletePath(final Path filePath) throws IOException {
        if (!fSystem.exists(filePath)) {
            return false;
        }
        // fSystem.delete(filePath, true);
        return fSystem.delete(filePath, true);
    }
}

class HdfsExampleThread extends Thread {
    private final static Log LOG = LogFactory.getLog(HdfsExampleThread.class.getName());

    /**
     * @param threadName
     */
    public HdfsExampleThread(String threadName) {
        super(threadName);
    }

    public void run() {
        HdfsExample example;
        try {
            example = new HdfsExample("/user/hdfs-examples/" + getName(), "test.txt");
            example.test();
        } catch (IOException e) {
            LOG.error(e);
        }
    }
}
