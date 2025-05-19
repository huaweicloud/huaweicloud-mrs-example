package com.huawei.spring.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.annotation.PostConstruct;


@Repository
public class CustomerHDFSTemplate {

    protected static final Logger logger = LoggerFactory.getLogger(CustomerHDFSTemplate.class.getName());

    @Value("${spring.hdfs.config.dir}")
    private String configDir;

    @Value("${hdfs.user}")
    private String user;

    private FileSystem fSystem = null;

    @PostConstruct
    private void init() throws IOException {
        Configuration conf = new Configuration();
        // conf file
        conf.addResource(new Path(configDir + "/hdfs-site.xml"));
        conf.addResource(new Path(configDir + "/core-site.xml"));
        if ("kerberos".equalsIgnoreCase(conf.get("hadoop.security.authentication"))) {
            String krb5Conf = configDir + File.separator + "krb5.conf";
            String keytab = configDir + File.separator + "user.keytab";
            System.setProperty("java.security.krb5.conf", krb5Conf);
            LoginUtil.login(user, keytab, krb5Conf, conf);
        }
        fSystem = FileSystem.get(conf);
    }

    public void mkdir(String path) throws IOException {
        Path filePath = new Path(path);
        if (!fSystem.exists(filePath)) {
            fSystem.mkdirs(filePath);
            logger.info("mkdir path:{} success", path);
            return;
        }
        logger.info("path :{} exists", path);
    }

    public void remove(String path) throws IOException {
        Path filePath = new Path(path);
        if (fSystem.exists(filePath)) {
            fSystem.delete(filePath, true);
            logger.info("remove path:{} success", path);
            return;
        }
        logger.info("path :{} not exists", path);
    }

    public void write(String destPath, String fileName) throws IOException {
        final String content = "hi, I am bigdata. It is successful if you can see me.";
        FSDataOutputStream out = null;
        try {
            out = fSystem.create(new Path(destPath + File.separator + fileName));
            out.write(content.getBytes());
            out.hsync();
            logger.info("success to write.");
        } finally {
            // make sure the stream is closed finally.
            IOUtils.closeStream(out);
        }
    }

    public String read(String destPath, String fileName) throws IOException {
        String strPath = destPath + File.separator + fileName;
        Path path = new Path(strPath);
        FSDataInputStream in = null;
        BufferedReader reader = null;
        StringBuilder strBuffer = new StringBuilder();

        try {
            in = fSystem.open(path);
            reader = new BufferedReader(new InputStreamReader(in));
            String sTempOneLine;

            // write file
            while ((sTempOneLine = reader.readLine()) != null) {
                strBuffer.append(sTempOneLine);
            }
            logger.info("result is : " + strBuffer);
            logger.info("success to read.");
        } finally {
            // make sure the streams are closed finally.
            IOUtils.closeStream(reader);
            IOUtils.closeStream(in);
        }
        return strBuffer.toString();
    }

}
