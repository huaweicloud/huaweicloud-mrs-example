/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import com.huawei.jredis.client.SslSocketFactoryUtil;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import org.apache.flink.api.java.utils.ParameterTool;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.SSLSocketFactory;

/**
 * Read data from csv file and import to redis.
 * 
 * @since 2019/9/30
 */
public class RedisDataImport {

    private static final int MAX_ATTEMPTS = 2;

    private static final int TIMEOUT = 15000;

    public static void main(String[] args) throws Exception {
        // print comment for command to use run flink
        System.out.println(
                "use command as: \n"
                    + "java -cp /opt/FI-Client/Flink/flink/lib/*:/opt/FlinkConfigtableJavaExample.jar"
                    + " com.huawei.bigdata.flink.examples.RedisDataImport --configPath <config"
                    + " filePath>******************************************************************************************\n"
                    + "<config filePath> is for configure file to load\n"
                    + "you may write following content into config filePath: \n"
                    + "CsvPath=config/configtable.csv\n"
                    + "CsvHeaderExist=true\n"
                    + "ColumnNames=username,age,company,workLocation,educational,workYear,phone,nativeLocation,school\n"
                    + "Redis_IP_Port=SZV1000064084:22400,SZV1000064082:22400,SZV1000064085:22400\n"
                    + "******************************************************************************************");

        // read all configures
        final String configureFilePath = ParameterTool.fromArgs(args).get("configPath", "config/import.properties");
        final String csvFilePath =
                ParameterTool.fromPropertiesFile(configureFilePath).get("CsvPath", "config/configtable.csv");
        final boolean isHasHeaders =
                ParameterTool.fromPropertiesFile(configureFilePath).getBoolean("CsvHeaderExist", true);
        final String csvScheme = ParameterTool.fromPropertiesFile(configureFilePath).get("ColumnNames");
        final String redisIPPort = ParameterTool.fromPropertiesFile(configureFilePath).get("Redis_IP_Port");
        final boolean ssl = ParameterTool.fromPropertiesFile(configureFilePath).getBoolean("Redis_ssl_on", false);

        // init redis client
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        for (String node : redisIPPort.split(",")) {
            HostAndPort hostAndPort = genHostAndPort(node);
            if (hostAndPort == null) {
                continue;
            }
            hosts.add(hostAndPort);
        }
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        final SSLSocketFactory socketFactory = SslSocketFactoryUtil.createTrustALLSslSocketFactory();

        final JedisCluster client = new JedisCluster(hosts, TIMEOUT, TIMEOUT, MAX_ATTEMPTS,
            "","", poolConfig, ssl, socketFactory, null,
            null, null);


        // get all files under csv file path
        ArrayList<File> files = getListFiles(csvFilePath);
        System.out.println(
                "Read file or directory under  "
                        + csvFilePath
                        + ", total file num: "
                        + files.size()
                        + ", columns: "
                        + csvScheme);

        // run read csv file and analyze it
        for (int index = 0; index < files.size(); index++) {
            readWithCsvBeanReader(files.get(index).getAbsolutePath(), csvScheme, isHasHeaders, client);
        }
        client.close();
        System.out.println("Data import finish!!!");
    }

    private static HostAndPort genHostAndPort(String ipAndPort) {
        int lastIdx = ipAndPort.lastIndexOf(":");
        if (lastIdx == -1) {
            return null;
        }
        String ip = ipAndPort.substring(0, lastIdx);
        String port = ipAndPort.substring(lastIdx + 1);
        return new HostAndPort(ip, Integer.parseInt(port));
    }

    /**
     * 根据文件对象获取文件列表集合
     * 
     * @param obj 文件对象
     * @return 文件列表集合
     */
    public static ArrayList<File> getListFiles(Object obj) {
        File directory = null;
        if (obj instanceof File) {
            directory = (File) obj;
        } else {
            directory = new File(obj.toString());
        }
        ArrayList<File> files = new ArrayList<File>();
        if (directory.isFile()) {
            files.add(directory);
            return files;
        } else if (directory.isDirectory()) {
            File[] fileArr = directory.listFiles();
            for (int i = 0; i < fileArr.length; i++) {
                File fileOne = fileArr[i];
                files.addAll(getListFiles(fileOne));
            }
        }
        return files;
    }

    /**
     * Sets up the processors used for read csv. There are 9 CSV columns. Empty
     * columns are read as null (hence the NotNull() for mandatory columns).
     *
     * @return the cell processors
     */
    private static CellProcessor[] getProcessors() {
        final CellProcessor[] processors =
                new CellProcessor[] {
                    new NotNull(), // username
                    new NotNull(), // age
                    new NotNull(), // company
                    new NotNull(), // workLocation
                    new NotNull(), // educational
                    new NotNull(), // workYear
                    new NotNull(), // phone
                    new NotNull(), // nativeLocation
                    new NotNull(), // school
                };

        return processors;
    }

    private static void readWithCsvBeanReader(String path, String csvScheme, boolean isSkipHeader, JedisCluster client)
            throws Exception {
        ICsvBeanReader beanReader = null;
        try {
            beanReader = new CsvBeanReader(new FileReader(path), CsvPreference.STANDARD_PREFERENCE);

            // the header elements are used to map the values to the bean (names must match)
            final String[] header = isSkipHeader ? beanReader.getHeader(true) : csvScheme.split(",");
            final CellProcessor[] processors = getProcessors();

            UserInfo userinfo;
            while ((userinfo = beanReader.read(UserInfo.class, header, processors)) != null) {
                System.out.println(
                        String.format(Locale.ROOT, 
                                "lineNo=%s, rowNo=%s, userinfo=%s",
                                beanReader.getLineNumber(), beanReader.getRowNumber(), userinfo));

                // set redis key and value
                client.hmset(userinfo.getUsername(), userinfo.getMapInfo());
            }
        } finally {
            if (beanReader != null) {
                beanReader.close();
            }
        }
    }

    /**
     *  用户信息类
     *
     * @since 2019/9/30
     */
    public static class UserInfo {
        private String username;
        private String age;
        private String company;
        private String workLocation;
        private String educational;
        private String workYear;
        private String phone;
        private String nativeLocation;
        private String school;

        public UserInfo() {}

        public UserInfo(String username, String age, String company, String workLocation, String educational, String workYear, String phone, String nativeLocation, String school) {
            this.username = username;
            this.age = age;
            this.company = company;
            this.workLocation = workLocation;
            this.educational = educational;
            this.workYear = workYear;
            this.phone = phone;
            this.nativeLocation = nativeLocation;
            this.school = school;
        }

        @Override
        public String toString() {
            return "UserInfo-----[username: "
                + username
                + "  age: "
                + age
                + "  company: "
                + company
                + "  workLocation: "
                + workLocation
                + "  educational: "
                + educational
                + "  workYear: "
                + workYear
                + "  phone: "
                + phone
                + "  nativeLocation: "
                + nativeLocation
                + "  school: "
                + school
                + "]";
        }

        /**
         * @return 用户信息map
         */
        public Map<String, String> getMapInfo() {
            Map<String, String> info = new HashMap<String, String>();
            info.put("username", username);
            info.put("age", age);
            info.put("company", company);
            info.put("workLocation", workLocation);
            info.put("educational", educational);
            info.put("workYear", workYear);
            info.put("phone", phone);
            info.put("nativeLocation", nativeLocation);
            info.put("school", school);
            return info;
        }

        /**
         * @return the username
         */
        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        /**
         * @return the age
         */
        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        /**
         * @return the company
         */
        public String getCompany() {
            return company;
        }

        public void setCompany(String company) {
            this.company = company;
        }

        /**
         * @return the workLocation
         */
        public String getWorkLocation() {
            return workLocation;
        }

        public void setWorkLocation(String workLocation) {
            this.workLocation = workLocation;
        }

        /**
         * @return the educational
         */
        public String getEducational() {
            return educational;
        }

        public void setEducational(String educational) {
            this.educational = educational;
        }

        /**
         * @return the workYear
         */
        public String getWorkYear() {
            return workYear;
        }

        public void setWorkYear(String workYear) {
            this.workYear = workYear;
        }

        /**
         * @return the phone
         */
        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
        }

        /**
         * @return the nativeLocation
         */
        public String getNativeLocation() {
            return nativeLocation;
        }

        public void setNativeLocation(String nativeLocation) {
            this.nativeLocation = nativeLocation;
        }

        /**
         * @return the school
         */
        public String getSchool() {
            return school;
        }

        public void setSchool(String school) {
            this.school = school;
        }
    }
}
