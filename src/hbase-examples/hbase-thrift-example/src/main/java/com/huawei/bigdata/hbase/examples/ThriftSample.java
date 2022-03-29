/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.thrift2.generated.TColumnFamilyDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TPut;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TTableDescriptor;
import org.apache.hadoop.hbase.thrift2.generated.TTableName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * Function description:
 *
 * HBase Development Instruction Sample Code The sample code uses user
 * information as source data,it introduces how to implement businesss process
 * development using HBase Thrift API
 *
 * @since 2013
 */

public class ThriftSample {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThriftSample.class.getName());

    private static final String TABLE_NAME = "thrift_table";

    private static final String COLUMN_FAMILY = "cf1";

    private TTransport transport;

    private THBaseService.Iface client;

    private Object lock = new Object();

    /**
     * HBaseThriftSample test
     */
    public void test(String host, int port, Configuration conf) throws TException, IOException {
        PrivilegedAction action = (PrivilegedAction<Object>) () -> {
            try {
                doTest(host, port, conf);
            } catch (TException | IOException e) {
                LOGGER.error("Run test failed.", e);
            }
            return null;
        };
        UserGroupInformation.getLoginUser().doAs(action);
    }

    private void doTest(String host, int port, Configuration conf) throws IOException, TException {
        try {
            client = getClient(host, port, conf);

            // Get table of specified namespace.
            getTableNamesByNamespace(client, "default");

            // Create table.
            createTable(client, TABLE_NAME);

            // Write data with put.
            putData(client, TABLE_NAME);

            // Write data with putlist.
            putDataList(client, TABLE_NAME);

            // Get data with single get.
            getData(client, TABLE_NAME);

            // Get data with getlist.
            getDataList(client, TABLE_NAME);

            // Scan data.
            scanData(client, TABLE_NAME);

            // Delete specified table.
            deleteTable(client, TABLE_NAME);
        } finally {
            close();
        }
    }

    private Map<String, String> transferProctection(Configuration conf) {
        String thriftQop = conf.get("hbase.thrift.security.qop");
        String rpcSting = "";
        for (SaslUtil.QualityOfProtection qop : SaslUtil.QualityOfProtection.values()) {
            if (thriftQop.equalsIgnoreCase(qop.getSaslQop())) {
                rpcSting += qop.name();
                break;
            }
        }
        return SaslUtil.initSaslProperties(rpcSting);
    }

    private THBaseService.Iface getClient(String host, int port, Configuration conf)
        throws SaslException, TTransportException {
        if (client == null) {
            synchronized (lock) {
                if (client == null) {
                    transport = new TSocket(host, port);
                    if (User.isHBaseSecurityEnabled(conf)) {
                        String[] names = SaslUtil.splitKerberosName(conf.get("hbase.thrift.kerberos.principal"));
                        SaslClient saslClient = Sasl.createSaslClient(
                            new String[] {AuthMethod.KERBEROS.getMechanismName()}, null, names[0], names[1],
                            transferProctection(conf), null);
                        transport = new TSaslClientTransport(saslClient, transport);
                    }
                    TProtocol protocol = new TBinaryProtocol(transport);
                    client = new THBaseService.Client(protocol);
                    transport.open();
                }
            }
        }
        return client;
    }

    private void getTableNamesByNamespace(THBaseService.Iface client, String namespace) throws TException {
        client.getTableNamesByNamespace(namespace)
            .forEach(
                tTableName -> LOGGER.info("{}", TableName.valueOf(tTableName.getNs(), tTableName.getQualifier())));
    }

    private void createTable(THBaseService.Iface client, String tableName) throws TException, IOException {
        TTableName table = getTableName(tableName);
        TTableDescriptor descriptor = new TTableDescriptor(table);
        descriptor.setColumns(
            Collections.singletonList(new TColumnFamilyDescriptor().setName(COLUMN_FAMILY.getBytes())));
        if (client.tableExists(table)) {
            LOGGER.warn("Table {} is exists, delete it.", tableName);
            client.disableTable(table);
            client.deleteTable(table);
        }
        client.createTable(descriptor, null);
        if (client.tableExists(table)) {
            LOGGER.info("Created {}.", tableName);
        } else {
            LOGGER.error("Create {} failed.", tableName);
        }
    }

    private void putData(THBaseService.Iface client, String tableName) throws TException {
        LOGGER.info("Test putData.");
        TPut put = new TPut();
        put.setRow("row1".getBytes());

        TColumnValue columnValue = new TColumnValue();
        columnValue.setFamily(COLUMN_FAMILY.getBytes());
        columnValue.setQualifier("q1".getBytes());
        columnValue.setValue("test value".getBytes());
        List<TColumnValue> columnValues = new ArrayList<>(1);
        columnValues.add(columnValue);
        put.setColumnValues(columnValues);

        ByteBuffer table = ByteBuffer.wrap(tableName.getBytes());
        client.put(table, put);
        LOGGER.info("Test putData done.");
    }

    private void putDataList(THBaseService.Iface client, String tableName) throws TException {
        LOGGER.info("Test putDataList.");
        TPut put1 = new TPut();
        put1.setRow("row2".getBytes());
        List<TPut> putList = new ArrayList<>();

        TColumnValue q1Value = new TColumnValue(ByteBuffer.wrap(COLUMN_FAMILY.getBytes()),
            ByteBuffer.wrap("q1".getBytes()), ByteBuffer.wrap("test value".getBytes()));
        TColumnValue q2Value = new TColumnValue(ByteBuffer.wrap(COLUMN_FAMILY.getBytes()),
            ByteBuffer.wrap("q2".getBytes()), ByteBuffer.wrap("test q2 value".getBytes()));
        List<TColumnValue> columnValues = new ArrayList<>(2);
        columnValues.add(q1Value);
        columnValues.add(q2Value);
        put1.setColumnValues(columnValues);
        putList.add(put1);

        TPut put2 = new TPut();
        put2.setRow("row3".getBytes());

        TColumnValue columnValue = new TColumnValue();
        columnValue.setFamily(COLUMN_FAMILY.getBytes());
        columnValue.setQualifier("q1".getBytes());
        columnValue.setValue("test q1 value".getBytes());
        put2.setColumnValues(Collections.singletonList(columnValue));
        putList.add(put2);

        ByteBuffer table = ByteBuffer.wrap(tableName.getBytes());
        client.putMultiple(table, putList);
        LOGGER.info("Test putDataList done.");
    }

    private void getData(THBaseService.Iface client, String tableName) throws TException {
        LOGGER.info("Test getData.");
        TGet get = new TGet();
        get.setRow("row1".getBytes());

        ByteBuffer table = ByteBuffer.wrap(tableName.getBytes());
        TResult result = client.get(table, get);
        printResult(result);
        LOGGER.info("Test getData done.");
    }

    private void getDataList(THBaseService.Iface client, String tableName) throws TException {
        LOGGER.info("Test getDataList.");
        List<TGet> getList = new ArrayList<>();
        TGet get1 = new TGet();
        get1.setRow("row1".getBytes());
        getList.add(get1);

        TGet get2 = new TGet();
        get2.setRow("row2".getBytes());
        getList.add(get2);

        ByteBuffer table = ByteBuffer.wrap(tableName.getBytes());
        List<TResult> resultList = client.getMultiple(table, getList);
        for (TResult tResult : resultList) {
            printResult(tResult);
        }
        LOGGER.info("Test getDataList done.");
    }

    private void scanData(THBaseService.Iface client, String tableName) throws TException {
        LOGGER.info("Test scanData.");
        int scannerId = -1;
        try {
            ByteBuffer table = ByteBuffer.wrap(tableName.getBytes());
            TScan scan = new TScan();
            scan.setLimit(500);
            scannerId = client.openScanner(table, scan);
            List<TResult> resultList = client.getScannerRows(scannerId, 100);
            while (resultList != null && !resultList.isEmpty()) {
                for (TResult tResult : resultList) {
                    printResult(tResult);
                }
                resultList = client.getScannerRows(scannerId, 100);
            }
        } finally {
            if (scannerId != -1) {
                client.closeScanner(scannerId);
                LOGGER.info("Closed scanner {}.", scannerId);
            }
        }
        LOGGER.info("Test scanData done.");
    }

    private void deleteTable(THBaseService.Iface client, String tableName) throws TException, IOException {
        TTableName table = getTableName(tableName);
        if (client.tableExists(table)) {
            client.disableTable(table);
            client.deleteTable(table);
            LOGGER.info("Deleted {}.", tableName);
        } else {
            LOGGER.warn("{} not exist.", tableName);
        }
    }

    private TTableName getTableName(String tableName) throws IOException {
        if (tableName == null || tableName.isEmpty()) {
            LOGGER.error("tableName is empty.");
            throw new IOException("tableName is empty.");
        }
        TTableName table = new TTableName();
        if (tableName.contains(":")) {
            String[] tableArray = tableName.split(":");
            table.setNs(tableArray[0].getBytes());
            table.setQualifier(tableArray[1].getBytes());
        } else {
            table.setQualifier(tableName.getBytes());
        }
        return table;
    }

    private void printResult(TResult result) {
        LOGGER.info("Row={}", result.getRow());
        for (TColumnValue resultColumnValue : result.getColumnValues()) {
            LOGGER.info("{}:{}={},timestamp:{}", resultColumnValue.getFamily(),
                resultColumnValue.getQualifier(),
                resultColumnValue.getValue(),
                resultColumnValue.getTimestamp());
        }
    }

    private synchronized void close() {
        if (transport != null && transport.isOpen()) {
            transport.close();
            transport = null;
        }
    }
}

