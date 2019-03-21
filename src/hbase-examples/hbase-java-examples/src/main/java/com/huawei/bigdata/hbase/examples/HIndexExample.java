package com.huawei.bigdata.hbase.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import org.apache.hadoop.hbase.hindex.client.HIndexAdmin;
import org.apache.hadoop.hbase.hindex.client.impl.HIndexClient;
import org.apache.hadoop.hbase.hindex.common.HIndexSpecification;
import org.apache.hadoop.hbase.hindex.common.HIndexSpecification.ValueType;
import org.apache.hadoop.hbase.hindex.common.TableIndices;
import org.apache.hadoop.hbase.hindex.server.manager.HIndexManager.IndexState;

public class HIndexExample {
  private final static Log LOG = LogFactory.getLog(HIndexExample.class.getName());
  private TableName tableName = null;
  private Connection conn = null;
  public String TABLE_NAME = "hbase_hindex_table";
  public String indexNameToAdd = "idx_0";
  private static Configuration conf = null;

  public HIndexExample(Configuration conf) throws IOException {
    this.tableName = TableName.valueOf(TABLE_NAME);
    this.conn = ConnectionFactory.createConnection(conf);
  }

  public void test() throws Exception {
    testCreateTable();
    addIndicesExample();
    disableIndicesExample();
    enableIndicesExample();
    listIndicesIntable();
    dropIndicesExample();
    testPut();
    addIndicesExampleWithData();
    scanDataByHIndex();
    dropIndicesExampleWithData();
    dropTable();
  }

  /**
   * Create user table for HIndex
   */
  public void testCreateTable() {
    LOG.info("Entering CreateTable for HIndex.");

    // Specify the table descriptor.
    HTableDescriptor htd = new HTableDescriptor(tableName);

    // Set the column family name to info.
    HColumnDescriptor hcd = new HColumnDescriptor("info");

    // Set data encoding methods. HBase provides DIFF,FAST_DIFF,PREFIX
    // and PREFIX_TREE
    hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

    // Set compression methods, HBase provides two default compression
    // methods:GZ and SNAPPY
    // GZ has the highest compression rate,but low compression and
    // decompression effeciency,fit for cold data
    // SNAPPY has low compression rate, but high compression and
    // decompression effeciency,fit for hot data.
    // it is advised to use SANPPY
    hcd.setCompressionType(Compression.Algorithm.SNAPPY);

    htd.addFamily(hcd);

    Admin admin = null;
    try {
      // Instantiate an Admin object.
      admin = conn.getAdmin();
      if (!admin.tableExists(tableName)) {
        LOG.info("Creating table...");
        admin.createTable(htd);
        LOG.info(admin.getClusterStatus());
        LOG.info(admin.listNamespaceDescriptors());
        LOG.info("Table created successfully.");
      } else {
        LOG.warn("table already exists");
      }
    } catch (IOException e) {
      LOG.error("Create table failed.", e);
    } finally {
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin ", e);
        }
      }
    }
    LOG.info("Exiting testCreateTable.");

  }

  /**
   * Adding a Hindex
   */
  public void addIndicesExample() {
    LOG.info("Entering Adding a Hindex.");
    // Create index instance
    TableIndices tableIndices = new TableIndices();
    HIndexSpecification spec = new HIndexSpecification(indexNameToAdd);
    spec.addIndexColumn(new HColumnDescriptor("info"), "name", ValueType.STRING);
    tableIndices.addIndex(spec);
    Admin admin = null;
    HIndexAdmin iAdmin = null;
    try {
      admin = conn.getAdmin();
      iAdmin = HIndexClient.newHIndexAdmin(admin);
      // add index to the table
      iAdmin.addIndices(tableName, tableIndices);
      // Alternately, add the specified indices with data
      // iAdmin.addIndicesWithData(tableName, tableIndices);
      LOG.info("Successfully added indices to the table " + tableName);
    } catch (IOException e) {
      LOG.error("Add Indices failed for table " + tableName + "." + e);
    } finally {
      if (iAdmin != null) {
        try {
          // Close the HIndexAdmin object.
          iAdmin.close();
        } catch (IOException e) {
          LOG.error("Failed to close HIndexAdmin ", e);
        }
      }
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin ", e);
        }
      }
    }
    LOG.info("Exiting Adding a Hindex.");
  }

  /**
   * HIndex-based Query
   */
  public void scanDataByHIndex() {
    LOG.info("Entering HIndex-based Query.");
    Table table = null;
    ResultScanner rScanner = null;
    try {
      table = conn.getTable(tableName);
      // Create a filter for indexed column.
      SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
          CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("26"));
      filter.setFilterIfMissing(true);

      Scan scan = new Scan();
      scan.setFilter(filter);
      rScanner = table.getScanner(scan);

      // Scan the data
      LOG.info("Scan data using indices..");
      for (Result result : rScanner) {
        LOG.info("Scanned row is:");
        for (Cell cell : result.rawCells()) {
          LOG.info(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell)) + ","
              + Bytes.toString(CellUtil.cloneQualifier(cell)) + "," + Bytes.toString(CellUtil.cloneValue(cell)));
        }
      }
      LOG.info("Successfully scanned data using indices for table " + tableName + ".");
    } catch (IOException e) {
      LOG.error("Failed to scan data using indices for table " + tableName + "." + e);
    } finally {
      if (rScanner != null) {
        rScanner.close();
      }
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          LOG.error("failed to close table, ", e);
        }
      }
    }
    LOG.info("Entering HIndex-based Query.");
  }

  /**
   * Disabling a Hindex
   */
  public void disableIndicesExample() {
    LOG.info("Entering Disabling a Hindex.");
    List<String> indexNameList = new ArrayList<String>();
    indexNameList.add(indexNameToAdd);
    Admin admin = null;
    HIndexAdmin iAdmin = null;
    try {
      admin = conn.getAdmin();
      iAdmin = HIndexClient.newHIndexAdmin(admin);
      // Disable the specified indices
      iAdmin.disableIndices(tableName, indexNameList);
      // Alternately, enable the specified indices
      // iAdmin.enableIndices(tableName, indexNameList);
      LOG.info("Successfully disabled indices " + indexNameList + " of the table " + tableName);
    } catch (IOException e) {
      LOG.error("Failed to disable indices " + indexNameList + " of the table " + tableName);
    } finally {
      if (iAdmin != null) {
        try {
          // Close the HIndexAdmin object.
          iAdmin.close();
        } catch (IOException e) {
          LOG.error("Failed to close HIndexAdmin ", e);
        }
      }
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin ", e);
        }
      }
    }
    LOG.info("Exiting Disabling a Hindex.");
  }

  /**
   * Enabling a Hindex
   */
  public void enableIndicesExample() {
    LOG.info("Entering Enabling a Hindex.");
    List<String> indexNameList = new ArrayList<String>();
    indexNameList.add(indexNameToAdd);
    Admin admin = null;
    HIndexAdmin iAdmin = null;
    try {
      admin = conn.getAdmin();
      iAdmin = HIndexClient.newHIndexAdmin(admin);
      // Disable the specified indices
      iAdmin.enableIndices(tableName, indexNameList);
      // Alternately, enable the specified indices
      // iAdmin.enableIndices(tableName, indexNameList);
      LOG.info("Successfully enable indices " + indexNameList + " of the table " + tableName);
    } catch (IOException e) {
      LOG.error("Failed to enable indices " + indexNameList + " of the table " + tableName);
    } finally {
      if (iAdmin != null) {
        try {
          // Close the HIndexAdmin object.
          iAdmin.close();
        } catch (IOException e) {
          LOG.error("Failed to close HIndexAdmin ", e);
        }
      }
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin ", e);
        }
      }
    }
    LOG.info("Exiting Enabling a Hindex.");
  }

  /**
   * Listing Hindex
   */
  public void listIndicesIntable() {
    LOG.info("Entering Listing Hindex.");
    Admin admin = null;
    HIndexAdmin iAdmin = null;
    try {
      admin = conn.getAdmin();
      iAdmin = HIndexClient.newHIndexAdmin(admin);
      // Retreive the list of indices and print it
      List<Pair<HIndexSpecification, IndexState>> indicesList = iAdmin.listIndices(tableName);
      LOG.info("indicesList:" + indicesList);
      LOG.info("Successfully listed indices for table " + tableName + ".");
    } catch (IOException e) {
      LOG.error("Failed to list indices for table " + tableName + "." + e);
    } finally {
      if (iAdmin != null) {
        try {
          // Close the HIndexAdmin object.
          iAdmin.close();
        } catch (IOException e) {
          LOG.error("Failed to close HIndexAdmin ", e);
        }
      }
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin ", e);
        }
      }
    }
    LOG.info("Exiting Listing Hindex.");
  }

  /**
   * Deleting a Hindex
   */
  public void dropIndicesExample() {
    LOG.info("Entering Deleting a Hindex.");
    List<String> indexNameList = new ArrayList<String>();
    indexNameList.add(indexNameToAdd);
    Admin admin = null;
    HIndexAdmin iAdmin = null;
    try {
      admin = conn.getAdmin();
      iAdmin = HIndexClient.newHIndexAdmin(admin);
      // Drop the specified indices without dropping index data
      iAdmin.dropIndices(tableName, indexNameList);
      // Alternately, drop the specified indices with data
      // iAdmin.dropIndicesWithData(tableName, indexNameList);
      LOG.info("Successfully dropped indices " + indexNameList + " from the table " + tableName);
    } catch (IOException e) {
      LOG.error("Failed to drop indices " + indexNameList + " from the table " + tableName);
    } finally {
      if (iAdmin != null) {
        try {
          // Close the HIndexAdmin object.
          iAdmin.close();
        } catch (IOException e) {
          LOG.error("Failed to close HIndexAdmin ", e);
        }
      }
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin ", e);
        }
      }
    }
    LOG.info("Exiting Deleting a Hindex.");
  }

  /**
   * Insert data
   */
  public void testPut() {
    LOG.info("Entering testPut.");

    // Specify the column family name.
    byte[] familyName = Bytes.toBytes("info");
    // Specify the column name.
    byte[][] qualifiers = { Bytes.toBytes("name"), Bytes.toBytes("gender"), Bytes.toBytes("age"),
        Bytes.toBytes("address") };

    Table table = null;
    try {
      // Instantiate an HTable object.
      table = conn.getTable(tableName);
      List<Put> puts = new ArrayList<Put>();
      // Instantiate a Put object.
      Put put = new Put(Bytes.toBytes("012005000201"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhang San"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("19"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shenzhen, Guangdong"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000202"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Li Wanting"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("23"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shijiazhuang, Hebei"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000203"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Wang Ming"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("26"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Ningbo, Zhejiang"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000204"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Li Gang"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("18"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Xiangyang, Hubei"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000205"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhao Enru"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("21"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Shangrao, Jiangxi"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000206"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Chen Long"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("32"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Zhuzhou, Hunan"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000207"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Zhou Wei"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("29"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Nanyang, Henan"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000208"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Yang Yiwen"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Female"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("30"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Kaixian, Chongqing"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000209"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Xu Bing"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("26"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Weinan, Shaanxi"));
      puts.add(put);

      put = new Put(Bytes.toBytes("012005000210"));
      put.addColumn(familyName, qualifiers[0], Bytes.toBytes("Xiao Kai"));
      put.addColumn(familyName, qualifiers[1], Bytes.toBytes("Male"));
      put.addColumn(familyName, qualifiers[2], Bytes.toBytes("25"));
      put.addColumn(familyName, qualifiers[3], Bytes.toBytes("Dalian, Liaoning"));
      puts.add(put);

      // Submit a put request.
      table.put(puts);

      LOG.info("Put successfully.");
    } catch (IOException e) {
      LOG.error("Put failed ", e);
    } finally {
      if (table != null) {
        try {
          // Close the HTable object.
          table.close();
        } catch (IOException e) {
          LOG.error("Close table failed ", e);
        }
      }
    }
    LOG.info("Exiting testPut.");
  }

  /**
   * Adding a Hindex With Data
   */
  public void addIndicesExampleWithData() {
    LOG.info("Entering Adding a Hindex With Data.");
    // Create index instance
    TableIndices tableIndices = new TableIndices();
    HIndexSpecification spec = new HIndexSpecification(indexNameToAdd);
    spec.addIndexColumn(new HColumnDescriptor("info"), "age", ValueType.STRING);
    tableIndices.addIndex(spec);
    Admin admin = null;
    HIndexAdmin iAdmin = null;
    try {
      admin = conn.getAdmin();
      iAdmin = HIndexClient.newHIndexAdmin(admin);
      // add index to the table
      iAdmin.addIndicesWithData(tableName, tableIndices);
      // Alternately, add the specified indices with data
      // iAdmin.addIndicesWithData(tableName, tableIndices);
      LOG.info("Successfully added indices to the table " + tableName);
    } catch (IOException e) {
      LOG.error("Add Indices failed for table " + tableName + "." + e);
    } finally {
      if (iAdmin != null) {
        try {
          // Close the HIndexAdmin object.
          iAdmin.close();
        } catch (IOException e) {
          LOG.error("Failed to close HIndexAdmin ", e);
        }
      }
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin ", e);
        }
      }
    }
    LOG.info("Exiting Adding a Hindex With Data.");
  }

  /**
   * Deleting a Hindex With Data
   */
  public void dropIndicesExampleWithData() {
    LOG.info("Entering Deleting a Hindex With Data.");
    List<String> indexNameList = new ArrayList<String>();
    indexNameList.add(indexNameToAdd);
    Admin admin = null;
    HIndexAdmin iAdmin = null;
    try {
      admin = conn.getAdmin();
      iAdmin = HIndexClient.newHIndexAdmin(admin);
      // Drop the specified indices without dropping index data
      iAdmin.dropIndicesWithData(tableName, indexNameList);
      // Alternately, drop the specified indices with data
      // iAdmin.dropIndicesWithData(tableName, indexNameList);
      LOG.info("Successfully dropped indices " + indexNameList + " from the table " + tableName);
    } catch (IOException e) {
      LOG.error("Failed to drop indices " + indexNameList + " from the table " + tableName);
    } finally {
      if (iAdmin != null) {
        try {
          // Close the HIndexAdmin object.
          iAdmin.close();
        } catch (IOException e) {
          LOG.error("Failed to close HIndexAdmin ", e);
        }
      }
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Failed to close admin ", e);
        }
      }
    }
    LOG.info("Exiting Deleting a Hindex With Data.");
  }

  public void dropTable() {
    LOG.info("Entering dropTable.");

    Admin admin = null;
    try {
      admin = conn.getAdmin();
      if (admin.tableExists(tableName)) {
        // Disable the table before deleting it.
        admin.disableTable(tableName);

        // Delete table.
        admin.deleteTable(tableName);
      }
      LOG.info("Drop table successfully.");
    } catch (IOException e) {
      LOG.error("Drop table failed ", e);
    } finally {
      if (admin != null) {
        try {
          // Close the Admin object.
          admin.close();
        } catch (IOException e) {
          LOG.error("Close admin failed ", e);
        }
      }
    }
    LOG.info("Exiting dropTable.");
  }
}
