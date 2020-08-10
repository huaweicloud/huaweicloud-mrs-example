package com.huawei.bigdata.hbase.examples;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.hindex.client.HIndexAdmin;
import org.apache.hadoop.hbase.hindex.client.impl.HIndexClient;
import org.apache.hadoop.hbase.hindex.common.HIndexSpecification;
import org.apache.hadoop.hbase.hindex.common.HIndexSpecification.ValueType;
import org.apache.hadoop.hbase.hindex.common.TableIndices;
import org.apache.hadoop.hbase.hindex.server.manager.HIndexManager.IndexState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HIndexExample extends HBaseExample {
  private final static Log LOG = LogFactory.getLog(HIndexExample.class.getName());
  public static final String INDEX_TABLE_NAME = "hbase_hindex_table";
  public String indexNameToAdd = "idx_0";

  public HIndexExample(Configuration conf) throws IOException {
    super(conf, INDEX_TABLE_NAME);
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
  @Override
  public void testCreateTable() {
    LOG.info("Entering CreateTable for HIndex: " + INDEX_TABLE_NAME);
    super.testCreateTable();
  }

  /**
   * Adding a Hindex
   */
  public void addIndicesExample() {
    LOG.info("Entering Adding a Hindex.");
    // Create index instance
    TableIndices tableIndices = new TableIndices();
    HIndexSpecification spec = new HIndexSpecification(indexNameToAdd);
    spec.addIndexColumn(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build(),
            "name", ValueType.STRING, null);
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
          CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes("26"));
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
   * Adding a Hindex With Data
   */
  public void addIndicesExampleWithData() {
    LOG.info("Entering Adding a Hindex With Data.");
    // Create index instance
    TableIndices tableIndices = new TableIndices();
    HIndexSpecification spec = new HIndexSpecification(indexNameToAdd);
    spec.addIndexColumn(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build(),
            "age", ValueType.STRING, null);
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
}
