/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.bigdata.ctbase.examples

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Result, Scan, Table}
import org.apache.hadoop.hbase.clustertable.acl.RunningMode
import org.apache.hadoop.hbase.clustertable.client.{CTBase, CTBaseInterface, CTResult, CTRow, QueryColumns}
import org.apache.hadoop.hbase.clustertable.common.{CTConstants, CTUtil}
import org.apache.hadoop.hbase.clustertable.schema.{CellMappings, IndexInfo, RowKeySchema, SchemaManager, Section, SubTableInfo}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.net.URI
import java.util
import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsScalaMapConverter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

/**
 * ctbase 导出样例主类
 */
object CTBaseExportExample {
	private val LOG: Logger = LoggerFactory.getLogger("CTBaseExportExample")

	// 本地连接配置
	private val conf: Configuration = HBaseConfiguration.create()

	// HDFS 客户端
	private var fs: FileSystem = _

	// ctbase类
	private var ctBase: CTBaseInterface = _

	// 分区数量
	private val PARTITION_NUM: Int = 10

	// CTBase聚簇表名
	private var clusterTableName: String = "ClusterTableTest"

	// CTBase用户表名
	private var userTableName: String = "Cust_info"

	// CTBase用户表查询列
	private val queryColumns: QueryColumns = new QueryColumns

	// 导出使用索引名
	private var indexName: String = "idx_p"

	// 使用条件（仅限索引字段）：1.非独立索引表主索引或二级索引导出 2.独立索引表二级索引导出，其他场景无效，需要增加后置过滤
	private var conditions: Map[String, String] = Map[String, String]()

	// CTBase用户表导出路径(HDFS)
	private var exportPath: String = "/tmp/export"

	// 删除文件夹标志
	private val DELETE_PATH_FLAG: Boolean = true

	// scan缓存
	private val CACHE_NUM: Int = 2000

	def main(args: Array[String]): Unit = {
		parserParams(args)
		try {
			ctBase = new CTBase(conf, RunningMode.DBA_MODE, true)
			fs = FileSystem.newInstance(new URI(exportPath), conf)

			// 初始化查询列
			queryColumns.addColumn("ID")
			queryColumns.addColumn("UID")
			queryColumns.addColumn("NAME")
			queryColumns.addColumn("AGE")
			queryColumns.addColumn("WEIGHT")
			queryColumns.addColumn("HIGHT")
			queryColumns.addColumn("LOCATION")
			queryColumns.addColumn("GENDER")
			queryColumns.addColumn("CDATE")
			queryColumns.addColumn("UDATE")
			queryColumns.addColumn("ISCHINESE")

			exportUserTable(indexName, conditions)
		} catch {
			case ex: Exception => LOG.error("Failed to export table, because ", ex)
		}
		System.exit(0)
	}

	/**
	 * 参数解析
	 * @param args 传入参数
	 */
	def parserParams(args: Array[String]): Unit = {
		if (args.length < 4 && args.length > 0) {
			throw new IllegalArgumentException("param numbs must equals 0 or 4")
		}

		if (StringUtils.isNotEmpty(args(0))) {
			userTableName = args(0)
		}

		if (StringUtils.isNotEmpty(args(1))) {
			clusterTableName = args(1)
		}

		if (StringUtils.isNotEmpty(args(2))) {
			indexName = args(2)
		}

		if (StringUtils.isNotEmpty(args(3))) {
			exportPath = args(3)
		}

		if (args.length > 4) {
			if (StringUtils.isNotEmpty(args(4))) {
				val condMap: util.HashMap[String, String] = new Gson().fromJson(args(4), new TypeToken[util
				.HashMap[String,
					String]]() {}
					.getType)
				conditions = condMap.asScala.toMap
			}
		}
	}

	/**
	 * 导出用户表--可选择主索引或者二级索引
	 *
	 * @param index      索引名
	 * @param conditions 条件
	 */
	private def exportUserTable(index: String = "", conditions: Map[String, String] = Map[String, String]()): Unit = {
		if (StringUtils.isEmpty(index)) {
			LOG.error("index can not be null!")
			throw new IllegalArgumentException("index can not be null!")
		}

		val userTableInfo: SubTableInfo = ctBase.getSubTable(userTableName, clusterTableName)
		if (Option(userTableInfo).isEmpty) {
			LOG.error("get userTableInfo ailed!")
			throw new IOException("get userTableInfo failed!")
		}

		val priIndexInfo: IndexInfo = userTableInfo.getPrimaryIndex
		if (Option(priIndexInfo).isEmpty) {
			LOG.error("get priIndexInfo ailed!")
			throw new IOException("get priIndexInfo failed!")
		}

		var indexInfo: IndexInfo = userTableInfo.getIndexInfo(index)
		if (Option(indexInfo).isEmpty) {
			LOG.error("index name error!")
			throw new IllegalArgumentException("index name error!")
		}

		// 全表扫描标志
		var needFullScan = false

		// 非独立索引表，条件为空，强制使用主索引
		if (!priIndexInfo.isIndexTableStandAlone && (Option(conditions).isEmpty || conditions.isEmpty)) {
			indexInfo = priIndexInfo
			needFullScan = true
		}

		// 独立索引表主索引查询，使用全表扫描
		if (priIndexInfo.isIndexTableStandAlone && indexInfo.isPrimary) {
			needFullScan = true
		}

		// 根据索引获取rowKey范围
		val (startKey, stopKey) = getRowKeyRange(indexInfo, priIndexInfo, conditions, needFullScan)

		if (!priIndexInfo.isIndexTableStandAlone && (Option(startKey).isEmpty || Option(stopKey).isEmpty)) {
			LOG.error("get rowkey range error!")
			throw new IOException("get rowkey range error!")
		}
		LOG.info("************startKey={}", Bytes.toString(startKey))
		LOG.info("************stopKey={}", Bytes.toString(stopKey))

		// 获取mapping
		val mappings: CellMappings = userTableInfo.getMappings
		if (Option(mappings).isEmpty) {
			LOG.error("get mappings error!")
			throw new IOException("get mappings error!")
		}

		// 非独立索引表使用，用于区分数据是哪个用户表
		val (identifierPos, idxParts, identifier): (Int, Int, String) = getIdentifier4dataCheck(priIndexInfo)

		// 分割区间,如果精确查找，不分割
		val splitRowKeyBytes: ListBuffer[(Array[Byte], Array[Byte])] = ListBuffer[(Array[Byte], Array[Byte])]()
		if (!priIndexInfo.isIndexTableStandAlone && (Bytes.toString(startKey).split("\\" + CTConstants.ROW_KEY_SEPARATOR_DEFAULT).length + 1 >= idxParts)) {
			splitRowKeyBytes += new Tuple2[Array[Byte], Array[Byte]](startKey, stopKey)
		} else {
			splitRowKeyBytes ++= splitRowKeyRange(startKey, stopKey)
		}

		// 特殊场景，独立索引表二级索引导出
		val exportBySecIndex4StandAlone: Boolean = (!indexInfo.isPrimary) && priIndexInfo.isIndexTableStandAlone

		// 变更变量类型，用于广播
		val CLUSTER_TABLE_NAME: String = clusterTableName
		val USER_TABLE_NAME: String = userTableName
		val INDEX_NAME: String = indexName

		// spark配置
		val sc: SparkContext = initSparkContext()

		// 广播变量
		sc.broadcast(CLUSTER_TABLE_NAME)
		sc.broadcast(USER_TABLE_NAME)
		sc.broadcast(INDEX_NAME)
		sc.broadcast(CACHE_NUM)
		sc.broadcast(exportBySecIndex4StandAlone)

		// 扫描聚簇表/独立索引表
		val scanResultRDD: RDD[Result] = scanByIndex(CLUSTER_TABLE_NAME, USER_TABLE_NAME, INDEX_NAME, sc,
			splitRowKeyBytes,
			exportBySecIndex4StandAlone)

		val scanResultsArray: Array[Result] = scanResultRDD.collect()
		LOG.info("***************scan data nums={}", scanResultsArray.length)

		// 如果是二级索引导出，需要获取rowKey
		var resultCollectArray: Array[Result] = null
		if (!indexInfo.isPrimary) {
			val rowKeyGets: ListBuffer[Get] = ListBuffer[Get]()
			for (scanResult <- scanResultsArray) {
				val priRowKey: Array[Byte] = RowKeySchema.getPrimaryKey(userTableInfo, indexInfo, Bytes
					.toString(scanResult.getRow), true)
				rowKeyGets += new Get(priRowKey)
			}
			resultCollectArray = collectDataArrayUseSecIdx(CLUSTER_TABLE_NAME, sc, rowKeyGets)
		} else {
			resultCollectArray = scanResultsArray
		}

		// 转换result到ctResult，并获取查询列对应字符串
		SchemaManager.switchRunningMode(RunningMode.DEV_MODE, true)

		// 非独立索引表需要过滤用户表数据
		val ctValues: mutable.LinkedHashSet[String] = mutable.LinkedHashSet[String]()
		for (result <- resultCollectArray) {
			if (priIndexInfo.isIndexTableStandAlone || !indexInfo.isPrimary || !StringUtils.isNotEmpty(priIndexInfo.getIdxCluster) || !CTUtil
				.filterClusterRow(result, priIndexInfo
					.getRowkeySeparator,
					idxParts,
					identifier,
					identifierPos)) {
				val ctResult = new CTResult(result, queryColumns, mappings, priIndexInfo)
				val ctValueBuffer = ListBuffer[String]()
				for (col <- queryColumns.getColumns.asScala) {
					ctValueBuffer += ctResult.getColumnAsStr(col)
				}
				ctValues += ctValueBuffer.mkString(CTConstants.COMMON_ITEM_SEPARATOR.toString)
			}
		}
		LOG.info("*******************export data nums={}", ctValues.size)

		// 保存数据到HDFS目录
		if (DELETE_PATH_FLAG) {
			deleteExportPath(new Path(exportPath))
		}
		sc.parallelize(ctValues.toSeq, PARTITION_NUM).saveAsTextFile(exportPath)
		closeFs()
		sc.stop()
	}

	/**
	 * 根据索引获取rowkey范围
	 *
	 * @param indexInfo  指定的索引信息
	 * @param conditions 筛选条件
	 * @return rowkey范围
	 */
	private def getRowKeyRange(indexInfo: IndexInfo, priIndexInfo: IndexInfo, conditions: Map[String, String] =
	Map[String, String](), needFullScan: Boolean):
	(Array[Byte], Array[Byte]) = {
		var startRowKey: Array[Byte] = Array[Byte]()
		var stopRowKey: Array[Byte] = Array[Byte]()
		val row: CTRow = new CTRow

		// 条件为空，默认全表扫描
		if (needFullScan) {
			if (!priIndexInfo.isIndexTableStandAlone) {
				startRowKey = RowKeySchema.getRowPrefixOnly(priIndexInfo)
			}
		} else {
			val indexSectionColSet: Set[String] = indexInfo.getSections.asScala.toStream.map(section => section.getColumn)
				.filter(section => Option(section).nonEmpty)
				.toSet
			if (conditions.nonEmpty && (conditions.keySet & indexSectionColSet).nonEmpty) {
				for ((col, value) <- conditions) {
					row.addColumn(col, value)
				}
			}
			startRowKey = RowKeySchema.generateRowKey(indexInfo, row, true, false)
		}
		stopRowKey = RowKeySchema.getStopRow(startRowKey)
		(startRowKey, stopRowKey)
	}

	/**
	 * 分割scan rowkey的范围，假设rowkey拼接都是可见字符，非中文
	 *
	 * @param startRowKey 起始rowkey
	 * @param stopRowKey  结束rowkey，划分区间时，暂时不使用
	 * @return 分割的扫描范围
	 */
	private def splitRowKeyRange(startRowKey: Array[Byte], stopRowKey: Array[Byte]): ListBuffer[
		(Array[Byte],
			Array[Byte])] = {
		val rowKeyRanges: ListBuffer[(Array[Byte], Array[Byte])] = ListBuffer[(Array[Byte], Array[Byte])]()
		// 假设索引字段都是可见字符，不包含中文，对应ascii码 48~126, 按照字符划分
		for (i <- Range(32, 127, 1)) {
			rowKeyRanges += Tuple2(startRowKey ++ Array[Byte](i.toByte), startRowKey ++ Array[Byte]((i + 1).toByte))
		}
		rowKeyRanges
	}

	/**
	 * 获取主索引相关信息，主索引扫描时用于区分用户表数据
	 *
	 * @param priIndexInfo 主索引信息
	 * @return 索引标识位置，索引字段长度（包含前后缀），索引标识符
	 */
	private def getIdentifier4dataCheck(priIndexInfo: IndexInfo): (Int, Int, String) = {
		var identifierPos: Int = 0
		var idxParts: Int = 0
		val prefix = priIndexInfo.getRowPrefix
		val sections: util.List[Section] = priIndexInfo.getSections
		var identifier: String = null
		if (StringUtils.isNotEmpty(prefix)) {
			identifierPos += 1
			idxParts += 1
		}
		val loop = new Breaks
		loop.breakable {
			for (section <- sections.asScala) {
				if (section.isUsedAsIdentifier) {
					identifier = section.getIdentifier
					loop.break()
				} else {
					identifierPos += 1
				}
			}
		}
		idxParts += sections.size
		if (StringUtils.isNotEmpty(priIndexInfo.getRowSuffix)) {
			idxParts += 1
		}

		// The last separator.
		idxParts += 1

		(identifierPos, idxParts, identifier)
	}

	/**
	 * 初始化SparkContext
	 *
	 * @return sparkContext
	 */
	private def initSparkContext(): SparkContext = {
		val sparkConf: SparkConf = new SparkConf()
		sparkConf.setAppName("CTBase Export Example")
		sparkConf.set("spark.hbase.obtainToken.enabled", "true")
		sparkConf.set("spark.security.credentials.hbase.enabled", "true")
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		sparkConf.set("spark.kryo.registrator", "com.huawei.bigdata.ctbase.examples.MyRegistrator")
		val session: SparkSession = SparkSession.builder().appName("CTBase Export Example").config(sparkConf)
			.getOrCreate()
		session.sparkContext
	}

	/**
	 * 根据索引扫描数据
	 *
	 * @param clusterTableName            聚簇表名
	 * @param userTableName               用户表名
	 * @param indexName                   索引名
	 * @param sc                          SparkContext
	 * @param splitRowKeyBytes            分割的扫描区间
	 * @param exportBySecIndex4StandAlone 独立索引表二级索引导出标志
	 * @return 扫描结果
	 */
	private def scanByIndex(clusterTableName: String, userTableName: String, indexName: String, sc: SparkContext,
	                        splitRowKeyBytes: ListBuffer[(Array[Byte],
		                        Array[Byte])],
	                        exportBySecIndex4StandAlone: Boolean): RDD[Result] = {
		val scanResultRDD: RDD[Result] = sc.parallelize(splitRowKeyBytes, PARTITION_NUM).flatMap(
			(splitRowKeyRange: (Array[Byte], Array[Byte])) => {
				// 扫描数据
				var conn: Connection = null
				var table: Table = null
				val scanResults: ListBuffer[Result] = ListBuffer[Result]()
				try {
					conn = ConnectionFactory.createConnection(HBaseConfiguration.create())
					if (exportBySecIndex4StandAlone) {
						table = conn.getTable(TableName.valueOf(clusterTableName + "-" + userTableName + "-" + indexName))
					} else {
						table = conn.getTable(TableName.valueOf(clusterTableName))
					}
					val scan: Scan = new Scan().withStartRow(splitRowKeyRange._1).withStopRow(splitRowKeyRange._2)
					scan.setCaching(CACHE_NUM)
					val scannerIt: util.Iterator[Result] = table.getScanner(scan).iterator()
					while (scannerIt.hasNext) {
						scanResults += scannerIt.next
					}
				} catch {
					case ex: Exception => LOG.error("scan error!", ex)
				} finally {
					if (Option(table).isDefined) {
						try {
							table.close()
						}
						catch {
							case ex: Exception => LOG.error("table close error!", ex)
						}
					}
					if (Option(conn).isDefined) {
						try {
							conn.close()
						}
						catch {
							case ex: Exception => LOG.error("conn close error!", ex)
						}
					}
				}
				scanResults
			}
		)
		scanResultRDD
	}

	/**
	 * 使用二级索引时，收集真实数据
	 *
	 * @param clusterTableName 聚簇表名
	 * @param sc               SparkContext
	 * @param rowKeyGets       根据二级索引信息获得的rowkey集合
	 * @return 用户表数据信息
	 */
	private def collectDataArrayUseSecIdx(clusterTableName: String,
	                                      sc: SparkContext,
	                                      rowKeyGets: ListBuffer[Get]): Array[Result] = {
		sc.parallelize(rowKeyGets, PARTITION_NUM).mapPartitionsWithIndex(
			(_, getsIter: Iterator[Get]) => {
				var conn: Connection = null
				var table: Table = null
				val gets: util.List[Get] = new util.ArrayList[Get]()
				val getResults: ListBuffer[Result] = ListBuffer[Result]()
				try {
					conn = ConnectionFactory.createConnection(HBaseConfiguration.create())
					table = conn.getTable(TableName.valueOf(clusterTableName))
					while (getsIter.hasNext) {
						gets.add(getsIter.next)
						if (gets.size() >= CACHE_NUM) {
							getResults ++= table.get(gets).to[ListBuffer]
							gets.clear()
						}
						if (gets.size() > 0) {
							getResults ++= table.get(gets).to[ListBuffer]
						}
					}

				} catch {
					case ex: Exception => LOG.error("scan error!", ex)
				} finally {
					if (Option(table).isDefined) {
						try {
							table.close()
						}
						catch {
							case ex: Exception => LOG.error("table close error!", ex)
						}
					}
					if (Option(conn).isDefined) {
						try {
							conn.close()
						}
						catch {
							case ex: Exception => LOG.error("conn close error!", ex)
						}
					}
				}
				getResults.iterator
			}
		).collect()
	}

	/**
	 * 删除上传残留的导出问题
	 *
	 * @param exportPath 导出路径
	 */
	private def deleteExportPath(exportPath: Path): Unit = {
		try {
			fs.delete(exportPath, true)
		}
		catch {
			case ex: IOException => LOG.error("delete path failed!", ex)
		}
	}

	/**
	 * 关闭FileSystem
	 */
	private def closeFs(): Unit = {
		try {
			if (Option(fs).isDefined) {
				fs.close()
			} else {
				throw new IOException("fileSystem is null")
			}
		} catch {
			case ex: Exception => LOG.error("close fileSystem error!", ex)
		}
	}
}


