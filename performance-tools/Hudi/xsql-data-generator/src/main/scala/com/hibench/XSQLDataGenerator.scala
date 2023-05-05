package com.hibench

import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.sql
import java.text.SimpleDateFormat
import java.util.{Date, UUID}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class XSQLDataGenerator {
}

/**
  * $1: 数据大小，支持K、M、G、T、L 4种单位, 其中L表示直接指定行
  * $1: 数据格式，"string(30)|int(5)|string(100)|timestamp(3D)|double(5)"，其中timestamp的长度表示时间跨度,1D表示1天，1M表示1个月，1Y表示1年
  * $2: HDFS上的文件存储路径 /path/table_name
  * $4: 生成数据时，Spark任务的Parallelism参数
  * Demo:
   spark-submit --master yarn-client --executor-cores 5  --executor-memory 15G  --num-executors 100 --class com.huawei.tools.spark.generator.XSQLDataGenerator /opt/client/SQLDataGenerator.jar 1000L "string(10),name-int(3),string(3)" /tmp/test.data 5
  */

object XSQLDataGenerator {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[XSQLDataGenerator])

  val REGEX = """(string|int|long|timestamp|double|date)\(([0-9]+[y|m|d|:]?[0-9]*)\)""".r
  val DATE_RANGE_REGEX = """([0-9]+)([y|m|d]?)""".r

  val D_RANGE: Long = 24 * 60 * 60 * 1000L
  val M_RANGE: Long = 30 * 24 * 60 * 60 * 1000L
  val Y_RANGE: Long = 365 * 24 * 60 * 60 * 1000L
  val MAX_TIME: Long = new Date().getTime

  val DEFAULT_PARTITION_PATHS = List ("americas/united_states/san_francisco", "americas/brazil/sao_paulo", "asia/india/chennai"
    , "americas1/united_states/san_francisco", "americas/brazil1/sao_paulo", "asia/india/chennai1"
    ,"americas2/united_states/san_francisco", "americas/brazil2/sao_paulo", "asia/india/chennai2"
    ,"americas3/united_states/san_francisco", "americas/brazil3/sao_paulo", "asia/india/chennai3"
    ,"americas4/united_states/san_francisco", "americas/brazil4/sao_paulo", "asia/india/chennai4"
    ,"americas5/united_states/san_francisco", "americas/brazil5/sao_paulo", "asia/india/chennai5"
    ,"americas6/united_states/san_francisco", "americas/brazil6/sao_paulo", "asia/india/chennai63"
    ,"americas6/united_states/san_francisco5", "americas/brazil6/sao_paul5", "asia/india/chennai62"
    ,"americas6/united_states/san_francisco6", "americas/brazil6/sao_paulo6", "asia/india/chennai61"
    ,"americas6/united_states/san_francisco7", "americas/brazil6/sao_paulo7", "asia/india/chennai7"
    ,"americas6/united_states/san_francisco8", "americas/brazil6/sao_paulo8", "asia/india/chennai8"
    ,"americas6/united_states/san_francisco9", "americas/brazil6/sao_paulo9", "asia/india/chennai9"
    ,"americas6/united_states/san_francisco10", "americas/brazil6/sao_paulo10", "asia/india/chennai10"
    ,"americas6/united_states/san_francisco11", "americas/brazil6/sao_paulo11", "asia/india/chennai11"
    ,"americas6/united_states/san_francisco12", "americas/brazil6/sao_paulo12", "asia/india/chennai12"
    ,"americas6/united_states/san_francisco13", "americas/brazil6/sao_paulo13", "asia/india/chennai13"
    ,"americas6/united_states/san_francisco14", "americas/brazil6/sao_paulo14", "asia/india/chennai14"
    ,"americas6/united_states/san_francisco15", "americas/brazil6/sao_paulo15", "asia/india/chennai15"
    ,"americas6/united_states/san_francisco16", "americas/brazil6/sao_paulo16", "asia/india/chennai16"
    ,"americas6/united_states/san_francisco17", "americas/brazil6/sao_paulo17", "asia/india/chennai17"
    ,"americas6/united_states/san_francisco18", "americas/brazil6/sao_paulo18", "asia/india/chennai18"
    ,"americas6/united_states/san_francisco19", "americas/brazil6/sao_paulo19", "asia/india/chennai19"
    ,"americas6/united_states/san_francisco20", "americas/brazil6/sao_paulo20", "asia/india/chennai20"
    ,"americas6/united_states/san_francisco21", "americas/brazil6/sao_paulo21", "asia/india/chennai21"
    ,"americas6/united_states/san_francisco22", "americas/brazil6/sao_paulo22", "asia/india/chennai22"
    ,"americas6/united_states/san_francisco23", "americas/brazil6/sao_paulo23", "asia/india/chennai23"
    ,"americas6/united_states/san_francisco24", "americas/brazil6/sao_paulo24", "asia/india/chennai24"
    ,"americas6/united_states/san_francisco25", "americas/brazil6/sao_paulo25", "asia/india/chennai25"
    ,"americas6/united_states/san_francisco26", "americas/brazil6/sao_paulo26", "asia/india/chennai26"
    ,"americas6/united_states/san_francisco27", "americas/brazil6/sao_paulo27", "asia/india/chennai27"
    ,"americas6/united_states/san_francisco28", "americas/brazil6/sao_paulo28", "asia/india/chennai28"
    ,"americas6/united_states/san_francisco29", "americas/brazil6/sao_paulo29", "asia/india/chennai29"
    ,"americas6/united_states/san_francisco30", "americas/brazil6/sao_paulo30", "asia/india/chennai30"
    ,"americas6/united_states/san_francisco31", "americas/brazil6/sao_paulo31", "asia/india/chennai31")

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      LOG.error("Usage:SparkSQLDataGenerator 3M \"string(length)|int(length)|long(length)|double(5:3)|timestamp(3M)...\" outputFile.txt 200")
      return
    }

    val Array(dataSizeStr, rowFormat, outputFile, slices, myNumber) = args
    LOG.info(s"Arguments: DataSize:$dataSizeStr, RowFormat:$rowFormat, SaveFile:$outputFile, slices:$slices")

    val line = (rowFormat toLowerCase()).trim
    val (dataSize, totalLines, lineLength, cols) = getTotalLine(dataSizeStr, line)

    // 避免出现totalLines超过Int.MaxValue的情况
    var times: Int = 1
    var odd: Int = 0
    var repeatTimes = totalLines.toInt
    if (Int.MaxValue < totalLines) {
      times = (totalLines / (Int.MaxValue - 1)).toInt
      odd = (totalLines % Int.MaxValue).toInt
      repeatTimes = Int.MaxValue - 1
    }

    val iSlices = Math.min(slices.toInt, repeatTimes)
    LOG.warn(s"DataSize       : $dataSizeStr ($dataSize Byte)")
    LOG.warn(s"RowDataFormate : $line")
    LOG.warn(s"Total Lines    : $totalLines")
    LOG.warn(s"Slices         : $iSlices")
    LOG.warn(s"Line Length    : $lineLength")
    LOG.warn(s"RepeatTimes    : $repeatTimes")
    LOG.warn(s"OneTimeLines   : $times")
    LOG.warn(s"Odd            : $odd")


    val spark = SparkSession.builder()
      .config("spark.task.maxDirectResultSize", "2G")
      .config("spark.rpc.message.maxSize", "2046")
      .config("spark.kryoserializer.buffer.max", "256m")
      .config("spark.kryoserializer.buffer", "64m")
      .appName(s"XSQLDataGenerator:($dataSizeStr)")
      .getOrCreate()

    val keyRange = myNumber.toInt / slices.toInt

    val resultRDD = spark.sparkContext.parallelize(0 to myNumber.toInt, slices.toInt).mapPartitionsWithIndex { case (index, itor) =>
      var startKey: Long = index*keyRange
      itor.map { item =>
        startKey = startKey + 1
        generatorRowSeq(cols,
          generatorLine(cols, line), DEFAULT_PARTITION_PATHS.length, startKey - 1)
      }
    }

    var schema: Option[StructType] = Option(generatorSchema(cols))

    val df = spark.createDataFrame(resultRDD, schema.get)
    LOG.info(s"Start to Save data to $outputFile ....")
    df.write.mode(SaveMode.Overwrite).save(outputFile)

    spark.sparkContext.stop()

    LOG.info(s"Save data finish!")
  }

  def getTotalLine(strSize: String, line:String): (Long,Long,Long, List[(String, String, String)]) = {
    val dataSizeRegex = """([0-9]+)([K|M|G|T|L]?)""".r
    val dataSizeRegex(num, unit) = strSize
    var dataSize : Long = 0
    var totalLine:Long = -1

    val stringCols = REGEX.findAllIn(line).toList
    LOG.info(s"stringCols:$stringCols")
    val cols = stringCols.map
    {
      oneCol =>
      {
        val REGEX(dataType, range) = oneCol
        (dataType, range, oneCol.replace("(","\\(").replace(")","\\)"))
      }
    }
    LOG.info(s"cols:$cols")
    val oneLine = generatorLine(cols, line)
    LOG.info(s"oneLine:$oneLine")
    val lineLength = oneLine.length() + System.getProperty("line.separator").length()


    unit match {
      case "K" => dataSize = num.toLong * 1024
      case "M" => dataSize = num.toLong * 1024 * 1024
      case "G" => dataSize = num.toLong * 1024 * 1024 * 1024
      case "T" => dataSize = num.toLong * 1024 * 1024 * 1024 * 1024
      case "L" => totalLine = num.toLong

      case _ => LOG.warn(s"Unknow Unit: $unit , use K."); num.toLong * 1024
    }

    if (totalLine > 0)
    {
      dataSize = totalLine * lineLength
    }
    else
    {
      totalLine = dataSize / lineLength
    }
    (dataSize, totalLine, lineLength, cols)
  }

  def generatorSchema(cols: List[(String, String, String)]): StructType = {
    var schema: StructType = new StructType()
    var index: Int = 0
    // 加入分区路径 和uuid 以及递增id
    schema = schema.add(new StructField("p", StringType, false))
    schema = schema.add(new StructField("uuid_key", StringType, false))
    schema = schema.add(new StructField("primary_key", LongType, false))
    cols.foreach { oneCol =>
      val (dataType, _, _) = oneCol
      schema = dataType match {
        case "string" => schema.add(new StructField("col" + index, StringType, true))
        case "int" => schema.add(new StructField("col" + index, IntegerType, true))
        case "long" => schema.add(new StructField("col" + index, LongType, true))
        case "timestamp" => schema.add(new StructField("col" + index, TimestampType, true))
        case "double" => schema.add(new StructField("col" + index, DoubleType, true))
        case "date" => schema.add(new StructField("col" + index, DateType, true))
        case _ => schema
      }
      index = index + 1
    }
    schema
  }

  def generatorRowSeq(cols: List[(String, String, String)], rawString: String, numPartitions: Int = 2, keyIndex: Long = 0): Row = {
    val tmpRes = ArrayBuffer[Any]()
    // 加入分区路径 和uuid
    tmpRes += DEFAULT_PARTITION_PATHS(Random.nextInt(numPartitions))
    tmpRes += reorderUUID(java.util.UUID.randomUUID())
    tmpRes += keyIndex
    cols.map(_._1).zip(rawString.split('|')).foreach { case (dataType, dataValue) =>
        dataType match {
          case "string" => tmpRes += dataValue
          case "int" => tmpRes += dataValue.toInt
          case "long" => tmpRes += dataValue.toLong
          case "timestamp" => tmpRes += java.sql.Timestamp.valueOf(dataValue)
          case "double" => tmpRes += dataValue.toDouble
          case "date" => tmpRes += convertStringToDate(dataValue)
          case _ =>
        }
    }
    Row.fromSeq(tmpRes)
  }

  def reorderUUID(uuid: UUID): String = {
    val strs = uuid.toString.split("-")
    val sb: StringBuffer = new StringBuffer();
    sb.append(strs(2)).append(strs(1)).append(strs(0)).append(strs(3)).append(strs(4))
    return sb.toString
  }

  def generatorLine(cols: List[(String, String, String)], rowFormat: String): String = {
    var line = rowFormat
    cols.foreach(oneCol => {
      val (dataType, range, strCol) = oneCol
      dataType match {
        case "string" => line = line.replaceFirst(strCol, RandomStringUtils.randomAlphanumeric(range.toInt))
        case "int" => line = line.replaceFirst(strCol, RandomStringUtils.randomNumeric(range.toInt))
        case "long" => line = line.replaceFirst(strCol, RandomStringUtils.randomNumeric(range.toInt))
        case "timestamp" => line = line.replaceFirst(strCol, randomDataTime(range))
        case "double" => line = line.replaceFirst(strCol, randomDouble(range))
        case "date" => line = line.replaceFirst(strCol, randomDataTime(range))
        case _ => line = ""
      }
    })
    line
  }

  def generatorLine1(cols: List[(String, String, String)], rowFormat: String): String = {
    var line = rowFormat
    cols.foreach(oneCol => {
      val (dataType, range, strCol) = oneCol
      dataType match {
        case "string" => line = line.replaceFirst(strCol, "add")
        case "int" => line = line.replaceFirst(strCol, "200")
        case "timestamp" => line = line.replaceFirst(strCol, randomDataTime(range))
        case "double" => line = line.replaceFirst(strCol, "2.3")
        case _ => line = ""
      }
    })
    line
  }

  def randomDouble(range:String) :String = {
    val Array(r1,r2) = range.split(":")
    RandomStringUtils.randomNumeric(r1.toInt) + "." + RandomStringUtils.randomNumeric(r2.toInt)
  }

  def convertStringToDate(range: String): java.sql.Date = {
    val formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val ud = formater.parse(range)
    new sql.Date(ud.getYear, ud.getMonth, ud.getDay)
  }

  def randomDataTime(range: String): String = {
    val DATE_RANGE_REGEX(r, u) = range
    val formater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var lRange: Long = r.toLong
    u match {
      case "y" => lRange = lRange * Y_RANGE
      case "m" => lRange = lRange * M_RANGE
      case "d" => lRange = lRange * D_RANGE
      case _ => lRange =   lRange * D_RANGE
    }
    val time = MAX_TIME - (lRange * Random.nextDouble()).toLong
    formater.format(new Date(time))
  }

  def runCreateCarbonTable(df: DataFrame): Unit = {
    val xtime = System.currentTimeMillis()
    df.write.format("carbon")
  }
}
