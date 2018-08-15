package uk.gov.ons

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

package object timeseries {

  val conf = new SparkConf().setAppName("RegArimaSpecTests").setMaster("local[2]")
  val sc = new SparkContext(conf)
  val sqlContext = SparkSession.builder().getOrCreate().sqlContext

  val SPEC_FILE_LOC = "../SML_TimeSeries/src/main/resources/"
  val XML = ".xml"

  val tsv = SPEC_FILE_LOC + "data/JD_Test.tsv"

  val TIMESTAMP = "timestamp"
  val SYMBOL = "symbol"
  val PRICE = "price"

  def loadObservations(sqlContext: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
      val tokens = line.split('\t')
      val dt = ZonedDateTime.of(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, 0, 0, 0, 0,
        ZoneId.systemDefault())
      val symbol = tokens(3)
      val price = tokens(5).toDouble
      Row(Timestamp.from(dt.toInstant), symbol, price)
    }
    val fields = Seq(
      StructField(TIMESTAMP, TimestampType, true),
      StructField(SYMBOL, StringType, true),
      StructField(PRICE, DoubleType, true)
    )
    val schema = StructType(fields)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def getONSTimeSeriesRDD : ONSTimeSeriesRDD[String] = {

    val df = loadObservations(sqlContext, tsv)

    val startDate = "2015-01-01T00:00:00"
    val finishDate = "2017-12-01T00:00:00"

    val dtIndex = DateTimeIndexUtils.monthlyDTI(startDate, finishDate)

    ONSTimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, df,  TIMESTAMP, SYMBOL, PRICE)
  }
}