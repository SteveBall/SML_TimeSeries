package uk.gov.ons.timeseries

import java.io.{BufferedReader, InputStreamReader}
import java.sql.Timestamp
import java.time.ZonedDateTime
import java.util.Arrays

import com.cloudera.sparkts.{DateTimeIndex, TimeSeriesRDD}
import uk.gov.ons.JDemetra.processors.{RegArima, TramoSeats, X13, convertToIndexArgs}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.math.Ordering
import scala.reflect.ClassTag

class ONSTimeSeriesRDD[K] (override val index: DateTimeIndex, parent: RDD[(K, Vector)])
                          (implicit override val kClassTag: ClassTag[K])
  extends TimeSeriesRDD(index, parent) with TramoSeats with RegArima with X13{


  indexArgs = convertToIndexArgs(index)

  /*
   * kt._1  is the key of the timeseries
   * kt._2  is the series observations
   */
  override def mapSeries[U](f: (Vector) => Vector, dti : DateTimeIndex): ONSTimeSeriesRDD[K] = {
    //  def map[U](f : scala.Function1[T, U])(implicit evidence$3 : scala.reflect.ClassTag[U]) : org.apache.spark.rdd.RDD[U] = { /* compiled code */ }
    new ONSTimeSeriesRDD[K](dti, map(kt => (kt._1, f(kt._2))))
  }

  /*
   * Apply a Tramo Seats forecast to the series
   */
  def forecastTramoSeats(predLength : Int, fCastIn : Int, outliersCriticalVal : Double) : ONSTimeSeriesRDD[K] = {

    setTSProcessingArgs(predLength, fCastIn, outliersCriticalVal)

    mapSeries(forecastTS, DateTimeIndexUtils.forecast(index, forecastAdj * 12 ))  // sort this out    * 12 ???
  }

  /*
   * Apply a RegArima forecast to the series
   * This function will only return the forecasted elements
   */
  def forecastRegArima(nf : Int) : ONSTimeSeriesRDD[K] = {

    setRegArimaProcessingArgs(nf, false)

    mapSeries(forecastRegArima, DateTimeIndexUtils.forecast(index, forecastAdj))
  }

  /*
   * Apply a RegArima forecast to the series
   * This function will return both the original and forecasted series combined
   */
  def forecastRegArimaFull(nf : Int) : ONSTimeSeriesRDD[K] = {

    setRegArimaProcessingArgs(nf, true)

    mapSeries(forecastRegArima, DateTimeIndexUtils.forecastFull(index, forecastAdj))
  }

  def seasAdj : ONSTimeSeriesRDD[K] = {

    mapSeries(seasAdj, index)
  }

  def trend : ONSTimeSeriesRDD[K] = {

    mapSeries(trend, index)
  }

  def applyOutlierDetection : RDD[(K, String)] = {

    this.mapValues(outliers)
  }

  def detectOutliers : RDD[(K, String)] = {

    applyOutlierDetection.map(o => (o._1, o._2.split(OutlierDelim)))
      .flatMapValues(x => x)
  }


}

object ONSTimeSeriesRDD {

  /**
    * Loads an ONSTimeSeriesRDD from a directory containing a set of CSV files and a date-time index.
    */
  def onsTimeSeriesRDDFromCsv(path: String, sc: SparkContext)
  : ONSTimeSeriesRDD[String] = {
    val rdd = sc.textFile(path).map { line =>
      val tokens = line.split(",")
      val series = new DenseVector(tokens.tail.map(_.toDouble))
      (tokens.head, series.asInstanceOf[Vector])
    }

    val fs = FileSystem.get(new Configuration())
    val is = fs.open(new Path(path + "/timeIndex"))
    val dtIndex = DateTimeIndex.fromString(new BufferedReader(new InputStreamReader(is)).readLine())
    is.close()

    new ONSTimeSeriesRDD[String](dtIndex, rdd)
  }


  /**
    * Instantiates a TimeSeriesRDD from a DataFrame of observations.
    *
    * @param targetIndex DateTimeIndex to conform all the series to.
    * @param df The DataFrame.
    * @param tsCol The Timestamp column telling when the observation occurred.
    * @param keyCol The string column labeling which string key the observation belongs to..
    * @param valueCol The observed value..
    */
  def timeSeriesRDDFromObservations(
                                     targetIndex: DateTimeIndex,
                                     df: DataFrame,
                                     tsCol: String,
                                     keyCol: String,
                                     valueCol: String): ONSTimeSeriesRDD[String] = {
    val rdd = df.select(tsCol, keyCol, valueCol).rdd.map { row =>
      ((row.getString(1), row.getAs[Timestamp](0)), row.getDouble(2))
    }
    implicit val ordering = new Ordering[(String, Timestamp)] {
      override def compare(a: (String, Timestamp), b: (String, Timestamp)): Int = {
        val strCompare = a._1.compareTo(b._1)
        if (strCompare != 0) strCompare else a._2.compareTo(b._2)
      }
    }

    val shuffled = rdd.repartitionAndSortWithinPartitions(new Partitioner() {
      val hashPartitioner = new HashPartitioner(rdd.partitions.length)
      override def numPartitions: Int = hashPartitioner.numPartitions
      override def getPartition(key: Any): Int =
        hashPartitioner.getPartition(key.asInstanceOf[(Any, Any)]._1)
    })

    new ONSTimeSeriesRDD[String](targetIndex, shuffled.mapPartitions { iter =>
      val bufferedIter = iter.buffered
      new Iterator[(String, DenseVector)]() {
        override def hasNext: Boolean = bufferedIter.hasNext

        override def next(): (String, DenseVector) = {
          // TODO: this will be slow for Irregular DateTimeIndexes because it will result in an
          // O(log n) lookup for each element.
          val series = new Array[Double](targetIndex.size)
          Arrays.fill(series, Double.NaN)
          val first = bufferedIter.next()
          val firstLoc = targetIndex.locAtDateTime(
            ZonedDateTime.ofInstant(first._1._2.toInstant, targetIndex.zone))
          if (firstLoc >= 0) {
            series(firstLoc) = first._2
          }
          val key = first._1._1
          while (bufferedIter.hasNext && bufferedIter.head._1._1 == key) {
            val sample = bufferedIter.next()
            val sampleLoc = targetIndex.locAtDateTime(
              ZonedDateTime.ofInstant(sample._1._2.toInstant, targetIndex.zone))
            if (sampleLoc >= 0) {
              series(sampleLoc) = sample._2
            }
          }
          (key, new DenseVector(series))
        }
      }
    })
  }
}


