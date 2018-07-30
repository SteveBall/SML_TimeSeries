package uk.gov.ons.JDemetra

import com.cloudera.sparkts.{DateTimeIndex, UniformDateTimeIndex}

import scala.runtime.ScalaRunTime.stringOf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import ec.tstoolkit.timeseries.simplets.{TsData, TsFrequency}

import scala.annotation.switch


package object processors {

  case class IndexArgs(tsFreq : TsFrequency, startYear : Int, startPeriod : Int)

  val freqMap = Map("com.cloudera.sparkts.MonthFrequency" -> TsFrequency.Monthly,
                    "com.cloudera.sparkts.YearFrequency" -> TsFrequency.Yearly)

  val SA = "sa"

  def getFcast(fCast : Int) : String = "fcasts(-" + fCast + ")"
  def getBcast(bCast : Int) : String = "bcasts(-" + bCast + ")"


  // Debug helpers
  def printVector(vectorIn : Vector) = {
    printArray(vectorIn.toArray)
  }

  def printArray(arr : Array[Double]) = {
    val str = stringOf(arr)
    System.out.println(str)
  }

  def getTsData(indexArgs : IndexArgs, tsIn : Array[Double], copyFlag : Boolean) : TsData = {
    new TsData(indexArgs.tsFreq, indexArgs.startYear, indexArgs.startPeriod, tsIn, copyFlag)
  }

  def convertToIndexArgs(dtIndex : DateTimeIndex) : IndexArgs = {

    val freq = freqMap(dtIndex.asInstanceOf[UniformDateTimeIndex].frequency.getClass.getTypeName)

    val start = dtIndex.first

    val startYear = start.getYear

    val startPeriod = (freq: @switch) match {
      case TsFrequency.Monthly  => start.getMonthValue - 1
      case TsFrequency.Yearly  => 0
      case _  => throw new Exception("Unsupported TsFrequency : " + freq)
    }

    IndexArgs(freq, startYear, startPeriod)

  }

}

