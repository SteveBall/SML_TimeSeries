package uk.gov.ons.timeseries

import com.cloudera.sparkts.api.java.DateTimeIndexFactory
import com.cloudera.sparkts.{DateTimeIndex, Frequency, UniformDateTimeIndex}

import scala.annotation.switch

import java.time.{LocalDateTime, ZoneId, ZonedDateTime}

import com.cloudera.sparkts.{BusinessDayFrequency, DateTimeIndex, MonthFrequency, TimeSeriesRDD }

object DateTimeIndexUtils {

  val zone = ZoneId.systemDefault()

  val ONE = 1

  def adjustMonthsFull(dt : DateTimeIndex, adjVal : Int, freq : Frequency) : UniformDateTimeIndex = {

    DateTimeIndexFactory.uniformFromInterval(dt.first, dt.last.plusMonths(adjVal), freq)
  }

  def adjustMonths(dt : DateTimeIndex, adjVal : Int, freq : Frequency) : UniformDateTimeIndex = {

    //DateTimeIndexFactory.uniformFromInterval(dt.last.plusMonths(ONE), dt.last.plusMonths(adjVal * 12), freq)
    DateTimeIndexFactory.uniformFromInterval(dt.last.plusMonths(ONE), dt.last.plusMonths(adjVal), freq)
  }

  def adjustYearsFull(dt : DateTimeIndex, adjVal : Int, freq : Frequency) : UniformDateTimeIndex = {

    DateTimeIndexFactory.uniformFromInterval(dt.first, dt.last.plusYears(adjVal), freq)
  }

  def adjustYears(dt : DateTimeIndex, adjVal : Int, freq : Frequency) : UniformDateTimeIndex = {

    DateTimeIndexFactory.uniformFromInterval(dt.last.plusYears(ONE), dt.last.plusYears(adjVal), freq)
  }

  /*
      Returns a new DateTimeIndex to cover the forecast period
   */
  def forecast(dt : DateTimeIndex, adjVal : Int) : UniformDateTimeIndex = {

    val freq = dt.asInstanceOf[UniformDateTimeIndex].frequency
    val freqType = freq.getClass.getTypeName

    (freqType: @switch) match {
      case "com.cloudera.sparkts.MonthFrequency"  => adjustMonths(dt, adjVal, freq)
      case "com.cloudera.sparkts.YearFrequency"  => adjustYears(dt, adjVal, freq)
      case _  => throw new Exception("Unsupported sparkts Frequency : " + freq)
    }

  }

  def forecastFull(dt : DateTimeIndex, adjVal : Int) : UniformDateTimeIndex = {

    val freq = dt.asInstanceOf[UniformDateTimeIndex].frequency
    val freqType = freq.getClass.getTypeName

    (freqType: @switch) match {
      case "com.cloudera.sparkts.MonthFrequency"  => adjustMonthsFull(dt, adjVal, freq)
      case "com.cloudera.sparkts.YearFrequency"  => adjustYearsFull(dt, adjVal, freq)
      case _  => throw new Exception("Unsupported sparkts Frequency : " + freq)
    }

  }

  // val dti = monthlyDTI("2015-01-01T00:00:00", "2017-12-01T00:00:00")
  def monthlyDTI(startDateTime : String, finishDateTime : String) : UniformDateTimeIndex = {

    DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse(startDateTime), zone),
      ZonedDateTime.of(LocalDateTime.parse(finishDateTime), zone),
      new MonthFrequency(ONE))
  }
}

