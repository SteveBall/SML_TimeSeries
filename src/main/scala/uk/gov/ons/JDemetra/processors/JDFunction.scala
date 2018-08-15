package uk.gov.ons.JDemetra.processors


import ec.tstoolkit.timeseries.simplets.TsFrequency
import org.apache.spark.SparkContext

trait JDFunction {


  var indexArgs : IndexArgs = IndexArgs(TsFrequency.Monthly, 0, 0)

  // need to sort this out - maybe use TaskContext and / or setLocalProperty
  val SPEC_FILE_LOC = "file:///Users/stephenball/Downloads/SML_TimeSeries/src/main/resources/"
  val XML = ".xml"

  def getSparkContext : SparkContext


  val specFileLocation = SPEC_FILE_LOC
}