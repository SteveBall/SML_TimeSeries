package uk.gov.ons.JDemetra.processors

import ec.satoolkit.algorithm.implementation.TramoSeatsProcessingFactory
import ec.satoolkit.tramoseats.TramoSeatsSpecification
import ec.tstoolkit.modelling.arima.tramo.TramoSpecification
import ec.tstoolkit.timeseries.simplets.{TsData, TsFrequency}
import org.apache.spark.mllib.linalg.{Vector, Vectors}


trait TramoSeats extends JDFunction{

  private case class TSProcessArgs(predictionLength : Int, fCast : Int, outliersCriticalValue : Double)

  private var tsProcessingArgs : TSProcessArgs = TSProcessArgs(0, 0, 0.0)

  val OutlierDelim = "<>"

  def forecastTS(vectorIn: Vector) : Vector = {

    val tsIn = vectorIn.toArray

    val tsData = getTsData(indexArgs, tsIn, false)

    val rsafull = TramoSeatsSpecification.RSAfull.clone

    rsafull.getSeatsSpecification.setPredictionLength(tsProcessingArgs.predictionLength)

    rsafull.getTramoSpecification.getOutliers.setCriticalValue(tsProcessingArgs.outliersCriticalValue)

    rsafull.getBenchmarkingSpecification.setEnabled(true)

    // Process the data using the RSAfull spec
    val myrslts = TramoSeatsProcessingFactory.process(tsData, rsafull)

    /*
      fcasts(-5) returns 60 obs
      fcasts(-6) return 72 obs

      so the -n figure returns ...   n X 12 obs   (annual ??)
     */
    val res = myrslts.getData(getFcast(tsProcessingArgs.fCast), classOf[TsData])

    // ********* THIS NEEDS LOOKING AT
    Vectors.dense(res.internalStorage())
  }



  def setTSProcessingArgs(predLength : Int, fCastIn : Int, outliersCriticalVal : Double) = {
    forecastAdj = fCastIn
    tsProcessingArgs = TSProcessArgs(predLength, fCastIn, outliersCriticalVal)
  }

  def outliers(vectorIn: Vector) : String = {

    val tsIn = vectorIn.toArray

    val tsData = getTsData(indexArgs, tsIn, false)

    val preprocessor = TramoSpecification.TR4.build

    val model = preprocessor.process(tsData, null)
    val outliers = model.outliersEstimation(true, false)

    var res = ""

    for (o <- outliers) res += o + OutlierDelim

    res
  }
}
