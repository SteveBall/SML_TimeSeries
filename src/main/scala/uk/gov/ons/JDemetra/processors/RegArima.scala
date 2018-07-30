package uk.gov.ons.JDemetra.processors

import ec.tstoolkit.modelling.arima.IPreprocessor
import ec.tstoolkit.modelling.arima.x13.RegArimaSpecification
import ec.tstoolkit.timeseries.regression.OutlierType
import org.apache.spark.mllib.linalg.{Vector, Vectors}

trait RegArima extends JDFunction{

  private case class RegArimaProcessingArgs(full : Boolean)

  private var regArimaProcessingArgs : RegArimaProcessingArgs = RegArimaProcessingArgs(false)


  def forecastRegArima(vectorIn: Vector) : Vector = {

    val tsIn = vectorIn.toArray

    val tsData = getTsData(indexArgs, tsIn, false)

    val model = getPreProcessor.process(tsData, null)

    // results can be retrieved directly or using the usual dictionary (generic approach)
    val forecast = model.forecast(forecastAdj, false)

    Vectors.dense(if (regArimaProcessingArgs.full == true) tsIn ++ forecast.internalStorage() else forecast.internalStorage())
  }

  def setRegArimaProcessingArgs(nf : Int, full : Boolean) = {

    forecastAdj = nf

    regArimaProcessingArgs = RegArimaProcessingArgs(full)
  }


  private def getPreProcessor : IPreprocessor = {

    val spec = RegArimaSpecification.RG5.clone

    spec.getOutliers.remove(OutlierType.TC)

    spec.build

  }


}
