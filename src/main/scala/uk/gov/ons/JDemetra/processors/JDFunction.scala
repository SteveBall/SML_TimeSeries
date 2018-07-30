package uk.gov.ons.JDemetra.processors

import ec.tstoolkit.timeseries.simplets.TsFrequency

trait JDFunction {


  var indexArgs : IndexArgs = IndexArgs(TsFrequency.Monthly, 0, 0)

  var forecastAdj : Int = 0

}