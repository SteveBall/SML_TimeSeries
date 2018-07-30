package uk.gov.ons.JDemetra.processors

import ec.satoolkit.algorithm.implementation.X13ProcessingFactory
import ec.satoolkit.x13.X13Specification
import ec.tstoolkit.algorithm.{CompositeResults, IProcResults}
import ec.tstoolkit.timeseries.simplets.TsData
import org.apache.spark.mllib.linalg.{Vector, Vectors}

trait X13 extends JDFunction{

  private def processX13(vectorIn: Vector) : IProcResults = {
    val tsIn = vectorIn.toArray

    val tsData = getTsData(indexArgs, tsIn, false)

    // Using a pre-defined specification
    val rsa5: X13Specification = X13Specification.RSA5

    // Process
    X13ProcessingFactory.process(tsData, rsa5)
  }

  def seasAdj(vectorIn: Vector) : Vector = {

    Vectors.dense(processX13(vectorIn).getData("sa", classOf[TsData]).internalStorage())
  }


  def trend(vectorIn: Vector) : Vector = {

    Vectors.dense(processX13(vectorIn).getData("t", classOf[TsData]).internalStorage())
  }

}

