package uk.gov.ons.JDemetra.processors

import java.net.URL

import ec.satoolkit.algorithm.implementation.TramoSeatsProcessingFactory
import ec.satoolkit.tramoseats.TramoSeatsSpecification
import ec.tstoolkit.modelling.arima.tramo.TramoSpecification
import ec.tstoolkit.timeseries.simplets.TsData
import org.apache.spark.mllib.linalg.{Vector, Vectors}


trait TramoSeats extends JDFunction{

  /*
  def getTSSpecFileURL(s : String) : URL = {
    new URL(SPEC_FILE_LOC + "tramoSeats" + "/" + s + XML)
  }
*/

  def getTSSpecFileURL(s : String) : URL = {
    new URL(specFileLocation + "tramoSeats" + "/" + s + XML)
  }

  // Indicates the Default Specification name
  val DEF_RSA0 = "DEF_RSA0"
  val DEF_RSA1 = "DEF_RSA1"
  val DEF_RSA2 = "DEF_RSA2"
  val DEF_RSA3 = "DEF_RSA3"
  val DEF_RSA4 = "DEF_RSA4"
  val DEF_RSA5 = "DEF_RSA5"
  val DEF_RSAfull = "DEF_RSAfull"

  val DEF_TR0 = "DEF_TR0"
  val DEF_TR1 = "DEF_TR1"
  val DEF_TR2 = "DEF_TR2"
  val DEF_TR3 = "DEF_TR3"
  val DEF_TR4 = "DEF_TR4"
  val DEF_TR5 = "DEF_TR5"
  val DEF_TRfull = "DEF_TRfull"

  // Indicates the file name of the XML Specification
  val RSA0 = "RSA0"
  val RSA1 = "RSA1"
  val RSA2 = "RSA2"
  val RSA3 = "RSA3"
  val RSA4 = "RSA4"
  val RSA5 = "RSA5"
  val RSAfull = "RSAfull"

  val TR0 = "TR0"
  val TR1 = "TR1"
  val TR2 = "TR2"
  val TR3 = "TR3"
  val TR4 = "TR4"
  val TR5 = "TR5"
  val TRfull = "TRfull"

  val OutlierDelim = "<>"

  // A Set of the valid Specs for this processor
  val tsSpecs : Set[String] = Set(DEF_RSA0, DEF_RSA1, DEF_RSA2, DEF_RSA3, DEF_RSA4, DEF_RSA5, DEF_RSAfull,
                                  DEF_TR0, DEF_TR1, DEF_TR2, DEF_TR3, DEF_TR4, DEF_TR5, DEF_TRfull,
                                  RSA0, RSA1, RSA2, RSA3, RSA4, RSA5, RSAfull,
                                  TR0, TR1, TR2, TR3, TR4, TR5, TRfull)

  val TS_DefaultSpec : String = DEF_RSAfull

  private case class TSProcessArgs(spec : Option[String] = Some(TS_DefaultSpec), predictionLength : Option[Int] = None,
                                   outliersCriticalValue : Option[Double] = None, benchFlag : Option[Boolean] = None)

  private var tsProcessingArgs = TSProcessArgs()

  def getTramoSeatsSpec : TramoSeatsSpecification = {

    tsProcessingArgs.spec.getOrElse(TS_DefaultSpec) match {
      // Built in Specs
      case DEF_RSA0 => TramoSeatsSpecification.RSA0.clone();
      case DEF_RSA1 => TramoSeatsSpecification.RSA1.clone();
      case DEF_RSA2 => TramoSeatsSpecification.RSA2.clone();
      case DEF_RSA3 => TramoSeatsSpecification.RSA3.clone();
      case DEF_RSA4 => TramoSeatsSpecification.RSA4.clone();
      case DEF_RSA5 => TramoSeatsSpecification.RSA5.clone();
      case DEF_RSAfull => TramoSeatsSpecification.RSAfull.clone();

      // TramoSeats Spec from a File
      case RSA0 => loadTramoSeatsSpecificationFromURL(getTSSpecFileURL(RSA0));
      case RSA1 => loadTramoSeatsSpecificationFromURL(getTSSpecFileURL(RSA1));
      case RSA2 => loadTramoSeatsSpecificationFromURL(getTSSpecFileURL(RSA2));
      case RSA3 => loadTramoSeatsSpecificationFromURL(getTSSpecFileURL(RSA3));
      case RSA4 => loadTramoSeatsSpecificationFromURL(getTSSpecFileURL(RSA4));
      case RSA5 => loadTramoSeatsSpecificationFromURL(getTSSpecFileURL(RSA5));
      case RSAfull => loadTramoSeatsSpecificationFromURL(getTSSpecFileURL(RSAfull));

      // Default is to parse user input as XML
      case default => loadTramoSeatsSpecificationFromXML(default)}
  }

  def getTramoSpec : TramoSpecification = {

     tsProcessingArgs.spec.getOrElse(DEF_TRfull) match { // Built in Specs
                                  case DEF_TR0 => TramoSpecification.TR0.clone();
                                  case DEF_TR1 => TramoSpecification.TR1.clone();
                                  case DEF_TR2 => TramoSpecification.TR2.clone();
                                  case DEF_TR3 => TramoSpecification.TR3.clone();
                                  case DEF_TR4 => TramoSpecification.TR4.clone();
                                  case DEF_TR5 => TramoSpecification.TR5.clone();
                                  case DEF_TRfull => TramoSpecification.TRfull.clone();

                                   // Tramo Spec from a File
                                  case TR0 => loadTramoSpecificationFromURL(getTSSpecFileURL(TR0));
                                  case TR1 => loadTramoSpecificationFromURL(getTSSpecFileURL(TR1));
                                  case TR2 => loadTramoSpecificationFromURL(getTSSpecFileURL(TR2));
                                  case TR3 => loadTramoSpecificationFromURL(getTSSpecFileURL(TR3));
                                  case TR4 => loadTramoSpecificationFromURL(getTSSpecFileURL(TR4));
                                  case TR5 => loadTramoSpecificationFromURL(getTSSpecFileURL(TR5));
                                  case TRfull => loadTramoSpecificationFromURL(getTSSpecFileURL(TRfull));

                                  // Default is to parse user input as XML
                                  case default => loadTramoSpecificationFromXML(default)}
  }

  def setTSProcessingArgs(spec : Option[String] = Some(TS_DefaultSpec), predLength : Option[Int] = None, outliersCriticalVal : Option[Double] = None,
                          benchFlag : Option[Boolean] = None) : Unit = {

    val s = spec.get

    tsProcessingArgs = if (tsSpecs.contains(s) || s.contains("<trs:TramoSeatsSpecification") || s.contains("<trs:TramoSpecification")) TSProcessArgs(spec, predLength, outliersCriticalVal, benchFlag)
                       else throw new Exception("Spec " + s + " not valid for TramoSeats")
  }

  /*
   Modify the TramoSeatsSpecification by adjusting any user supplied Arguments for the function requested
 */
  def addUserSuppliedArguments(spec : TramoSeatsSpecification) : TramoSeatsSpecification = {

    tsProcessingArgs.predictionLength match {case Some(pLen) => spec.getSeatsSpecification.setPredictionLength(pLen);
                                             case None => }

    tsProcessingArgs.outliersCriticalValue match {case Some(oCV) => addUserSuppliedArguments(spec.getTramoSpecification);
                                                  case None => }

    tsProcessingArgs.benchFlag match {case Some(bF) => spec.getBenchmarkingSpecification.setEnabled(bF);
                                      case None => }

    // debug remove later
    // saveTramoSeatsSpecification(spec, "TS_Debug")

    spec
  }

  /*
    Modify the TramoSpecification by adjusting any user supplied Arguments for the function requested
   */
  def addUserSuppliedArguments(spec : TramoSpecification) : TramoSpecification = {

    tsProcessingArgs.outliersCriticalValue match {case Some(oCV) => spec.getOutliers.setCriticalValue(oCV);
    case None => }

    // debug remove later
    // saveTramoSpecification(spec, "T_Debug")

    spec
  }

  def forecastTS(vectorIn: Vector) : Vector = {

    val tsIn = vectorIn.toArray

    val tsData = getTsData(indexArgs, tsIn, false)

    val spec = addUserSuppliedArguments(getTramoSeatsSpec)

    val myrslts = TramoSeatsProcessingFactory.process(tsData, spec)

    val res = tsProcessingArgs.predictionLength match {case Some(pLen) => myrslts.getData(getFcast(pLen), classOf[TsData]);
                                                       case None => throw new Exception("Tramo Seats prediction length not set")}

    // ********* THIS NEEDS LOOKING AT
    Vectors.dense(res.internalStorage())
  }



  def getOutliers(vectorIn: Vector) : String = {

    val tsIn = vectorIn.toArray

    val tsData = getTsData(indexArgs, tsIn, false)

    val spec = addUserSuppliedArguments(getTramoSpec)

    val preprocessor = spec.build

    val model = preprocessor.process(tsData, null)
    val outliers = model.outliersEstimation(true, false)

    var res = ""

    for (o <- outliers) res += o + OutlierDelim

    res
  }

}


