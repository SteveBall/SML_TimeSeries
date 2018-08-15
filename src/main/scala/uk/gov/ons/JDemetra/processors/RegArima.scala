package uk.gov.ons.JDemetra.processors

import java.net.URL

import ec.tstoolkit.modelling.arima.x13.RegArimaSpecification
import org.apache.spark.mllib.linalg.{Vector, Vectors}

trait RegArima extends JDFunction{

  /*
   def getRASpecFileURL(s : String) : URL = {
    new URL(SPEC_FILE_LOC + "regArima" + "/" + s + XML)
  }
*/

  def getRASpecFileURL(s : String) : URL = {
    new URL(specFileLocation + "regArima" + "/" + s + XML)
  }

  // Indicates the Default Specification name
  val DEF_RG0 = "DEF_RG0"
  val DEF_RG1 = "DEF_RG1"
  val DEF_RG2 = "DEF_RG2"
  val DEF_RG3 = "DEF_RG3"
  val DEF_RG4 = "DEF_RG4"
  val DEF_RG5 = "DEF_RG5"

  // Indicates the file name of the XML Specification
  val RG0 = "RG0"
  val RG1 = "RG1"
  val RG2 = "RG2"
  val RG3 = "RG3"
  val RG4 = "RG4"
  val RG5 = "RG5"

  // A Set of the valid Specs for this processor
  val raSpecs : Set[String] = Set(DEF_RG0, DEF_RG1, DEF_RG2, DEF_RG3, DEF_RG4, DEF_RG5,
                                  RG0, RG1, RG2, RG3, RG4, RG5)

  val DEF_FORECAST = 3
  val RA_DefaultSpec : String = DEF_RG5

  private case class RegArimaProcessingArgs(spec : Option[String] = Some(RA_DefaultSpec), fullForecast : Option[Boolean] = None, numFcasts : Option[Int] = None)

  private var regArimaProcessingArgs = RegArimaProcessingArgs()

  def getRegArimaSpecification : RegArimaSpecification = {

    regArimaProcessingArgs.spec.getOrElse(RA_DefaultSpec) match {
      // Built in Specs
      case DEF_RG0 => RegArimaSpecification.RG0.clone();
      case DEF_RG1 => RegArimaSpecification.RG1.clone();
      case DEF_RG2 => RegArimaSpecification.RG2.clone();
      case DEF_RG3 => RegArimaSpecification.RG3.clone();
      case DEF_RG4 => RegArimaSpecification.RG4.clone();
      case DEF_RG5 => RegArimaSpecification.RG5.clone();

      // RegArima Spec from a File
      case RG0 => loadRegArimaSpecificationFromURL(getRASpecFileURL(RG0));
      case RG1 => loadRegArimaSpecificationFromURL(getRASpecFileURL(RG1));
      case RG2 => loadRegArimaSpecificationFromURL(getRASpecFileURL(RG2));
      case RG3 => loadRegArimaSpecificationFromURL(getRASpecFileURL(RG3));
      case RG4 => loadRegArimaSpecificationFromURL(getRASpecFileURL(RG4));
      case RG5 => loadRegArimaSpecificationFromURL(getRASpecFileURL(RG5));

      // Default is to parse user input as XML
      case default => loadRegArimaSpecificationFromXML(default)}

  }

  def setRegArimaProcessingArgs(spec : Option[String] = Some(RA_DefaultSpec), fullForecast : Option[Boolean] = None, numFcasts : Option[Int] = None) : Unit = {

    val s = spec.get

    regArimaProcessingArgs = if (raSpecs.contains(s) || s.contains("<x13:RegArimaSpecification")) RegArimaProcessingArgs(spec, fullForecast, numFcasts)
                             else throw new Exception("Spec " + s + " not valid for RegArima")
  }

  def forecastRegArima(vectorIn: Vector) : Vector = {

    val tsIn = vectorIn.toArray

    val tsData = getTsData(indexArgs, tsIn, false)

    val model = getRegArimaSpecification.build.process(tsData, null)

    val forecast = regArimaProcessingArgs.numFcasts match {case Some(nF) => model.forecast(nF, false);
                                                           case None => throw new Exception("Number of forecast periods not set")}

    Vectors.dense(
         regArimaProcessingArgs.fullForecast match {case Some(ff) => if (ff) tsIn ++ forecast.internalStorage()
                                                                     else forecast.internalStorage();
                                                    case None => throw new Exception("Full Forecast flag not set")}
    )

  }

}
