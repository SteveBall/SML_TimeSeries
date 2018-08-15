package uk.gov.ons.JDemetra.processors

import java.net.URL

import ec.satoolkit.algorithm.implementation.X13ProcessingFactory
import ec.satoolkit.x13.X13Specification
import ec.tstoolkit.algorithm.{CompositeResults, IProcResults}
import ec.tstoolkit.timeseries.simplets.TsData
import org.apache.spark.mllib.linalg.{Vector, Vectors}

trait X13 extends JDFunction{

  /*
   def getX13SpecFileURL(s : String) : URL = {
    new URL(SPEC_FILE_LOC + "x13" + "/" + s + XML)
  }
*/

  def getX13SpecFileURL(s : String) : URL = {
    new URL(specFileLocation + "x13" + "/" + s + XML)
  }

  // Indicates the Default Specification name
  val DEF_X13_RSA0 = "DEF_X13_RSA0"
  val DEF_X13_RSA1 = "DEF_X13_RSA1"
  val DEF_X13_RSA2 = "DEF_X13_RSA2"
  val DEF_X13_RSA3 = "DEF_X13_RSA3"
  val DEF_X13_RSA4 = "DEF_X13_RSA4"
  val DEF_X13_RSA5 = "DEF_X13_RSA5"
  val DEF_X13_RSAX11 = "DEF_X13_RSAX11"

  // Indicates the file name of the XML Specification
  val X13_RSA0 = "RSA0"
  val X13_RSA1 = "RSA1"
  val X13_RSA2 = "RSA2"
  val X13_RSA3 = "RSA3"
  val X13_RSA4 = "RSA4"
  val X13_RSA5 = "RSA5"
  val X13_RSAX11 = "RSAX11"

  // A Set of the valid Specs for this processor
  val x13Specs : Set[String] = Set(DEF_X13_RSA0, DEF_X13_RSA1, DEF_X13_RSA2, DEF_X13_RSA3, DEF_X13_RSA4, DEF_X13_RSA5, DEF_X13_RSAX11,
                                   X13_RSA0, X13_RSA1, X13_RSA2, X13_RSA3, X13_RSA4, X13_RSA5, X13_RSAX11)

  val X13_DefaultSpec : String = DEF_X13_RSA5

  private case class X13ProcessingArgs(spec : Option[String] = Some(X13_DefaultSpec))

  private var x13ProcessingArgs = X13ProcessingArgs()

  def getX13Specification : X13Specification = {

    x13ProcessingArgs.spec.getOrElse(X13_DefaultSpec) match {
      // Built in Specs
      case DEF_X13_RSA0 => X13Specification.RSA0.clone();
      case DEF_X13_RSA1 => X13Specification.RSA1.clone();
      case DEF_X13_RSA2 => X13Specification.RSA2.clone();
      case DEF_X13_RSA3 => X13Specification.RSA3.clone();
      case DEF_X13_RSA4 => X13Specification.RSA4.clone();
      case DEF_X13_RSA5 => X13Specification.RSA5.clone();
      case DEF_X13_RSAX11 => X13Specification.RSAX11.clone();

      // RegArima Spec from a File
      case X13_RSA0 => loadX13SpecificationFromURL(getX13SpecFileURL(X13_RSA0));
      case X13_RSA1 => loadX13SpecificationFromURL(getX13SpecFileURL(X13_RSA1));
      case X13_RSA2 => loadX13SpecificationFromURL(getX13SpecFileURL(X13_RSA2));
      case X13_RSA3 => loadX13SpecificationFromURL(getX13SpecFileURL(X13_RSA3));
      case X13_RSA4 => loadX13SpecificationFromURL(getX13SpecFileURL(X13_RSA4));
      case X13_RSA5 => loadX13SpecificationFromURL(getX13SpecFileURL(X13_RSA5));

      // Default is to parse user input as XML
      case default => loadX13SpecificationFromXML(default)}

  }

  def setX13ProcessingArgs(spec : Option[String] = Some(X13_DefaultSpec)) : Unit = {

    val s = spec.get

    x13ProcessingArgs = if (x13Specs.contains(spec.get) || s.contains("<x13:X13Specification")) X13ProcessingArgs(spec)
                        else throw new Exception("Spec " + spec.get + " not valid for X13")
  }

  private def processX13(vectorIn: Vector) : IProcResults = {

    val tsIn = vectorIn.toArray

    val tsData = getTsData(indexArgs, tsIn, false)

    val x13Spec = getX13Specification

    X13ProcessingFactory.process(tsData, x13Spec)
  }

  def seasAdj(vectorIn: Vector) : Vector = {

    Vectors.dense(processX13(vectorIn).getData("sa", classOf[TsData]).internalStorage())
  }


  def trend(vectorIn: Vector) : Vector = {

    Vectors.dense(processX13(vectorIn).getData("t", classOf[TsData]).internalStorage())
  }

}

