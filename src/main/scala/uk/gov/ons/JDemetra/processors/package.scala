package uk.gov.ons.JDemetra

import java.io._
import java.net.URL
import java.nio.charset.StandardCharsets

import com.cloudera.sparkts.{DateTimeIndex, UniformDateTimeIndex}
import ec.demetra.xml.sa.tramoseats.{XmlTramoSeatsSpecification, XmlTramoSpecification}
import ec.tstoolkit.modelling.arima.x13.RegArimaSpecification

import scala.runtime.ScalaRunTime.stringOf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import ec.tstoolkit.timeseries.simplets.{TsData, TsFrequency}
import javax.xml.bind.{JAXBContext, JAXBException, Marshaller, Unmarshaller}

import scala.annotation.switch
import ec.demetra.xml.sa.x13.{XmlRegArimaSpecification, XmlX13Specification}
import ec.satoolkit.tramoseats.TramoSeatsSpecification
import ec.satoolkit.x13.X13Specification
import ec.tstoolkit.modelling.arima.tramo.TramoSpecification



package object processors {



  //
  // --------------------------------- JDemetra+ Data Results helpers ----------------------------------
  //

  val SA = "sa"

  def getFcast(fCast : Int) : String = "fcasts(" + fCast + ")"
  def getBcast(bCast : Int) : String = "bcasts(" + bCast + ")"


  //
  // --------------------------------- Spark-ts  -->  JDemetra+ Data functions ----------------------------------
  //

  def getTsData(indexArgs : IndexArgs, tsIn : Array[Double], copyFlag : Boolean) : TsData = {
    new TsData(indexArgs.tsFreq, indexArgs.startYear, indexArgs.startPeriod, tsIn, copyFlag)
  }

  def getTsData(indexArgs : IndexArgs, vectorIn : Vector, copyFlag : Boolean) : TsData = {
    getTsData(indexArgs, vectorIn.toArray, copyFlag)
  }

  //
  // --------------------------------- Spark-ts  -->  JDemetra+ Time functions ----------------------------------
  //

  case class IndexArgs(tsFreq : TsFrequency, startYear : Int, startPeriod : Int)

  val freqMap = Map("com.cloudera.sparkts.MonthFrequency" -> TsFrequency.Monthly,
    "com.cloudera.sparkts.YearFrequency" -> TsFrequency.Yearly)

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


  //
  // --------------------------------- Common Spec Functions ----------------------------------
  //


  def getInputStreamReader(specFile : String) : InputStreamReader = {

    val istream = new FileInputStream(specFile)
    new InputStreamReader(istream, StandardCharsets.UTF_8)
  }

  def getInputStreamReader(url : URL) : InputStreamReader = {

    new InputStreamReader(url.openStream(), StandardCharsets.UTF_8)
  }

  //
  // --------------------------------- RegArimaSpecification ----------------------------------
  //

  def loadRegArimaSpecificationFromURL(regArimaSpecFileURL: URL): RegArimaSpecification = {

    loadRegArimaSpecification(getInputStreamReader(regArimaSpecFileURL))
  }

  @throws[FileNotFoundException]
  @throws[IOException]
  def loadRegArimaSpecificationFromFile(regArimaSpecFile: String): RegArimaSpecification = {

    val istream = new FileInputStream(regArimaSpecFile)
    val reader = new InputStreamReader(istream, StandardCharsets.UTF_8)

    loadRegArimaSpecification(reader)
  }

  def loadRegArimaSpecificationFromXML(xml: String): RegArimaSpecification = {

    loadRegArimaSpecification(new StringReader(xml))
  }

  @throws[JAXBException]
  def loadRegArimaSpecification(reader: Reader): RegArimaSpecification = {

    val xspec = new XmlRegArimaSpecification
    val jaxb = JAXBContext.newInstance(xspec.getClass)
    val nspec = new RegArimaSpecification

      try {
        val unmarshaller = jaxb.createUnmarshaller
        var rslt = unmarshaller.unmarshal(reader).asInstanceOf[XmlRegArimaSpecification]

        XmlRegArimaSpecification.UNMARSHALLER.unmarshal(rslt, nspec)
        //   assertTrue(spec == nspec)
      } finally if (reader != null) reader.close()

    nspec
  }



  //
  // --------------------------------- TramoSeatsSpecification ----------------------------------
  //

  def loadTramoSeatsSpecificationFromURL(tramoSeatsSpecFileURL: URL): TramoSeatsSpecification = {

    loadTramoSeatsSpecification(getInputStreamReader(tramoSeatsSpecFileURL))
  }

  @throws[FileNotFoundException]
  @throws[IOException]
  def loadTramoSeatsSpecificationFromFile(tramoSeatsSpecFile: String): TramoSeatsSpecification = {

    loadTramoSeatsSpecification(getInputStreamReader(tramoSeatsSpecFile))
  }

  def loadTramoSeatsSpecificationFromXML(xml: String): TramoSeatsSpecification = {

    loadTramoSeatsSpecification(new StringReader(xml))
  }

  @throws[JAXBException]
  def loadTramoSeatsSpecification(reader: Reader): TramoSeatsSpecification = {

    val xspec = new XmlTramoSeatsSpecification
    val jaxb = JAXBContext.newInstance(xspec.getClass)
    val nspec = new TramoSeatsSpecification

    try {
      val unmarshaller = jaxb.createUnmarshaller
      var rslt = unmarshaller.unmarshal(reader).asInstanceOf[XmlTramoSeatsSpecification]

      XmlTramoSeatsSpecification.UNMARSHALLER.unmarshal(rslt, nspec)
      //   assertTrue(spec == nspec)
    } finally if (reader != null) reader.close()

    nspec
  }

  //
  // --------------------------------- TramoSpecification ----------------------------------
  //

  def loadTramoSpecificationFromURL(tramoSpecFileURL: URL): TramoSpecification = {

    loadTramoSpecification(getInputStreamReader(tramoSpecFileURL))
  }

  @throws[FileNotFoundException]
  @throws[IOException]
  def loadTramoSpecificationFromFile(tramoSpecFile: String): TramoSpecification = {

    loadTramoSpecification(getInputStreamReader(tramoSpecFile))
  }

  def loadTramoSpecificationFromXML(xml: String): TramoSpecification = {

    loadTramoSpecification(new StringReader(xml))
  }

  @throws[JAXBException]
  def loadTramoSpecification(reader: Reader): TramoSpecification = {

    val xspec = new XmlTramoSpecification
    val jaxb = JAXBContext.newInstance(xspec.getClass)
    val nspec = new TramoSpecification

    try {
      val unmarshaller = jaxb.createUnmarshaller
      var rslt = unmarshaller.unmarshal(reader).asInstanceOf[XmlTramoSpecification]

      XmlTramoSpecification.UNMARSHALLER.unmarshal(rslt, nspec)
      //   assertTrue(spec == nspec)
    } finally if (reader != null) reader.close()

    nspec
  }

  //
  // --------------------------------- X13Specification ----------------------------------
  //

  def loadX13SpecificationFromURL(x13SpecFileURL: URL): X13Specification = {

    loadX13Specification(getInputStreamReader(x13SpecFileURL))
  }

  @throws[FileNotFoundException]
  @throws[IOException]
  def loadX13SpecificationFromFile(x13SpecFile: String): X13Specification = {

    loadX13Specification(getInputStreamReader(x13SpecFile))
  }

  def loadX13SpecificationFromXML(xml: String): X13Specification = {

    loadX13Specification(new StringReader(xml))
  }

  @throws[JAXBException]
  def loadX13Specification(reader: Reader): X13Specification = {

    val xspec = new XmlX13Specification
    val jaxb = JAXBContext.newInstance(xspec.getClass)
    val nspec = new X13Specification

    try {
      val unmarshaller = jaxb.createUnmarshaller
      var rslt = unmarshaller.unmarshal(reader).asInstanceOf[XmlX13Specification]

      XmlX13Specification.UNMARSHALLER.unmarshal(rslt, nspec)
      //   assertTrue(spec == nspec)
    } finally if (reader != null) reader.close()

    nspec
  }
  //
  // --------------------------------- Debug Helper functions ----------------------------------
  //

  def printVector(vectorIn : Vector) = {
    printArray(vectorIn.toArray)
  }

  def printArray(arr : Array[Double]) = {
    val str = stringOf(arr)
    System.out.println(str)
  }


  val TS_SPEC_FILE_LOC = "../SML_TimeSeries/src/main/resources/tramoSeats/"
  val XML = ".xml"

  def saveTramoSeatsSpecification(tsSpec : TramoSeatsSpecification, fName: String): Unit = {

    val xspec = new XmlTramoSeatsSpecification
    XmlTramoSeatsSpecification.MARSHALLER.marshal(tsSpec, xspec)
    val jaxb = JAXBContext.newInstance(xspec.getClass)
    val ostream = new FileOutputStream(TS_SPEC_FILE_LOC + fName + XML)
    try {
      val writer = new OutputStreamWriter(ostream, StandardCharsets.UTF_8)
      try {
        val marshaller = jaxb.createMarshaller
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true)
        marshaller.marshal(xspec, writer)
        writer.flush()
      } finally if (writer != null) writer.close()
    }
  }

  def saveTramoSpecification(tSpec : TramoSpecification, fName: String): Unit = {

    val xspec = new XmlTramoSpecification
    XmlTramoSpecification.MARSHALLER.marshal(tSpec, xspec)
    val jaxb = JAXBContext.newInstance(xspec.getClass)
    val ostream = new FileOutputStream(TS_SPEC_FILE_LOC + fName + XML)
    try {
      val writer = new OutputStreamWriter(ostream, StandardCharsets.UTF_8)
      try {
        val marshaller = jaxb.createMarshaller
        marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true)
        marshaller.marshal(xspec, writer)
        writer.flush()
      } finally if (writer != null) writer.close()
    }
  }
}

