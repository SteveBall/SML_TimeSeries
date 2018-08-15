package uk.gov.ons.timeseries

import java.io.{FileNotFoundException, FileOutputStream, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import ec.demetra.xml.sa.x13.XmlRegArimaSpecification
import ec.tstoolkit.modelling.arima.x13.RegArimaSpecification
import ec.tstoolkit.timeseries.regression.OutlierType
import javax.xml.bind.{JAXBContext, JAXBException, Marshaller}

import uk.gov.ons.JDemetra.processors._

object RegArimaSpecTests {

  val RA_SPEC_FILE_LOC = SPEC_FILE_LOC + "regArima/"

  val DEF_RG0 = "DEF_RG0"
  val DEF_RG1 = "DEF_RG1"
  val DEF_RG2 = "DEF_RG2"
  val DEF_RG3 = "DEF_RG3"
  val DEF_RG4 = "DEF_RG4"
  val DEF_RG5 = "DEF_RG5"

  val RG0 = "RG0"
  val RG1 = "RG1"
  val RG2 = "RG2"
  val RG3 = "RG3"
  val RG4 = "RG4"
  val RG5 = "RG5"
  val RGDISABLED = "RGDisabled"

  val raSpecTypes = Map(RegArimaSpecification.RG0 -> RG0,
                        RegArimaSpecification.RG1 -> RG1,
                        RegArimaSpecification.RG2 -> RG2,
                        RegArimaSpecification.RG3 -> RG3,
                        RegArimaSpecification.RG4 -> RG4,
                        RegArimaSpecification.RG5 -> RG5,
                        RegArimaSpecification.RGDISABLED -> RGDISABLED)



  val RG5_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<x13:RegArimaSpecification xmlns=\"ec/tss.core\" xmlns:tss=\"ec/eurostat/jdemetra/core\" xmlns:x13=\"ec/eurostat/jdemetra/sa/x13\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:modelling=\"ec/eurostat/jdemetra/modelling\" xmlns:sa=\"ec/eurostat/jdemetra/sa\">\n    <x13:Transformation>\n        <x13:Auto/>\n    </x13:Transformation>\n    <x13:Calendar>\n        <x13:TradingDays>\n            <x13:Default>\n                <modelling:TdOption>TradingDays</modelling:TdOption>\n                <modelling:LpOption>LeapYear</modelling:LpOption>\n                <x13:AutoAdjust>true</x13:AutoAdjust>\n            </x13:Default>\n            <x13:Test>Remove</x13:Test>\n        </x13:TradingDays>\n        <x13:Easter>\n            <x13:Test>Add</x13:Test>\n        </x13:Easter>\n    </x13:Calendar>\n    <x13:Outliers>\n        <x13:Types>AO TC LS</x13:Types>\n    </x13:Outliers>\n    <x13:AutoModelling>\n        <x13:LjungBoxLimit>0.95</x13:LjungBoxLimit>\n    </x13:AutoModelling>\n</x13:RegArimaSpecification>"


  def saveAllRegArimaSpecs(): Unit = {

    val specs = RegArimaSpecification.allSpecifications()

    specs.iterator.foreach(f => saveRegArimaSpecification(f, raSpecTypes.getOrElse(f, "NO_RA_Spec")))
  }



  @throws[FileNotFoundException]
  @throws[JAXBException]
  @throws[IOException]
  def saveRegArimaSpecification(raSpec : RegArimaSpecification, fName: String): Unit = {

    val xspec = new XmlRegArimaSpecification
    XmlRegArimaSpecification.MARSHALLER.marshal(raSpec, xspec)
    val jaxb = JAXBContext.newInstance(xspec.getClass)
    val ostream = new FileOutputStream(RA_SPEC_FILE_LOC + fName + XML)
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



  def removeOutliers(): Unit = {

    val spec = loadRegArimaSpecificationFromFile(RA_SPEC_FILE_LOC + RG5 + XML)

    spec.getOutliers.remove(OutlierType.TC)

    saveRegArimaSpecification(spec, RG5 + "_No_TC")
  }

  def fromXMLString(): Unit = {

    val spec = loadRegArimaSpecificationFromXML(RG5_XML)

    saveRegArimaSpecification(spec, "RG5_From_String")
  }


  def forecast : Unit = {

    val o = getONSTimeSeriesRDD



    val onsRDD = getONSTimeSeriesRDD

    val res = onsRDD forecastRegArimaLite(DEF_RG5, 7 )

    o.collect().foreach(println)
    res.collect().foreach(println)
  }


  def manipulateSpecs : Unit = {

    saveAllRegArimaSpecs

    removeOutliers

    fromXMLString


  }

  def manipulateTS : Unit = {

    forecast
  }

  def main(args: Array[String]): Unit = {


  //  manipulateSpecs
    manipulateTS
  }
}
