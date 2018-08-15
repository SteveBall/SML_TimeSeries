package uk.gov.ons.timeseries

import java.io.{FileNotFoundException, FileOutputStream, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import ec.demetra.xml.sa.x13.XmlX13Specification
import ec.satoolkit.x13.X13Specification
import javax.xml.bind.{JAXBContext, JAXBException, Marshaller}

import uk.gov.ons.JDemetra.processors._

object X13SpecTests {

  val RSA5_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<x13:X13Specification xmlns=\"ec/tss.core\" xmlns:bench=\"ec/eurostat/jdemetra/benchmarking\" xmlns:tss=\"ec/eurostat/jdemetra/core\" xmlns:x13=\"ec/eurostat/jdemetra/sa/x13\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:modelling=\"ec/eurostat/jdemetra/modelling\" xmlns:sa=\"ec/eurostat/jdemetra/sa\">\n    <x13:Preprocessing>\n        <x13:Transformation>\n            <x13:Auto/>\n        </x13:Transformation>\n        <x13:Calendar>\n            <x13:TradingDays>\n                <x13:Default>\n                    <modelling:TdOption>TradingDays</modelling:TdOption>\n                    <modelling:LpOption>LeapYear</modelling:LpOption>\n                    <x13:AutoAdjust>true</x13:AutoAdjust>\n                </x13:Default>\n                <x13:Test>Remove</x13:Test>\n            </x13:TradingDays>\n            <x13:Easter>\n                <x13:Test>Add</x13:Test>\n            </x13:Easter>\n        </x13:Calendar>\n        <x13:Outliers>\n            <x13:Types>AO TC LS</x13:Types>\n        </x13:Outliers>\n        <x13:AutoModelling>\n            <x13:LjungBoxLimit>0.95</x13:LjungBoxLimit>\n        </x13:AutoModelling>\n    </x13:Preprocessing>\n    <x13:Decomposition>\n        <x13:Mode>Undefined</x13:Mode>\n    </x13:Decomposition>\n</x13:X13Specification>"

  val X13_SPEC_FILE_LOC = SPEC_FILE_LOC + "x13/"

  val X13_RSA0 = "RSA0"
  val X13_RSA1 = "RSA1"
  val X13_RSA2 = "RSA2"
  val X13_RSA3 = "RSA3"
  val X13_RSA4 = "RSA4"
  val X13_RSA5 = "RSA5"
  val X13_RSAX11 = "RSAX11"

  val x13SpecTypes = Map(X13Specification.RSA0 -> X13_RSA0,
      X13Specification.RSA1 -> X13_RSA1,
      X13Specification.RSA2 -> X13_RSA2,
      X13Specification.RSA3 -> X13_RSA3,
      X13Specification.RSA4 -> X13_RSA4,
      X13Specification.RSA5 -> X13_RSA5,
      X13Specification.RSAX11 -> X13_RSAX11
  )

  def saveAllX13Specs(): Unit = {

    val specs = X13Specification.allSpecifications()

    specs.iterator.foreach(f => saveX13Specification(f, x13SpecTypes.getOrElse(f, "NO_X13_Spec")))
  }


  @throws[FileNotFoundException]
  @throws[JAXBException]
  @throws[IOException]
  def saveX13Specification(raSpec : X13Specification, fName: String): Unit = {

    val xspec = new XmlX13Specification
    XmlX13Specification.MARSHALLER.marshal(raSpec, xspec)
    val jaxb = JAXBContext.newInstance(xspec.getClass)
    val ostream = new FileOutputStream(X13_SPEC_FILE_LOC + fName + XML)
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

  def fromXMLString(): Unit = {

    val spec = loadX13SpecificationFromXML(RSA5_XML)

    saveX13Specification(spec, "RSA5_From_String")
  }


  def manipulateSpecs : Unit = {

    saveAllX13Specs

    fromXMLString
  }

  def main(args: Array[String]): Unit = {

    manipulateSpecs
  }

}
