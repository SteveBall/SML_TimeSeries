package uk.gov.ons.timeseries

import java.io.{FileNotFoundException, FileOutputStream, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import ec.benchmarking.simplets.TsCholette
import ec.demetra.xml.sa.tramoseats.{XmlTramoSeatsSpecification, XmlTramoSpecification}
import ec.satoolkit.benchmarking.SaBenchmarkingSpec
import ec.satoolkit.seats.SeatsSpecification
import ec.satoolkit.tramoseats.TramoSeatsSpecification
import ec.tstoolkit.modelling.arima.tramo.TramoSpecification
import ec.tstoolkit.timeseries.regression.OutlierType
import javax.xml.bind.{JAXBContext, JAXBException, Marshaller}

object TramoSeatsSpecTests {

  val TS_SPEC_FILE_LOC = SPEC_FILE_LOC + "tramoSeats/"

  val RSA0 = "RSA0"
  val RSA1 = "RSA1"
  val RSA2 = "RSA2"
  val RSA3 = "RSA3"
  val RSA4 = "RSA4"
  val RSA5 = "RSA5"
  val RSAfull = "RSAfull"

  val tsSpecTypes = Map(TramoSeatsSpecification.RSA0 -> RSA0,
                        TramoSeatsSpecification.RSA1 -> RSA1,
                        TramoSeatsSpecification.RSA2 -> RSA2,
                        TramoSeatsSpecification.RSA3 -> RSA3,
                        TramoSeatsSpecification.RSA4 -> RSA4,
                        TramoSeatsSpecification.RSA5 -> RSA5,
                        TramoSeatsSpecification.RSAfull -> RSAfull)

  val TR0 = "TR0"
  val TR1 = "TR1"
  val TR2 = "TR2"
  val TR3 = "TR3"
  val TR4 = "TR4"
  val TR5 = "TR5"
  val TRfull = "TRfull"

  val tSpecTypes = Map( TramoSpecification.TR0 -> TR0,
                        TramoSpecification.TR1 -> TR1,
                        TramoSpecification.TR2 -> TR2,
                        TramoSpecification.TR3 -> TR3,
                        TramoSpecification.TR4 -> TR4,
                        TramoSpecification.TR5 -> TR5,
                        TramoSpecification.TRfull -> TRfull)

  val RSA_Full_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<trs:TramoSeatsSpecification xmlns=\"ec/tss.core\" xmlns:bench=\"ec/eurostat/jdemetra/benchmarking\" xmlns:trs=\"ec/eurostat/jdemetra/sa/tramoseats\" xmlns:tss=\"ec/eurostat/jdemetra/core\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:modelling=\"ec/eurostat/jdemetra/modelling\" xmlns:sa=\"ec/eurostat/jdemetra/sa\">\n    <trs:Preprocessing>\n        <trs:Transformation>\n            <trs:Auto/>\n        </trs:Transformation>\n        <trs:Calendar>\n            <trs:TradingDays>\n                <trs:Automatic>\n                    <trs:FTest ftest=\"0.01\"/>\n                </trs:Automatic>\n            </trs:TradingDays>\n            <trs:Easter>\n                <trs:Test>true</trs:Test>\n            </trs:Easter>\n        </trs:Calendar>\n        <trs:Outliers>\n            <trs:Types>AO TC LS</trs:Types>\n        </trs:Outliers>\n        <trs:AutoModelling/>\n    </trs:Preprocessing>\n    <trs:Decomposition/>\n</trs:TramoSeatsSpecification>"
  val TR4_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n<trs:TramoSpecification xmlns=\"ec/tss.core\" xmlns:trs=\"ec/eurostat/jdemetra/sa/tramoseats\" xmlns:tss=\"ec/eurostat/jdemetra/core\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:modelling=\"ec/eurostat/jdemetra/modelling\" xmlns:sa=\"ec/eurostat/jdemetra/sa\">\n    <trs:Transformation>\n        <trs:Auto/>\n    </trs:Transformation>\n    <trs:Calendar>\n        <trs:TradingDays>\n            <trs:Default>\n                <modelling:TdOption>WorkingDays</modelling:TdOption>\n                <modelling:LpOption>LeapYear</modelling:LpOption>\n                <trs:Test>T</trs:Test>\n            </trs:Default>\n        </trs:TradingDays>\n        <trs:Easter option=\"Standard\">\n            <trs:Test>true</trs:Test>\n        </trs:Easter>\n    </trs:Calendar>\n    <trs:Outliers>\n        <trs:Types>AO TC LS</trs:Types>\n    </trs:Outliers>\n    <trs:AutoModelling/>\n</trs:TramoSpecification>"

  def tsForecast(spec : String, numPeriods : Int, ocv : Double) : Unit = {

    val onsRDD = getONSTimeSeriesRDD

    val fcastRDD = onsRDD.forecastTramoSeats(spec, numPeriods, Option(ocv))

    fcastRDD foreach println

  }

  def tsForecastAllSpecTypes : Unit = {

    val numPeriods = 6
    val ocv = 2.0

    val specs = Array[String]("DEF_RSAfull", RSAfull, RSA_Full_XML)

    specs.iterator.foreach(s => tsForecast(s, numPeriods, ocv))

  }

  def saveAllTramoSeatsSpecs : Unit = {

    val specs = TramoSeatsSpecification.allSpecifications()

    specs.iterator.foreach(f => saveTramoSeatsSpecification(f, tsSpecTypes.getOrElse(f, "NO_TS_Spec")))
  }

  def saveAllTramoSpecs : Unit = {

    val specs = TramoSpecification.allSpecifications()

    specs.iterator.foreach(f => saveTramoSpecification(f, tSpecTypes.getOrElse(f, "NO_T_Spec")))
  }


  @throws[FileNotFoundException]
  @throws[JAXBException]
  @throws[IOException]
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

  @throws[FileNotFoundException]
  @throws[JAXBException]
  @throws[IOException]
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


  def setTramoSeatsAdjustments(): Unit = {

    val rsafull = TramoSeatsSpecification.RSAfull.clone

    rsafull.getSeatsSpecification.setPredictionLength(6666666)                                                           // Not Set in XML
    rsafull.getSeatsSpecification.setApproximationMode(SeatsSpecification.ApproximationMode.Noisy)
    //   rsafull.getSeatsSpecification.setArima(SarimaComponent)
    rsafull.getSeatsSpecification.setLog(true)
    rsafull.getSeatsSpecification.setMethod(SeatsSpecification.EstimationMethod.McElroyMatrix)
    rsafull.getSeatsSpecification.setSeasBoundary(0.0)
    rsafull.getSeatsSpecification.setSeasBoundary1(1.0)
    rsafull.getSeatsSpecification.setSeasTolerance(10.0)
    rsafull.getSeatsSpecification.setTrendBoundary(1.0)
    rsafull.getSeatsSpecification.setXlBoundary(0.9)

    rsafull.getTramoSpecification.getOutliers.setCriticalValue(2.0)
    rsafull.getTramoSpecification.getOutliers.setDeltaTC(0.3)
    rsafull.getTramoSpecification.getOutliers.setAIO(5)                                                                  // Not Set in XML
    rsafull.getTramoSpecification.getOutliers.setEML(true)
    //   rsafull.getTramoSpecification.getOutliers.setSpan(TsPeriodSelector.DEF_BEG)
    rsafull.getTramoSpecification.getOutliers.setTypes(Array(OutlierType.AO, OutlierType.IO, OutlierType.LS))

    rsafull.getBenchmarkingSpecification.setEnabled(true)
    rsafull.getBenchmarkingSpecification.setBias(TsCholette.BiasCorrection.Additive)
    rsafull.getBenchmarkingSpecification.setLambda(4.0)
    rsafull.getBenchmarkingSpecification.setRho(4.1)
    rsafull.getBenchmarkingSpecification.setTarget(SaBenchmarkingSpec.Target.CalendarAdjusted)

    saveTramoSeatsSpecification(rsafull, "RSAfull_Adjusted")
  }

  def detectOutliers : Unit = {

    val onsRDD = getONSTimeSeriesRDD

    val outliers = onsRDD detectOutliers onsRDD.TR4

    outliers.collect().foreach(println)
  }

  def manipulateSpecs : Unit = {

    saveAllTramoSeatsSpecs

    saveAllTramoSpecs

    setTramoSeatsAdjustments
  }

  def manipulateTS : Unit = {

    tsForecastAllSpecTypes

    detectOutliers
  }

  def scratch : Unit = {

  //  val onsRDD = getONSTimeSeriesRDD

  }

  def main(args: Array[String]): Unit = {

   // manipulateSpecs
   // manipulateTS


   //  scratch
  }
}


