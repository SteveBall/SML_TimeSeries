# SML_TimeSeries

This repo shows how we can provide a Time Series Analytical capability in an Apache Spark environment

It extends the excellent [spark-ts](https://github.com/sryza/spark-timeseries) library build at Cloudera by Sandy Ryza but replaces its mathematical models with those contained in the [JDemetra+](https://github.com/jdemetra) software package.

It uses the TimeSeriesRDD components from spark-ts to provide data partitioning and distributed processing capability (Scala Spark) whilst using the mathematical models from the JDemetra+ tstoolkit library (Java)



