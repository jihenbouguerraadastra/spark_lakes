name := "spark"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.hudi/hudi-spark
libraryDependencies += "org.apache.hudi" %% "hudi-spark" % "0.6.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
// https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-api
libraryDependencies += "org.apache.iceberg" % "iceberg-api" % "0.9.1"
// https://mvnrepository.com/artifact/io.delta/delta-core
libraryDependencies += "io.delta" %% "delta-core" % "0.6.1"

