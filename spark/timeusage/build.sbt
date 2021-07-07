
course := "bigdata"
assignment := "timeusage"

scalaVersion := "2.12.8"
scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  ("org.apache.spark" %% "spark-core" % "2.4.3"),
  ("org.apache.spark" %% "spark-sql" % "2.4.3")
)
dependencyOverrides ++= Seq(
  ("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7")
)

testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-s")


import sbt.io.Using
import sbt.io.IO._

Compile / resourceGenerators += Def.task {
  val log = streams.value.log
  val DataUrl = url("https://moocs.scala-lang.org/~dockermoocs/bigdata/atussum.csv")
  val DataFile = (Compile / resourceDirectory).value / "timeusage" / "atussum.csv"
  if(!DataFile.exists()){
    log.info(s"Downloading $DataUrl")
    Using.urlInputStream(DataUrl){ in => transfer( in, DataFile)}
    log.info("...done")
  }
  Seq(DataFile)
}.taskValue
