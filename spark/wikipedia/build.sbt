course := "bigdata"
assignment := "wikipedia"

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

import scala.language.postfixOps
import scala.sys.process._

val DataUrl = "https://moocs.scala-lang.org/~dockermoocs/bigdata/wikipedia.dat"
val DataFile = file("src/main/resources/wikipedia/wikipedia.dat")

lazy val downloadData = taskKey[Unit](s"Download additional data from $DataUrl")

downloadData := {
  if(DataFile.exists()) {
    println("Data exists, no need to download.")
  } else {
    DataFile.getParentFile.mkdirs()
    println(s"Downloading data from $DataUrl")
    url(DataUrl) #> file(DataFile.toString) !
  }
}

compile in Compile := (compile in Compile).dependsOn(downloadData).value