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


import scala.language.postfixOps
import scala.sys.process._

val DataUrl = "https://moocs.scala-lang.org/~dockermoocs/bigdata/atussum.csv"
val DataFile = file("src/main/resources/timeusage/atussum.csv")

lazy val downloadData = taskKey[Unit](s"Download additional data from $DataUrl")

downloadData := {
  val log = streams.value.log
  log.info(s"Checking $DataFile...")
  if(DataFile.exists()) {
    log.info("...exists - OK")
  } else {
    DataFile.getParentFile.mkdirs()
    log.info(s"...downloading from $DataUrl...")
    url(DataUrl) #> file(DataFile.toString) ! ;
    log.info("...done")
  }
}

compile in Compile := (compile in Compile).dependsOn(downloadData).value