course := "capstone"
assignment := "observatory"

scalaVersion := "2.12.8"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-Xlint",
)

javaOptions += "-Xmx7G"

libraryDependencies ++= Seq(
  "com.sksamuel.scrimage" %% "scrimage-core" % "2.1.8", // for visualization
  // You don’t *have to* use Spark, but in case you want to, we have added the dependency
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  // You don’t *have to* use akka-stream, but in case you want to, we have added the dependency
  "com.typesafe.akka" %% "akka-stream" % "2.6.0",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.6.0" % Test,
  // You don’t *have to* use Monix, but in case you want to, we have added the dependency
  "io.monix" %% "monix" % "2.3.3",
  // You don’t *have to* use fs2, but in case you want to, we have added the dependency
  "co.fs2" %% "fs2-io" % "1.0.5",
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test",
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % "1.3.0"
)

testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-s")

parallelExecution in Test := false // So that tests are executed for each milestone, one after the other
