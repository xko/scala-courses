course := "reactive"
assignment := "followers"

scalaVersion := "3.0.0"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked"
)

val akkaVersion = "2.6.9"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
  "com.typesafe.akka" %% "akka-stream-typed" % akkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
).map(_.cross(CrossVersion.for3Use2_13))

libraryDependencies += "org.scalameta" %% "munit" % "0.7.26" % Test

testFrameworks += new TestFramework("munit.Framework")
Test / parallelExecution := false
