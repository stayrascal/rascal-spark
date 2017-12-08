import Dependencies._
import Resolvers._

name := "rascal-spark"
description := "Spark demo"

val playVersion = "2.6.3"

val buildVersion = sys.env.get("GO_PIPELINE_LABEL") match {
  case Some(label) => s"$label"
  case _ => "1.0-SNAPSHOT"
}

val dockerRegistry = sys.env.get("DOCKER_REGISTRY") match {
  case Some(label) => Some(label)
  case _ => Some("docker.io")
}

lazy val common = Seq(
  version := buildVersion,
  scalaVersion := "2.11.8",
  organization := "com.stayrascal",
  dockerRepository := dockerRegistry,
  resolvers ++= resolverSetting
)

lazy val assemblyCommonSettings = Seq(
  version := buildVersion,
  scalaVersion := "2.11.8",
  organization := "com.stayrascal",
  test in assembly := {}
)

lazy val root = project in file(".") aggregate `rascal-data`

lazy val libSettings = common ++ Seq(libraryDependencies ++= commonDeps)

lazy val `rascal-data` = (project in file("rascal-data")).settings(assemblyCommonSettings: _*).settings(
  assemblyJarName in assembly := "rascal-data.jar",
  mainClass in assembly := Some("com.stayrascal.spark"),
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  }
)

lazy val  `rascal-data-provider` = project in file("rascal-data-provider")

lazy val `rascal-kafka` = project in file("rascal-kafka")

lazy val `rascal-oozie` = project in file("rascal-oozie")

libraryDependencies += "com.twitter" % "finagle-core_2.11" % "7.1.0"