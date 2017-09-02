import play.sbt.PlayImport._
import sbt._

object Dependencies {
  // Version
  lazy val scalaModulesVersion = "1.0.6"

  //Libraries
  val combinator = "org.scala-lang.modules" %% "scala-parser-combinators_2.11" % scalaModulesVersion
  val xml = "org.scala-lang.modules" %% "scala-xml" % scalaModulesVersion
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.4"
  val mockito = "org.mockito" %% "mockito-core" % "2.9.0"
  val playSlick = "com.typesafe.play" %% "play-slick" % "3.0.1"
  val playSlickEvolution = "com.typesafe.play" %% "play-slick-evolutions" % "3.0.1"
  val akka = "com.typesafe.akka" %% "akka-testkit" % "2.5.4"

  // Projects
  val commonDeps = Seq(combinator, xml, scalaTest % Test, mockito % Test, akka)

  val commonDepsWithPlay = commonDeps ++ Seq(
    component("play"),
    ws
  )

  val playDeps = commonDeps ++ Seq(
    cache,
    ws,
    filters,
    "mysql" % "mysql-connector-java" % "5.1.18",
    "com.h2database" % "h2" % "1.4.177",
    playSlick, playSlickEvolution,
    specs2,
    "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test
  )

}
