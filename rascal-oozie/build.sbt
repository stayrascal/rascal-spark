
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % Test
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
libraryDependencies += "org.apache.oozie" % "oozie-core" % "4.2.0"

libraryDependencies += "org.scalikejdbc" %% "scalikejdbc" % "3.1.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-pickling" % "0.10.1"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"

scalaVersion := "2.11.8"

fork := true

javaOptions += "-Xmx8G"
javaOptions += "-Xss10m"
