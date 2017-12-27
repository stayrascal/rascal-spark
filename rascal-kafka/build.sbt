incOptions := incOptions.value.withNameHashing(true).withRecompileOnMacroDef(false)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.4" % Test
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.0"
libraryDependencies += "org.scalikejdbc" %% "scalikejdbc" % "3.1.0"
libraryDependencies += "org.scala-lang.modules" %% "scala-pickling" % "0.10.1"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"
libraryDependencies += "org.eclipse.jetty" % "jetty-server" % "9.4.7.RC0"
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.6"

scalaVersion := "2.11.8"

fork := true

javaOptions += "-Xmx8G"
javaOptions += "-Xss10m"
