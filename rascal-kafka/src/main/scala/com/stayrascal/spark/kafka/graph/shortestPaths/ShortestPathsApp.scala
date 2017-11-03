package com.stayrascal.spark.kafka.graph.shortestPaths

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, GraphLoader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object ShortestPathsApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("usage: <input> <output> <minEdge> <numV>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val spark = SparkSession.builder().appName("Spark ShortestPAth Application").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val output = args(1)
    val minEdge = args(2).toInt
    //    val numVertices = args(3).toInt

    val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)

    val numVertices = graph.vertices.count().toInt
    val edges = sc.textFile(input).map { line =>
      val fields = line.split("::")
      Edge(fields(0).toLong, fields(1).toLong, fields(2).toDouble)
    }

    var numv = 100
    if (numVertices <= 20000) {
      numv = numVertices / 10
    } else {
      numv = 500
    }

    val landmarks = Seq(1, numVertices).map(_.toLong)
    val results = ShortestPaths.run(graph, landmarks).vertices
    results.saveAsTextFile(output)
    sc.stop()
  }


}
