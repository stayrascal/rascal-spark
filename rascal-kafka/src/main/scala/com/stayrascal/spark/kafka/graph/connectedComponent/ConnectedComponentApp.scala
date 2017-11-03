package com.stayrascal.spark.kafka.connectedComponent

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object ConnectedComponentApp {
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      println("usage: <input> <output> <minEdge> <numV>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val spark = SparkSession.builder().appName("Page Rank Data Gen Run").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val output = args(1)
    val minEdge = args(2).toInt

    val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)

    val strongkyRes = graph.stronglyConnectedComponents(3).vertices
    val res = graph.connectedComponents().vertices

    res.saveAsTextFile(output)

    sc.stop()

  }
}
