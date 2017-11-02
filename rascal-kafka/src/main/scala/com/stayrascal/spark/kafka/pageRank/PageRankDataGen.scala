package com.stayrascal.spark.kafka.pageRank

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession

object PageRankDataGen {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println("usage: <output> <numVertices> <numPartitions> <mu> <sigma>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Page Rank Data Gen").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val output = args(0)
    val numVertices = args(1).toInt
    val numPar = args(2).toInt
    val mu = args(3).toDouble
    val sigma = args(4).toDouble

    val graph = GraphGenerators.logNormalGraph(sc, numVertices, numPar, mu, sigma)
    graph.edges.map(s => s.srcId.toString + " " + s.dstId.toString + " " + s.attr.toString).saveAsTextFile(output)

    sc.stop()
  }
}
