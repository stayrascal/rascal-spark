package com.stayrascal.spark.kafka.pageRank

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PageRankApp {
  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      println("usage: <input> <output> <minEdge> <maxIterations> <tolerance> <resetProb> <StorageLevel>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val spark = SparkSession.builder().appName("Page Rank Data Gen Run").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val output = args(1)
    val minEdge = args(2).toInt
    val maxIterations = args(3).toInt
    val tolerance = args(4).toDouble
    val resetProb = args(5).toDouble
    val storageLevel = args(6)

    val sl: StorageLevel = storageLevel match {
      case "MEMORY_AND_DISK+SER" => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case _ => StorageLevel.MEMORY_ONLY
    }

    val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, sl, sl)

    val staticRanks = graph.staticPageRank(maxIterations, resetProb).vertices
    staticRanks.saveAsTextFile(output)

    sc.stop()
  }

  def pagerank_usingSampledata(sc: SparkContext, input: String, output: String, maxIterations: Integer, tolerance: Double, resetProb: Double): Unit = {
    val graph = GraphLoader.edgeListFile(sc, input + "/followers.txt")
    val staticRanks = graph.staticPageRank(maxIterations, resetProb).vertices
    val ranks = graph.pageRank(tolerance, resetProb).vertices

    val users = sc.textFile(input + "/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    println(ranksByUsername.collect().mkString("\n"))
  }
}
