package com.stayrascal.spark.kafka.pageRank

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession

object PageRankDataGenRun {
  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      println("usage: <input> <output>; datagen: <numVertices> <numPartitions> <mu> <sigma>; run: <maxIterations> <tolerance> <resetProb>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val spark = SparkSession.builder().appName("Page Rank Data Gen Run").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val output = args(1)

    val numVertices = args(2).toInt
    val numPar = args(3).toInt
    val mu = args(4).toDouble
    val sigma = args(5).toDouble

    val numIter = args(6).toInt
    val tolerance = args(7).toDouble
    val resetProb = args(8).toDouble

    val graph = GraphGenerators.logNormalGraph(sc, numVertices, numPar, mu, sigma)
    val staticRanks = graph.staticPageRank(numIter, resetProb).vertices.persist()
    val dynamicRanks = graph.pageRank(tolerance, resetProb).vertices.persist()
    val err = compareRanks(staticRanks, dynamicRanks)
  }

  def compareRanks(vertexA: VertexRDD[Double], vertexB: VertexRDD[Double]): Double = {
    vertexA.leftJoin(vertexB) { case (id, a, bOpt) => (a - bOpt.getOrElse(0.0)) * (a - bOpt.getOrElse(0.0)) }.map { case (id, error) => error }.sum()
  }
}
