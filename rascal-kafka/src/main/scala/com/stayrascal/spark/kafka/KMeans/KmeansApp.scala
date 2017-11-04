package com.stayrascal.spark.kafka.KMeans

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object KmeansApp {

  def calculateRuns(args: Array[String]): Int = {
    if (args.length > 4) args(4).toInt else 1
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("usage: <input> <output> <numClusters> <maxIterations> <runs> - optional")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Spark KMeans Example").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val output = args(1)
    val K = args(2).toInt
    val maxIterations = args(3).toInt
    val runs = calculateRuns(args)

    // Load and PArse the data
    var start = System.currentTimeMillis()
    val data = sc.textFile(input).map(s => Vectors.dense(s.split(' ').map(_.toDouble))).persist()
    val loadTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    // Cluster the data into two classes using KMeans
    start = System.currentTimeMillis()
    val clusters: KMeansModel = KMeans.train(data, K, maxIterations, runs, KMeans.K_MEANS_PARALLEL, seed = 127L)
    val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0
    println("Cluster centers: " + clusters.clusterCenters.mkString(","))


    start = System.currentTimeMillis()
    val vectorsAndClusterIdx = data.map { point =>
      val prediction = clusters.predict(point)
      (point.toString, prediction)
    }
    vectorsAndClusterIdx.saveAsTextFile(output)
    val saveTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    // Evaluate clustering by computing within set sum of squared errors
    start = System.currentTimeMillis()
    val WSSSE = clusters.computeCost(data)
    val testTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    println(compact(Map("loadTime" -> loadTime, "trainingTime" -> trainingTime, "testTime" -> testTime, "saveTime" -> saveTime)))
    println("Within Set Sum of Squared Errors = " + WSSSE)
    sc.stop()

    sc.stop()
  }
}
