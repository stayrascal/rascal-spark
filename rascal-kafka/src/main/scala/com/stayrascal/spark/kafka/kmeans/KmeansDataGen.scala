package com.stayrascal.spark.kafka.kmeans

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.util.KMeansDataGenerator
import org.apache.spark.sql.SparkSession

object KmeansDataGen {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println("usage: <output> <numPoints> <numClusters> <dimension> <scaling factor> [numpar]")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Spark KMeans DataGen").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val output = args(0)
    val numPoint = args(1).toInt
    val numCluster = args(2).toInt
    val numDim = args(3).toInt
    val scaling = args(4).toDouble
    val defPar = if (System.getProperty("spark.default.parallelism") == null) 2 else System.getProperty("spark.default.parallelism").toInt
    val numPar = if (args.length > 5) args(5).toInt else defPar

    val data = KMeansDataGenerator.generateKMeansRDD(sc, numPoint, numCluster, numDim, scaling, numPar)
    data.map(_.mkString("")).saveAsTextFile(output)
    sc.stop()
  }
}
