package com.stayrascal.spark.kafka

import breeze.linalg.{DenseVector, Vector, squaredDistance}
import org.apache.spark.sql.SparkSession

object KMeans {

  def parseVector(line: String): Vector[Double] = {
    DenseVector(line.split(" ").map(_.toDouble))
  }

  def closestPoint(p: Vector[Double], centers: Array[Vector[Double]]): Int = {
    var bestInt = 0;
    var closest = Double.PositiveInfinity

    for (i <- 0 until centers.length) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest.length) {
        closest = tempDist
        bestInt = i
      }
    }
    bestInt
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: KMeans <file> <k> <convergeDist>")
      System.exit(1)
    }

    val spark = SparkSession.builder().appName("KMeans").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val inputFile = args(0)
    val K = args(1).toInt
    val convergeDist = args(2).toDouble

    val data = sc.textFile(inputFile).map(parseVector).persist()
    val kPoints = data.takeSample(withReplacement = false, K, 42,).toArray
    var tempDist = 1.0

    while (tempDist > convergeDist) {
      val closet = data.map(p => (closestPoint(p, kPoints), (p, 1)))
      val pointStats = closet.reduceByKey { case ((x1, y1), (x2, y2)) => (x1 + x2, y1 + y2) }
      val newPoints = pointStats.map(pair => (pair._1, pair._2._1 * (1.0 / pair._2._2))).collectAsMap()
      tempDist = 0.0
      for (i <- 0 until K) {
        tempDist += squaredDistance(kPoints(i), newPoints(i))
      }

      for (newP <- newPoints) {
        kPoints(newP._1) = newP._2
      }

      println(s"Finished iteration (delta = $tempDist")
    }

    println("Final centers:")
    kPoints.foreach(println)

  }
}
