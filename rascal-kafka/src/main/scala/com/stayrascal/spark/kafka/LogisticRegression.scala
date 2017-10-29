package com.stayrascal.spark.kafka

import breeze.linalg.{DenseVector, Vector}
import breeze.numerics.exp
import org.apache.spark.sql.SparkSession

import scala.util.Random


object LogisticRegression {

  case class DataPoint(x: Vector[Double], y: Double)

  def parsePoint(line: String, d: Int): DataPoint = {
    val data = line.split(" ").take(d + 1).map(_.toDouble)
    DataPoint(new DenseVector(data.drop(1)), data(0))
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: LogisticRegression <file> <dimensions> <iterations>")
      System.exit(1)
    }

    val inputPath = args(0)
    val dimensions = args(1).toInt
    val iterations = args(2).toInt

    val spark = SparkSession.builder().appName("LogisticRegression").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val lines = sc.textFile(inputPath)
    val points = lines.map(parsePoint(_, dimensions)).persist()

    val rand = new Random(42)
    var w = DenseVector.fill(dimensions) {
      2 * rand.nextDouble() - 1
    }
    println("Initial w: " + w)
    for (i <- 1 to iterations) {
      println("On iteration " + i)
      val gradient = points map { p =>
        p.x * (1 / (1 + exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      } reduce (_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    sc.stop()
  }
}
