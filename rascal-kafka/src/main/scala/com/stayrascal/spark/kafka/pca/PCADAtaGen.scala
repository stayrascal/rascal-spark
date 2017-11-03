package com.stayrascal.spark.kafka.pca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.sql.SparkSession

object PCADAtaGen {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("usage: <output> <numExamples> <numFeatures> <numpar>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Spark PCA DataGen").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val output = args(0)
    val numExamples = args(1).toInt
    val numFeatures = args(2).toInt
    val epsilon = 10
    val defPAr = if (System.getProperty("spark.default.parallelism") == null) 2 else System.getProperty("spark.default.parallelism").toInt
    val numPar = if (args.length > 3) args(3).toInt else defPAr

    val data = LinearDataGenerator.generateLinearRDD(sc, numExamples, numFeatures, epsilon, numPar)
    data.saveAsTextFile(output)
    sc.stop()
  }
}
