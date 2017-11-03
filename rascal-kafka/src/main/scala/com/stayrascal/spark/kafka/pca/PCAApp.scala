package com.stayrascal.spark.kafka.pca

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

object PCAApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("usage: <output> <dimensions>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Spark PCA Example").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val dimensions = args(1).toInt

    // Load and parse the data
    val parseData = sc.textFile(input)

    var start = System.currentTimeMillis()
    val data = MLUtils.loadLabeledPoints(sc, input).persist()
    val loadTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    // Build model
    start = System.currentTimeMillis()
    val pca = new PCA(dimensions).fit(data.map(_.features))
    val trainingTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    start = System.currentTimeMillis()
    val training_pca = data.map(p => p.copy(features = pca.transform(p.features)))
    val numData = training_pca.count()
    val testTime = (System.currentTimeMillis() - start) / 1000.0

    println(compact(render(Map("loadTime" -> loadTime, "trainingTime" -> trainingTime, "testTime" -> testTime))))
    println("Number of Data = " + numData)
    sc.stop()

    sc.stop()
  }
}
