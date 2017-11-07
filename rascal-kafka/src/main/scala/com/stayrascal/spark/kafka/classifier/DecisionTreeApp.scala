package com.stayrascal.spark.kafka.classifier

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.sql.SparkSession

object DecisionTreeApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("usage: <input> <output> <numClass> <impurity> <maxDepth> <maxBin> <mode:regression/Classification>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Page Rank Data Gen").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val output = args(1)
    val numClasses = args(2).toInt
    val impurity = args(3) // "gini"
    val maxDepth = args(1).toInt
    val maxBins = args(2).toInt
    val mode = args(6)
    val categoricalFeaturesInfo = Map[Int, Int]()


    var start = System.currentTimeMillis()
    val data = sc.textFile(input).map(line => LabeledPoint.parse(line))
    val loadTime = (System.currentTimeMillis() - start).toDouble / 1000.0

    // Train a DecisionTree mode for classification
    start = System.currentTimeMillis()
    val model = mode match {
      case "Classification" => DecisionTree.trainClassifier(data, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
      case _ => DecisionTree.trainRegressor(data, categoricalFeaturesInfo, impurity, maxDepth, maxBins)
    }
    val trainingTime = (System.currentTimeMillis() - start) / 1000.0

    start = System.currentTimeMillis()
    val predictionAndLabel = data.map(p => (model.predict(p.features), p.label))
    val trainErr = 1.0 * predictionAndLabel.filter(p => p._1 != p._2).count() / data.count()
    val testTime = (System.currentTimeMillis() - start) / 1000.0

    println(s"loadTime: $loadTime, trainingTime: $trainingTime, testTime: $testTime")
    println(s"Training error: $trainErr")
    println(s"Leaned classification tree model:\n $model")
    sc.stop()

  }
}
