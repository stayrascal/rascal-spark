package com.stayrascal.spark.kafka.classifier

import com.stayrascal.spark.kafka.DataSet
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object MultilayerPerceptronClassifierApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Spark Multilayer Perceptron Classifier App").master("local[*]").getOrCreate()

    val data = DataSet.loadData(spark)

    val colArray = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    val dataDF = new VectorAssembler().setInputCols(colArray).setOutputCol("features").transform(data)

    val Array(trainData, testData) = dataDF.randomSplit(Array(0.6, 0.4), seed = 1234L)

    val layers = Array[Int](8, 9, 8, 2)

    val trainer = new MultilayerPerceptronClassifier()
      .setPredictionCol("prediction")
      .setLabelCol("label")
      .setLayers(layers)
      .setMaxIter(100)
      .setTol(1E-4)
      .setSeed(1234L)
      .setBlockSize(128)
      .setStepSize(0.03)

    val model = trainer.fit(trainData)
    val result = model.transform(testData)
    val predictionLabels = result.select("prediction", "label")

    val evaluator = new MulticlassClassificationEvaluator().setPredictionCol("prediction").setLabelCol("label").setMetricName("accuracy")
    println(s"Accuracy: ${evaluator.evaluate(predictionLabels)}")
  }
}
