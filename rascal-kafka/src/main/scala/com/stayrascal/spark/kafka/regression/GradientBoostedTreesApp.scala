package com.stayrascal.spark.kafka.regression

import com.stayrascal.spark.kafka.DataSet
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.sql.{DataFrame, SparkSession}

object GradientBoostedTreesApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Spark Logistic Regression Example").master("local[*]").getOrCreate()

    val data: DataFrame = DataSet.loadData(spark)

    val colArray = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    val vecDF: DataFrame = new VectorAssembler().setInputCols(colArray).setOutputCol("features").transform(data)

    val Array(trainData, testData) = vecDF.randomSplit(Array(0.7, 0.3))

    val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(5)

    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setImpurity("variance")
      .setLossType("squared")
      .setMaxIter(100)
      .setMinInstancesPerNode(100)

    val pipeline = new Pipeline().setStages(Array(featureIndexer, gbt))

    val model = pipeline.fit(trainData)

    val prediction = model.transform(testData)
    prediction.select("prediction", "label", "features").show(20, false)

    val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
    val rmse = evaluator.evaluate(prediction)
    println(s"Root Mean Squared Error (RMSE) on test data = ${rmse}")

    val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println(s"Learned regression GBT model:\n" + gbtModel.toDebugString)
  }
}
