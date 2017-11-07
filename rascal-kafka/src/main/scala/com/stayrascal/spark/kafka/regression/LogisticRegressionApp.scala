package com.stayrascal.spark.kafka.regression

import com.stayrascal.spark.kafka.DataSet
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}

object LogisticRegressionApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Spark Logistic Regression Example").master("local[*]").getOrCreate()

    val data: DataFrame = DataSet.loadData(spark)

    val colArray = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    val vecDF: DataFrame = new VectorAssembler().setInputCols(colArray).setOutputCol("features").transform(data)

    val Array(trainData, testData) = vecDF.randomSplit(Array(0.7, 0.3))

    val lrModel = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").fit(trainData)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    println(s"Elastic net param: ${lrModel.getElasticNetParam}") // alpha=0 => L2, alpha=1 => L1, 0<alpha<1 => combine L1 and L2
    println(s"Regularization parameters: ${lrModel.getRegParam}")
    println(s"Is Standardization: ${lrModel.getStandardization}")
    println(s"Threshold value: ${lrModel.getThreshold}")
    println(s"Tolerance: ${lrModel.getTol}")
    print(s"Max iteration: ${lrModel.getMaxIter}")

    lrModel.transform(testData).select("features", "rawPrediction", "probability", "prediction").show(30, false)
    val trainingSummary = lrModel.summary
    trainingSummary.objectiveHistory.foreach(println)
  }
}
