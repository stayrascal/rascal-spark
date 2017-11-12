package com.stayrascal.spark.kafka.regression

import com.stayrascal.spark.kafka.DataSet
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object LogisticRegressionApp {
  val spark: SparkSession = SparkSession.builder().appName("Spark Logistic Regression Example").master("local[*]").getOrCreate()
  import org.apache.spark.sql.functions._
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    // Load and parse the data
    val data: DataFrame = DataSet.loadData(spark)

    // Feature extraction and transformation
    val colArray = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
    val vecDF: DataFrame = new VectorAssembler().setInputCols(colArray).setOutputCol("features").transform(data)

    // Create test and training set
    val Array(trainData, testData) = vecDF.randomSplit(Array(0.7, 0.3))

    // Creating an estimator using the training sets
    val lrModel = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      /*.setMaxIter(50)
      .setElasticNetParam(0.01)*/
      .fit(trainData)

    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    println(s"Elastic net param: ${lrModel.getElasticNetParam}") // alpha=0 => L2, alpha=1 => L1, 0<alpha<1 => combine L1 and L2
    println(s"Regularization parameters: ${lrModel.getRegParam}")
    println(s"Is Standardization: ${lrModel.getStandardization}")
    println(s"Threshold value: ${lrModel.getThreshold}")
    println(s"Tolerance: ${lrModel.getTol}")
    print(s"Max iteration: ${lrModel.getMaxIter}")

    // Getting raw prediction, probability, and prediction for the test set
    val predictions = lrModel.transform(testData).select("features", "rawPrediction", "probability", "prediction")
    predictions.show(30, false)

    // Generating objective history of training
    val trainingSummary = lrModel.summary
    trainingSummary.objectiveHistory.foreach(loss => println(loss))

    // Evaluation the model
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
    val roc = binarySummary.roc
    roc.show()
    println(s"Area Under ROC: ${binarySummary.areaUnderROC}")
    evaluatingModel(predictions)

    // Calculate accuracy
    val fMeasure = binarySummary.fMeasureByThreshold
    val fm = fMeasure.col("F-Measure")
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure).select("threshold").head().getDouble(0)
    // Set the model threshold to maximize fMeasure
    lrModel.setThreshold(bestThreshold)

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Accuracy: " + accuracy)

  }

  private def evaluatingModel(predictions: DataFrame) = {

    val lp = predictions.select("label", "prediction")
    val countTotal = predictions.count()
    val correct = lp.filter($"label" === $"prediction").count()
    val wrong = lp.filter(not($"label" === $"prediction")).count()
    val trueP = lp.filter($"prediction" === 1.0).filter($"label" === $"prediction").count()
    val trueN = lp.filter($"prediction" === 0.0).filter($"label" === $"prediction").count()
    val falseN = lp.filter($"prediction" === 0.0).filter(not($"label" === $"prediction")).count()
    val falseP = lp.filter($"prediction" === 1.0).filter(not($"label" === $"prediction")).count()
    val ratioWrong = wrong.toDouble / countTotal.toDouble
    val ratioCorrect = correct.toDouble / countTotal.toDouble

    println(s"Total Count: $countTotal")
    println(s"Correctly Predicted: $correct")
    println(s"Wrongly Identified: $wrong")
    println(s"True Positive: $trueP")
    println(s"True Negative: $trueN")
    println(s"False Positive: $falseP")
    println(s"False Negative: $falseN")
    println(s"RatioWrong: $ratioWrong")
    println(s"RatioCorrect: $ratioCorrect")
  }

  def multiClassClassificationWithLogisticRegression = {
    // Load training data in LIBSVM format
    val data = MLUtils.loadLibSVMFile(spark.sparkContext, "data/minist.bz2")
    val Array(training, test) = data.randomSplit(Array(0.75, 0.25), seed = 12345L)
    training.persist()

    // running the training algorithm to build the model
    /*val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .setIntercept(true)
      .setValidateData(true)
      .run(training)*/

    val model = RandomForest.trainClassifier(
      training,
      numClasses = 10,
      categoricalFeaturesInfo = Map[Int, Int](),
      numTrees = 50,
      featureSubsetStrategy = "auto",
      impurity = "gini",
      maxDepth = 30,
      maxBins = 32
    )

    // Clear the default threshold for logistic
    // model.clearThreshold()

    // compute raw scores on the test set
    val scoreAndLabels = test.map {point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Instantiate a multiclass metrics for the evaluation
    val metrics = new MulticlassMetrics(scoreAndLabels)

    // Constructing the confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Overall statistics
    val accuracy = metrics.accuracy
    println("Summary Statistics")
    println(s"Accuracy = $accuracy")
    val labels = metrics.labels
    // Precision by label
    labels.foreach(l => println(s"Precision($l) = ${metrics.precision(l)}"))
    // Recall by label
    labels.foreach(l => println(s"Recall($l) = ${metrics.recall(l)}"))
    // False positive rate by label
    labels.foreach(l => println(s"FPR($l) = ${metrics.falsePositiveRate(l)}"))
    // F0measure by label
    labels.foreach(l => println(s"F1-Score($l) = ${metrics.fMeasure(l)}"))
    println(s"Weighted precision: ${metrics.weightedPrecision}")
    println(s"Weighted recall: ${metrics.weightedRecall}")
    println(s"Weighted F1 score: ${metrics.weightedFMeasure}")
    println(s"Weighted false positive rate: ${metrics.weightedFalsePositiveRate}")
    val testErr = scoreAndLabels.filter(r => r._1 != r._2).count().toDouble / test.count()
    println(s"Accuracy = ${(1 - testErr) * 100}%")
  }
}
