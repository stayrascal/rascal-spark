package com.stayrascal.spark.kafka.classifier

import com.stayrascal.spark.kafka.DataSet
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}

object RandomForestClassifierApp {
  val spark = SparkSession.builder().appName("Spark Random Forest Classifier").master("local[*]").getOrCreate()

  val dataLabelDF: DataFrame = DataSet.loadData(spark)

  val featuresArray = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
  val assembler = new VectorAssembler().setInputCols(featuresArray).setOutputCol("features")
  val vecDF: DataFrame = assembler.transform(dataLabelDF)
  vecDF.show(10, truncate = false)

  val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(vecDF)
  labelIndexer.transform(vecDF).show(10, truncate = false)

  val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(5).fit(vecDF)
  featureIndexer.transform(vecDF).show(10, truncate = false)

  val Array(trainingData, testData) = vecDF.randomSplit(Array(0.7, 0.3))

  def main(args: Array[String]): Unit = {


    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    // Convert index label to original label
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.impurity, Array("entropy", "gini")) // 不纯度
      .addGrid(rf.maxBins, Array(32, 64)) // 离散化连续特征的最大划分数
      .addGrid(rf.maxDepth, Array(5, 7, 10)) // 树的最大深度
      .addGrid(rf.minInfoGain, Array(0, 0.5, 1)) // 一个节点分裂的最小信息增益
      .addGrid(rf.minInstancesPerNode, Array(10, 20)) // 每个节点包含的最小样本树
      .addGrid(rf.numTrees, Array(20, 50)) // 树的数量
      .addGrid(rf.featureSubsetStrategy, Array("auto", "sqrt")) // 在每个树节点处分割的特征树，参数值比较多
      .addGrid(rf.maxMemoryInMB, Array(256, 512)) // 如果太小，则每次迭代将拆分1个节点，其聚合可能超过此大小
      .addGrid(rf.subsamplingRate, Array(0.8, 1)) // 给每颗树分配学习数据的比例 0-1
      .addGrid(rf.checkpointInterval, Array(10, 20)) // 设置检查间隔(>= 1)或禁用检查点(-1)，例如10意味着每迭代10次缓存将获得检查点
      .addGrid(rf.cacheNodeIds, Array(false, true)) // false：算法将树传递给执行器以将实例与节点匹配，true：算法将缓存每个实例的节点ID。 缓存可以加速更大深度的树的训练，可以通过设置checkpointInterval来设置检查或禁用缓存的频率
      .addGrid(rf.seed, Array(123456L, 111L)) // 种子
      .build()


    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    val cvModel = cv.fit(trainingData)

    println(cvModel.extractParamMap())
    println(cvModel.avgMetrics.length)
    println(cvModel.avgMetrics)
    println(cvModel.getEstimatorParamMaps.length)
    println(cvModel.getEstimatorParamMaps)
    println(cvModel.getEvaluator.extractParamMap())
    println(cvModel.getEvaluator.isLargerBetter)
    println(cvModel.getNumFolds)


    val predictDF: DataFrame = cvModel.transform(testData).selectExpr("predictedLabel", "label", "feature")
    predictDF.show(20, false)

    val predictions = cvModel.transform(testData)
    evaluation(predictions)

    // Finding the base cross-validation model
    println(s"The best fitted model: ${cvModel.bestModel.asInstanceOf[PipelineModel].stages(2).extractParamMap}")
  }

  private def firstVersion() = {
    val classifier = new RandomForestClassifier()
      .setImpurity("gini")
      .setMaxDepth(30)
      .setNumTrees(30)
      .setFeatureSubsetStrategy("auto")
      .setSeed(1234567)
      .setMaxBins(40)
      .setMinInfoGain(0.001)
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
    val model = classifier.fit(trainingData)
    val predictions = model.transform(testData)

    evaluation(predictions)
  }

  private def evaluation(predictions: DataFrame) = {
    val binaryClassificationEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("predictedLabel")

    val accuracy = binaryClassificationEvaluator.evaluate(predictions)
    println(s"The accuracy before pipeline fitting: $accuracy")

    def printlnMetric(metricName: String): Double = {
      val metrics = binaryClassificationEvaluator.setMetricName(metricName).evaluate(predictions)
      metrics
    }

    println(s"Area Under ROC before tuning: ${printlnMetric("areaUnderROC")}")
    println(s"Area Under RPC before tuning: ${printlnMetric("areaUnderPR")}")

    val rm = new RegressionMetrics(predictions.select("prediction", "label").rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double])))
    println(s"MSE: ${rm.meanSquaredError}")
    println(s"MAE: ${rm.meanAbsoluteError}")
    println(s"RMSE squared: ${rm.rootMeanSquaredError}")
    println(s"R Squared: ${rm.r2}")
    println(s"Explained Variance: ${rm.explainedVariance}")
  }
}
