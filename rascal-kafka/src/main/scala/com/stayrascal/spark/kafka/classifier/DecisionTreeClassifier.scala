package com.stayrascal.spark.kafka.classifier

import com.stayrascal.spark.kafka.DataSet
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DecisionTreeClassifier {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark Decision Tree Classifier").master("local[*]").getOrCreate()

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
    val dt = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setImpurity("entropy") // 不纯度
      .setMaxBins(100) // 离散化"连续特征"的最大划分数
      .setMaxDepth(5) // 树的最大深度
      .setMinInfoGain(0.01) // 一个节点分裂的最小信息增益，值为【0，1】
      .setMinInstancesPerNode(10) // 每个节点包含的最小样本树
      .setSeed(123456)

    // Convert index label to original label
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    // Train model, This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // make prediction
    val predictions = model.transform(testData)

    predictions.select("predictedLabel", "label", "features").show(10, truncate = false)

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.9 - accuracy))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    treeModel.getLabelCol
    treeModel.getFeaturesCol
    treeModel.featureImportances
    treeModel.getPredictionCol
    treeModel.getProbabilityCol

    treeModel.numClasses
    treeModel.numFeatures
    treeModel.depth
    treeModel.numNodes

    treeModel.getImpurity
    treeModel.getMaxBins
    treeModel.getMaxMemoryInMB
    treeModel.getMinInfoGain
    treeModel.getMinInstancesPerNode

    println("Leaned classification tree model: \n" + treeModel.toDebugString)

  }
}
