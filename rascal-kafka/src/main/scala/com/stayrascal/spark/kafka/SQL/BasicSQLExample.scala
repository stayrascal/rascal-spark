package com.stayrascal.spark.kafka.SQL

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicSQLExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()


    val dataDF: DataFrame = loadDataFrame(spark)
    val encodedDF: DataFrame = oneHotEncoding(dataDF)

    val vecDF: DataFrame = combineFeature(encodedDF)

    val scaledData: DataFrame = standardize(vecDF)

    pca(scaledData)

    kmeans(scaledData, spark)


  }

  private def kmeans(scaledData: DataFrame, spark: SparkSession) = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val KSSE = (2 to 20 by 1).toList.map { k =>
      // Trains a k-means model.
      val kmeans = new KMeans().setK(k).setSeed(1L).setFeaturesCol("scaledFeatures")
      val model = kmeans.fit(scaledData)

      // Evaluate clustering by computing within Set Sum of Squared Errors.
      val WSSSE = model.computeCost(scaledData)

      (k, model.getMaxIter, WSSSE, model.summary.cluster, model.summary.clusterSizes, model.clusterCenters)
    }

    val KSSEdf: DataFrame = KSSE.map { x => (x._1, x._2, x._3, x._5) }.toDF("K", "MaxIter", "SSE", "clusterSizes")

    KSSE.foreach(println)
  }

  private def pca(scaledData: DataFrame) = {
    val pca = new PCA().setInputCol("scaledFeatures").setInputCol("scaledFeatures").setOutputCol("pcaFeatures").setK(3).fit(scaledData)
    pca.explainedVariance.values // explaining variable variance

    pca.pc // 载荷 （观测变量与主成分的相关系数）
    pca.extractParamMap()
    pca.params
    val pcaDF: DataFrame = pca.transform(scaledData).persist()
    pcaDF.printSchema()
  }

  private def standardize(vecDF: DataFrame) = {
    val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(true)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(vecDF)

    // Normalize each feature to have unit standrd deviation.
    val scaledData: DataFrame = scalerModel.transform(vecDF)

    scaledData.select("features", "scaledFeatures").show()
    scaledData
  }

  private def combineFeature(encodedDF: DataFrame) = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("affairs", "age", "yearsmarried", "religiousness", "education", "occupation", "rating", "genderVec", "childrenVec"))
      .setOutputCol("features")
    val vecDF: DataFrame = assembler.transform(encodedDF)
    vecDF.select("features").show()
    vecDF
  }

  private def oneHotEncoding(dataDF: DataFrame) = {
    // convert string to number array
    val indexer = new StringIndexer().setInputCol("gender").setOutputCol("genderIndex").fit(dataDF)
    val indexed = indexer.transform(dataDF)

    // OneHot encoding
    val encoder = new OneHotEncoder().setInputCol("genderIndex").setOutputCol("genderVec").setDropLast(false)
    val encoded = encoder.transform(indexed)

    val indexed1 = new StringIndexer().setInputCol("children").setOutputCol("childrenIndex").fit(encoded).transform(encoded)
    val encodedDF = new OneHotEncoder().setInputCol("childrenIndex").setOutputCol("childrenVec").setDropLast(false).transform(indexed1)
    encodedDF
  }

  private def loadDataFrame(spark: SparkSession) = {
    val data: DataFrame = spark.read.format("csv").option("header", true).load("hdfs:///data.Affairs,csv").persist()

    data.limit(10).show() //affairs|gender|age|yearsmarried|children|religiousness|education|occupation|rating|

    val data1 = data.select(
      data("affairs").cast("Double"),
      data("age").cast("Double"),
      data("yearsmarried").cast("Double"),
      data("education").cast("Double"),
      data("occupation").cast("Double"),
      data("rating").cast("Double"),
      data("gender").cast("Double"),
      data("children").cast("Double")
    )

    data1.printSchema()
    val dataDF = data1.persist()
    dataDF
  }
}
