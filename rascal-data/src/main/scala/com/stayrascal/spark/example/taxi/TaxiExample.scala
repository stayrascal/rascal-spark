package com.stayrascal.spark.example.taxi

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

class TaxiExample {

}

object TaxiExample {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Rascal Spark Taxi Example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val fieldSchema = StructType(Array(
      StructField("TID", StringType, true),
      StructField("Lat", DoubleType, true),
      StructField("Lon", DoubleType, true),
      StructField("Time", StringType, true)
    ))

    val taxiDF = spark.read.format("com.databricks.spark.csv").option("header", "false").schema(fieldSchema).load("/Users/zpwu/workspace/spark/data/taxi/taxi.csv")

    val columns = Array("Lat", "Lon")
    val va = new VectorAssembler().setInputCols(columns).setOutputCol("features")
    val taxiDF2 = va.transform(taxiDF)
    taxiDF2.cache()

    val trainTestRatio = Array(0.7, 0.3)
    val Array(trainingData, testData) = taxiDF2.randomSplit(trainTestRatio, 2333)

    val km = new KMeans().setK(10).setFeaturesCol("features").setPredictionCol("prediction")
    val kmModel = km.fit(taxiDF2)
    val kmResult = kmModel.clusterCenters
    val kmRDD1 = spark.sparkContext.parallelize(kmResult)
    val kmRDD2 = kmRDD1.map(x => (x(1), x(0)))
    kmRDD2.saveAsTextFile("/Users/zpwu/workspace/spark/data/taxi/kmResult")


    val predictions = kmModel.transform(testData)
    predictions.createOrReplaceTempView("predictions")
    val tmpQuery = predictions.select(substring($"Time", 0, 2).alias("hour"), $"prediction").groupBy("hour", "prediction")
    val predictCount = tmpQuery.agg(count("prediction").alias("count")).orderBy(desc("count"))
    predictCount.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save("/Users/zpwu/workspace/spark/data/taxi/predictCount")
    val busyZones = predictions.groupBy("prediction").count()
    busyZones.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).save("/Users/zpwu/workspace/spark/data/taxi/busyZones")
  }
}
