package com.stayrascal.spark.kafka.kmeans

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object KmeansExample {

  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).filter(_ (6) != "?").map(_.drop(1)).map(_.map(_.toDouble))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("KMeans").config("spark.sql.warehouse.dir", "").getOrCreate()

    val start = System.currentTimeMillis()
    val dataPath = "data/Saratoga-NY-Homes.txt"
    val landDF = parseRDD(spark.sparkContext.textFile(dataPath))
  }
}
