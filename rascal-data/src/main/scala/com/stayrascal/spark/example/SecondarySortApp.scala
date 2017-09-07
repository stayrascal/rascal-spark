package com.stayrascal.spark.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SecondarySortApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Rascal Spark Secondary Sort App")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val tf = spark.read.textFile("access_log.log")
  }

  def mapTfRdd2Pair(accessLogRdd: RDD[String]): Unit = {
  }
}
