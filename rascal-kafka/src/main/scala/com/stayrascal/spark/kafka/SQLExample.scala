package com.stayrascal.spark.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SQLExample {
  val spark = SparkSession.builder()
      .appName("SQL Example")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val hdfsDs = spark.sqlContext.read.text("hdfs://*******").as[String]

    // DataFrame
    hdfsDs.flatMap(_.split(" "))
      .filter(_ != " ")
      .toDF()
        .groupBy("value")
      .agg(count("*") as "numOccurances")
      .orderBy("numOccurances")


    hdfsDs.select()
  }
}
