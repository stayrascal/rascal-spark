package com.stayrascal.spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameExample {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Rascal Spark For DataFrame")
    .master("local[*]")
    .getOrCreate()

  def convertColumn(df: DataFrame, name: String, newType: String): DataFrame = {
    val df_1 = df.withColumnRenamed(name, "swap")
    df_1.withColumn(name, df_1.col("swap").cast(newType)).drop("swap")
  }

  def main(args: Array[String]): Unit = {
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true").csv("/Users/zpwu/workspace/spark/data/1987.csv")
    val df_1 = df.withColumnRenamed("Year", "oldYear")
    val df_2 = df_1.withColumn("Year", df_1.col("oldYear").cast("int")).drop("oldYear")
    df_2.printSchema()
    val df_3 = convertColumn(df_2, "ArrDelay", "int")
    val df_4 = convertColumn(df_3, "DepDelay", "int")
    val averageDelays = df_4.groupBy("FlightNum").agg(avg(df_4.col("ArrDelay")), avg(df_4.col("DepDelay")))
    averageDelays.cache()


  }
}
