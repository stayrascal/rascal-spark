package com.stayrascal.spark

import org.apache.spark.sql.SparkSession

class EmployeeDataProcessSpec extends org.scalatest.FunSuite {
  test("Load td csv file") {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Rascal Spark Demo Data Process")
      .master("local[8]")
      .getOrCreate()

    val tdDF = spark.sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("~/data/td/td_sample.csv")

    tdDF.show()
  }
}
