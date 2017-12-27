package com.stayrascal.spark.example.sql

import org.apache.spark.sql.SparkSession

class SparkSQLSupport(val appName: String, val master: String = "local") {

  @transient
  val spark = SparkSession.builder().appName(appName).master(master).getOrCreate()

}
