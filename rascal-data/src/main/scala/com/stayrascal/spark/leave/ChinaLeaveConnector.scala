package com.stayrascal.spark.leave

import org.apache.spark.sql.SparkSession

class ChinaLeaveConnector {

}

object ChinaLeaveConnector {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Rascal Spark Demo Employee Data Process")
      .master("local[8]")
      .getOrCreate()

    spark.sqlContext.read.format("jdbc").options(
      Map(
        "url" -> "jdbc:postgresql:localhost:5432/leave_staging",
        "driver" -> "org.postgresql.Driver",
        "dbtable" -> "person")
    ).load().show()
  }
}