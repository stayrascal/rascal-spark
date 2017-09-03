package com.stayrascal.spark

import java.util.UUID

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scopt.OptionParser


case class Paramerter(path: String = "", tableName: String = "")

class EmployeeDataProcess {

}

object EmployeeDataProcess {

  val parser = new OptionParser[Paramerter]("scopt") {
    head("Data Cloud", "1.0")

    opt[String]('f', "path").required().action((x, c) => c.copy(path = x)).text("file path can't be empty")
    opt[String]('t', "tableName").required().action((x, c) => c.copy(tableName = x)).text("table name can't be empty")

    help("help").text("usage: ")
  }

  def saveAsTable(path: String, tableName: String) = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Rascal Spark Demo Employee Data Process")
      .getOrCreate()

    val employees = spark.sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(path)

    val newSchema = StructType(employees.schema.fields.map(f => StructField(s"field_${UUID.randomUUID().toString}", f.dataType, metadata = f.metadata)))
    val employeeDF = spark.sqlContext.createDataFrame(employees.rdd, newSchema)
    employeeDF.write.mode(SaveMode.Overwrite).saveAsTable(tableName)
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Paramerter()) match {
      case Some(config) => saveAsTable(config.path, config.tableName)
      case None => println("error parameter"); sys.exit(1)
    }
  }
}
