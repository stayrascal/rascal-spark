package com.stayrascal.spark.pipeline

import java.io.FileInputStream
import java.time.LocalDate

import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.pickling.Defaults._
import scala.pickling.binary._

class EmployeePicker(combiner: RascalCombiner, dataDir: String, spark: SparkSession) {

  import spark.implicits._

  def pickIds(ids: List[String], weekEndDay: LocalDate): Map[String, String] = {
    val recordDF = combiner.generateRecordDF(dataDir)
    val filteredDF = recordDF.filter(col("EmployeeId") isin (ids: _*)).filter(to_date(col("EndDate")) === "2015-10-25")
    val featured = combiner.buildFeatures(filteredDF, withLabel = false)

    featured.select("EmployeeId", "Features")
      .map(row => (row.getAs[String]("EmployeeId"), row.getAs[DenseVector]("FEatures").toArray.mkString(",")))
      .collect()
      .toMap
  }
}

object EmployeePicker {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("EmployeePicker")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val opNames = List("Test 1", "Test 2")
    val stream = new FileInputStream("combiner.txt")
    val pickle = BinaryPickle(stream)
    val params = pickle.unpickle[Map[String, StringIndexerParam]]
    println(params.mapValues(p => p.uid).mkString(", "))
    val indexers = params.mapValues(p => new StringIndexerModel(p.uid, p.labels).setInputCol(p.inputCol).setOutputCol(p.outputCol))

    val combiner = new RascalCombiner(spark)
    combiner.stringIndexers = indexers

    val picker = new EmployeePicker(combiner, "data", spark)
    val opList = picker.pickIds(opNames, LocalDate.parse("2015-10-25"))
    print(opList)
  }
}

