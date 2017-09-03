package com.stayrascal.spark.pipeline

import java.io.FileInputStream
import java.time.LocalDate

import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.pickling.Defaults._
import scala.pickling.binary._

class EmployeePickerSpec extends FunSuite {

  test("Qyery recode from per file") {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("EmployeePicker")
      .master("local[*]")
      .getOrCreate()

    val opNames = List("Test 1", "Test 2")

    val stream = new FileInputStream("~/data/combiner.txt")
    val pickle = BinaryPickle(stream)
    val params = pickle.unpickle[Map[String, StringIndexerParam]]
    println(params.mapValues(p => p.uid).mkString(", "))
    val indexers = params.mapValues(p => new StringIndexerModel(p.uid, p.labels).setInputCol(p.inputCol).setOutputCol(p.outputCol))

    val combiner = new RascalCombiner(spark)
    combiner.stringIndexers = indexers

    val picker = new EmployeePicker(combiner, "data", spark)
    val opList = picker.pickIds(opNames, LocalDate.parse("2015-10-25"))
    println(opList)
  }
}
