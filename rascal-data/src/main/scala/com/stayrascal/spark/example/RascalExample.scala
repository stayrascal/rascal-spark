package com.stayrascal.spark.example

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


case class Stock(date: String, openPrice: Double, highPrice: Double, lowPrice: Double, closePrice: Double, volume: Double, adjClosePrice: Double)

object RascalExample {

  import org.apache.spark.sql._

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Rascal Spark Example Demo")
    .master("local[*]")
    .getOrCreate()

  val conf = new SparkConf().setAppName("Rascal Spark Example Demo").setMaster("local[*]")
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    /*val spyDF = spark.read.csv("/Users/zpwu/workspace/spark/data/SPY_2016.csv")
    spyDF.show()*/
    //        val spyFile = spark.read.format("csv").textFile("/Users/zpwu/workspace/spark/data/SPY_2016.csv")
    //        val apcFile = spark.read.format("csv").textFile("/Users/zpwu/workspace/spark/data/APC_2016.csv")
    //        val xomFile = spark.read.format("csv").textFile("/Users/zpwu/workspace/spark/data/XOM_2016.csv")
    val spyFile = "/Users/zpwu/workspace/spark/data/SPY_2016.csv"
    val apcFile = "/Users/zpwu/workspace/spark/data/APC_2016.csv"
    val xomFile = "/Users/zpwu/workspace/spark/data/XOM_2016.csv"

    val data = spark.read.textFile(spyFile).map(X => X.split(",")).map(line =>
      Stock(line(0), line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble, line(6).toDouble)
    )
    data.createOrReplaceTempView("spy")
    spark.udf.register("ppa", pas)
  }

  def pas = (str: String) => {
    val format = new SimpleDateFormat("MM/dd/yyyy")
    var days = Array("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
    val split = days(format.parse(str).getDay).toString
  }

  def parseDataset(str: String): Stock = {
    val line = str.split(",")
    Stock(line(0), line(1).toDouble, line(2).toDouble, line(3).toDouble, line(4).toDouble, line(5).toDouble, line(6).toDouble)
  }

  def parseRDD(rdd: RDD[String]): RDD[Stock] = {
    rdd.map(parseDataset).cache()
  }
}
