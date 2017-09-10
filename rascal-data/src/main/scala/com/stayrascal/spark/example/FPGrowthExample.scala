package com.stayrascal.spark.example

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.SparkSession

class FPGrowthExample {

}

object FPGrowthExample {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Rascal Spark FPGrowth Example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val data = spark.read.textFile("/Users/zpwu/workspace/spark/data/Groceries.txt")
    val fpData = data.filter(line => !line.contains("items"))
      .map(line => line.split("\\{"))
      .map(line => line(1).replace("}\"", ""))
      .map(_.split(","))
      .cache()

    val fpGroup = new FPGrowth().setMinSupport(0.05).setNumPartitions(3)
    val fpModel = fpGroup.run(fpData.rdd)
    val frepItems = fpModel.freqItemsets.collect
    frepItems.foreach(f => println("FrequentItem: " + f.items.mkString(",") + " OccurrenceFrequency: " + f.freq))

    val userId = 2
    val userList = fpData.take(3)(userId)

    var goodsFreq = 0L
    for (goods <- frepItems) {
      if (goods.items.mkString == userList.mkString) {
        goodsFreq = goods.freq
      }
    }
    println("GoodsNumber: " + goodsFreq)

    for (f <- frepItems) {
      if (f.items.mkString.contains(userList.mkString) && f.items.size > userList.size) {
        val conf: Double = f.freq.toDouble / goodsFreq.toDouble
        if (conf >= 0.1) {
          var item = f.items
          for (i <- 0 until userList.size) {
            item = item.filter(_ != userList(i))
          }
          for (str <- item) {
            println(str + " === " + conf)
          }
        }
      }
    }

  }
}
