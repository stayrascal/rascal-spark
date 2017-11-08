package com.stayrascal.spark.kafka

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FPGrowth {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data = List("1,2,5", "1,2,3,5", "1,2").toDF("items")
    val transactions: RDD[Array[String]] = data.rdd.map { s =>
      val str = s.toString().drop(1).dropRight(1)
      str.trim().split(",")
    }
    val fpg = new FPGrowth().setMinSupport(0.5).setNumPartitions(8)

    val model = fpg.run(transactions)

    val freqItemSets = model.freqItemsets.map { itemset =>
      val items = itemset.items.mkString(",")
      val freq = itemset.freq
      (items, freq)
    }.toDF("items", "freq")

    freqItemSets.show

    val minConfidence = 0.6

    val rules = model.generateAssociationRules(minConfidence)
    val df = rules.map { s =>
      val l = s.antecedent.mkString(",")
      val r = s.consequent.mkString(",")
      val confidence = s.confidence
      (l, r, confidence)
    }.toDF("left_collect", "right_collect", "confidence")
    df.show
  }
}
