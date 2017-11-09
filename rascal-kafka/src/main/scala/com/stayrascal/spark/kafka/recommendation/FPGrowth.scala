package com.stayrascal.spark.kafka.recommendation

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 模式挖掘也叫关联规则，其实就是从大量的数据中挖掘出比较有用的数据，挖掘频繁项。
  * 关联规则算法发展到现在一共有三个算法：FP-Tree算法、Apriori算法、Eclat算法.
  * FP-Tree是一种不产生候选模式而采用频繁模式增长的方法挖掘频繁模式的算法；
  * 此算法只扫描两次，第一次扫描数据是得到频繁项集，第二次扫描是利用支持度过滤掉非频繁项，同时生成FP树。
  * 然后后面的模式挖掘就是在这棵树上进行。此算法与Apriori算法最大不同的有两点：不产生候选集，只遍历两次数据，大大提升了效率。
  * FPTree建立的规则就是树的根，为根节点，ROOT，定义为NULL，然后将数据里面的每一条数据进行插入节点操作，每个节点包含一个名称，和一个次数的属性
  *
  * FP的三个基本概念。支持度与置信度与提升度
  * 支持度：比如说A对B的支持度，就是表示AB同时出现的事件除以总事件的概率。
  * 置信度：比如说A对B的置信度，就是AB同时出现的事件除以A的总事件的概率。
  * 提升度：同样A对B，表示在包含A的条件下同时包含B的概率，除以不含A的条件下含有B的概率。
  *
  * Data: http://download.csdn.net/detail/sanqima/9301589
  * Tutorial: https://www.shiyanlou.com/courses/815/labs/2862/document
  */
object FPGrowth {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL example")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    /*demo(spark)*/

    val data = spark.read.textFile("file:///file.txt")
      .filter(line => !line.contains("items"))
      .map(line=> line.split("\\{"))
      .map(s => s(1).replace("}\"", ""))
      .map(_.split(",")).persist()

    val fpGroup = new FPGrowth().setMinSupport(0.05).setNumPartitions(3)
    val fpModel = fpGroup.run(data.rdd)

    val freqItemSets = fpModel.freqItemsets.map { itemset =>
      val items = itemset.items.mkString(",")
      val freq = itemset.freq
      (items, freq)
    }.toDF("items", "freq")

    freqItemSets.show

    val minConfidence = 0.6

    val rules = fpModel.generateAssociationRules(minConfidence)
    val df = rules.map { s =>
      val l = s.antecedent.mkString(",")
      val r = s.consequent.mkString(",")
      val confidence = s.confidence
      (l, r, confidence)
    }.toDF("left_collect", "right_collect", "confidence")

    val userId = 2
    val userList = data.take(3)(userId)
    var goodsFreq = 0L

  }

  private def demo(spark: SparkSession) = {
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
