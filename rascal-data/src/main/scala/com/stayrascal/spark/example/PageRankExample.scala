package com.stayrascal.spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object PageRankExample {

  var spark = SparkSession.builder()
    .appName("Page Rank")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    /*val links = spark.sparkContext.objectFile[(String, Seq[String])]("links")
      .partitionBy(new HashPartitioner(100))
      .persist()*/

    val file = spark.sparkContext.textFile("file:///Users/zpwu/test/pageRank.txt").persist(StorageLevel.MEMORY_ONLY_SER)

    val initFile = file.flatMap {
      _.split(",").toList
    } // Array(2, 1, 2, 4, 3, 2, 3, 5, 4, 1, 5, 3, 6, 7)


    val rankFile = file.map { line =>
      val token = line.split(",")
      (token(0), token(1))
    }.distinct() //  Array((2,1), (2,4), (3,2), (3,5), (4,1), (5,3), (6,7))

    file.unpersist(true)

    var init = initFile.distinct().map((_, 1f)) // Array((4,1.0), (6,1.0), (2,1.0), (7,1.0), (5,1.0), (3,1.0), (1,1.0))
    var map = spark.sparkContext.broadcast(init.collectAsMap())

    val num = 10
    for (i <- 1 to num) {
      init = rankFile.aggregateByKey(List[String]())(_ :+ _, _ ++ _)
        .flatMap(calculate(_, map.value)).reduceByKey(_ + _)
        .rightOuterJoin(init).map(x => (x._1, if (x._2._1.isDefined == true) x._2._1.get else 0f))
      if (i != num) {
        map = spark.sparkContext.broadcast(init.collectAsMap())
      }
      init.collect().foreach(println)
    }


    /*var ranks = links.mapValues(v => 1.0)
    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }
    ranks.saveAsTextFile("ranks")*/
  }

  def calculate(a: (String, List[String]), b: scala.collection.Map[String, Float]): ArrayBuffer[(String, Float)] = {
    var array = ArrayBuffer[(String, Float)]()
    for (i <- 0 to (a._2.size - 1)) {
      array += ((a._2(i), b.get(a._2(i)).get / a._2.size))
    }
    array
  }
}
