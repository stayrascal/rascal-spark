package com.stayrascal.spark.example

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object PageRankExample {

  var spark = SparkSession.builder()
    .appName("Page Rank")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val links = spark.sparkContext.objectFile[(String, Seq[String])]("links")
      .partitionBy(new HashPartitioner(100))
      .persist()
    var ranks = links.mapValues(v => 1.0)
    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)
    }
    ranks.saveAsTextFile("ranks")
  }
}
