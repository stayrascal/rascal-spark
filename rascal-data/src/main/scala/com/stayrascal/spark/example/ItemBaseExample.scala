package com.stayrascal.spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

case class Item(userId: Long, itemCode: Long, ref: Float)

object ItemBaseExample {
  val spark = SparkSession.builder()
    .appName("Item base recommend")
    .master("local[*]")
    .getOrCreate()

  def occMatrix(a: Array[(Long, Float)]): ArrayBuffer[((Long, Long), Float)] = {
    val array = ArrayBuffer[((Long, Long), Float)]()

    for (i <- 0 to (a.size - 1); j <- (i + 1) to (a.size - 1)) {
      array += (((a(i)._1, a(j)._1), a(j)._2 * a(j)._2))
    }
    array

  }

  def main(args: Array[String]): Unit = {
    val dataClean = spark.sparkContext.textFile("file:///Users/zpwu/test/itembase.txt").map {
      line =>
        val token = line.split(",")
        (token(0).toLong, (token(1).toLong, if (token.length > 2) token(2).toFloat else 0f))
    }.aggregateByKey(Array[(Long, Float)]())(_ :+ _, _ ++ _).filter(_._2.size > 2).values.persist(StorageLevel.MEMORY_ONLY_SER)


    val norms = dataClean.flatMap(_.map(y => (y._1, y._2 * y._2))).reduceByKey(_ + _)

    val normsMap = spark.sparkContext.broadcast(norms.collectAsMap())

    val matrix = dataClean.map(list => list.sortWith(_._1 > _._1)).flatMap(occMatrix).reduceByKey(_ + _)

    val similarity = matrix.map(a => (a._1._1,
      (a._1._2, 1 / (1 + Math.sqrt(normsMap.value.get(a._1._1).get + normsMap.value.get(a._1._2).get - 2 * a._2)))))
    similarity.collect.foreach(println)
    spark.sparkContext.stop()
  }
}
