package com.stayrascal.spark.example

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamExample {

  val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[*]")
  val ssc = new StreamingContext(conf, Seconds(2))
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
  }
}
