package com.stayrascal.spark.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordcount extends App {
  val conf = new SparkConf().setAppName("WordCount").setIfMissing("spark.master", "local[*]")
  val ssc = new StreamingContext(conf, Seconds(3));

  ssc.socketTextStream("localhost", 9999)
    .flatMap(_.split(" "))
    .map(_ -> 1)
    .reduceByKey(_ + _)
    .print

  ssc.start()
  ssc.awaitTermination()
}
