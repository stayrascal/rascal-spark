package com.stayrascal.spark.example.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object KafakaExample {
  var spark = SparkSession.builder()
    .appName("Kafka Example")
    .master("local[*]")
    .getOrCreate()

  val kafkaStream = {
    val sparkStreamingConsumerGroup = "spark-streaming-consumer-group"
    val kafkaParams = Map(
      "zookeeper.connect" -> "zookeeper1:2181",
      "group.id" -> "spark-streaming-test",
      "zookeeper.connection.timeout.ms" -> "1000"
    )
    val inputTopic = "input-topic"
    val numPartitionsOfInputTopic = 5
    val streams = (1 to numPartitionsOfInputTopic) map {_ =>
      KafkaUtils.createStream(spark.sparkContext, kafkaParams, Map(inputTopic => 1), StorageLevel.MEMORY_ONLY_SER).map(_._2)
    }

    val unifiedStream = spark.sparkContext.union(streams)
    val sparkProcessingParallelism = 1
    unifiedStream.repartition(sparkProcessingParallelism)
  }

  val numInputMessage = spark.sparkContext.accumul (0L, "Kafka message consumer")
}
