package com.stayrascal.spark.kafka

import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoUnit

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer
import scalikejdbc._

object ExactlyOnce {

  case class Log(time: LocalDateTime, level: String)

  val logPattern = "^(.{19}) ([A-Z]+).*".r
  val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def parseLog(line: String): Option[Log] = {
    line match {
      case logPattern(timeString, level) => {
        val timeOption = try {
          Some(LocalDateTime.parse(timeString, dateTimeFormatter))
        } catch {
          case _: DateTimeParseException => None
        }
        timeOption.map(Log(_, level))
      }
      case _ => None
    }
  }

  def processLogs(messages: RDD[ConsumerRecord[String, String]]): RDD[(LocalDateTime, Int)] = {
    messages.map(_.value)
      .flatMap(parseLog)
      .filter(_.level == "ERROR")
      .map(log => log.time.truncatedTo(ChronoUnit.MINUTES) -> 1)
      .reduceByKey(_ + _)
  }

  def main(args: Array[String]): Unit = {
    val brokers = "localhost:9092"
    val topic = "alog"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "exactly-once",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "auto.offset.reset" -> "none"
    )

    val spark = SparkSession.builder()
      .appName("ExactlyOnce")
      .master("local[*]")
      .getOrCreate()

    ConnectionPool.singleton("jdbc:mysql://localhost:3306/spark", "root", "")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val fromOffsets = DB.readOnly { implicit session =>
      sql"""
      SELECT `partition`, offset from kafka_offset
      WHERE topic = ${topic}
      """.map { rs =>
        new TopicPartition(topic, rs.int("partition")) -> rs.long("offset")
      }.list.apply().toMap
    }

    val messages = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets))

    messages.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val result = processLogs(rdd).collect()

      DB.localTx { implicit session =>
        sql"""
             INSERT INTO error_log(log_time, log_count)
             VALUE (${time}, ${count})
             ON duplicate key update log_count = log_count _ VALUES (log_count)
           """.update.apply()
      }

      val affectedRows = offsetRanges.foreach { offsetRange =>
        sql"""
             INSERT ignore INTO kafka_offset (topic, `partition`, offset) VALUE (${topic}, ${offsetRange.partition}, ${offsetRange.fromOffset})
           """.update.apply()
      }

      if (affectedRows != 1) {
        throw new Exception("fail to update offset")
      }
    }
    ssc.stop()
    ssc.awaitTermination()
  }
}
