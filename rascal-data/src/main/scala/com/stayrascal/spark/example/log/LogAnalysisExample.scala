package com.stayrascal.spark.example.log

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogAnalysisExample {

  val batch = 10
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Log Analysis Demo")
    .master("local[*]")
    .getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(batch))

  def main(args: Array[String]): Unit = {
    val lines = ssc.textFileStream("hdfs:///spark/streaming")
    lines.count().print() // total pv

    lines.map(line => {(line.split(" ")(0), 1)}).reduceByKey(_ + _).transform(rdd => {
      rdd.map(ip_pv => (ip_pv._2, ip_pv._1))
        .sortByKey(false)
        .map(ip_pv => (ip_pv._2, ip_pv._1))
    }).print() // order pv by IP desc

    val refer = lines.map(_.split("\"")(3))
    val searchEngineInfo = refer.map(r => {
      val f = r.split('/')
      val searchEngines = Map(
        "www.google.cn" -> "q",
        "www.yahoo.com" -> "p",
        "cn.bing.com" -> "q",
        "www.baidu.com" -> "wd",
        "www.sogou.com" -> "query"
      )

      if (r.length > 2) {
        val host = f(2)
        if (searchEngines.contains(host)) {
          val query = r.split('?')(1)
          if (query.length > 0) {
            val arr_search_q = query.split('&').filter(_.indexOf(searchEngines(host)+"=") == 0)
            if (arr_search_q.length > 0 ){
              (host, arr_search_q(0).split('=')(1))
            } else {
              (host, "")
            }
          } else {
            (host, "")
          }
        } else {
          ("", "")
        }
      } else {
        ("", "")
      }
    })

    // search engine pv
    searchEngineInfo.filter(_._1.length > 0).map(p => {(p._1, 1)}).reduceByKey(_ + _).print()

    // key word pv
    searchEngineInfo.filter(_._2.length > 0).map(p => {(p._2, 1)}).reduceByKey(_ + _).print()

    // terminal pv
    lines.map(_.split("\"")(5)).map(agent => {
      val types = Seq("iPhone", "Android")
      var r = "Default"
      for (t <- types) {
        if (agent.indexOf(t) != -1){
          r = t
        }
      }
      (r, 1)
    }).reduceByKey(_ + _).print()


    // page pv
    lines.map(line => {(line.split("\"")(1).split(" ")(1), 1)}).reduceByKey(_ + _).print()


    ssc.start()
    ssc.awaitTermination()
  }


}
