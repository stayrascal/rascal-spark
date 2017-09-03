package com.stayrascal.spark

import com.stayrascal.spark.pipeline.RascalCombiner
import org.apache.spark.sql.SparkSession
import org.eclipse.jetty.server.Server

object RascalEmbedServer {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Rascal Spark Demo Query Server")
      .getOrCreate()
    val combiner = new RascalCombiner(spark)
    if (args.length > 0) {
      new EmbedServer(spark, combiner, args(0).toInt).start()
    } else {
      new EmbedServer(spark, combiner).start()
    }
  }

  private class EmbedServer(spark: SparkSession, combiner: RascalCombiner, port: Int = 8888) {
    def start(): Unit = {
      val server = new Server(port)
      server.dumpStdErr()
      server.setHandler(new RascalHandler(spark, combiner))
      server.start()
      server.join()
    }
  }
}
