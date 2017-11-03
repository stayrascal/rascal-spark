package com.stayrascal.spark.kafka.triangleCount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * 社区发现和对社交网络中的用户社区的粘度分析
  *
  * Triangle counting是一种社区分析算法，被用于确定经过图中每个节点的三角形的数量。
  * 如果一个节点有两个相邻节点而且这两个相邻节点之间有一条边，那么该节点是三角形的一部分。
  * 三角形是一个三节点的子图，其中每两个节点是相连的。Triangle counting算法返回一个图对象，我们可以从上面提取节点。
  *
  * Triangle counting被大量用于社交网络分析中。它提供了衡量数据聚类分析的方法，这对在社交网站中寻找社区和度量区域群落的粘度很有用。
  * Clustering Coefficient是社交网络中的一个重要的度量标准，它显示了一个节点周围社区之间的紧密程度
  *
  * Triangle count的算法的其它用户案例有垃圾邮件检测和连接推荐
  *
  * 与其它图算法相比，Triangle counting涉及大量的信息和复杂耗时的计算。
  *
  * PageRank衡量相关度
  * Triangle Counting衡量聚类结果
  */
object TriangleCountApp {
  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      println("usage: <input> <output> <minEdge> <StorageLevel>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val spark = SparkSession.builder().appName("Spark TriagnleCount Application").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val output = args(1)
    val minEdge = args(2).toInt
    val storageLevel = args(3)

    val sl: StorageLevel = storageLevel match {
      case "MEMORY_AND_DISK+SER" => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case _ => StorageLevel.MEMORY_ONLY
    }

    val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, sl, sl).partitionBy(PartitionStrategy.RandomVertexCut)

    val triCounts = graph.triangleCount().vertices
    println("num triangles are " + triCounts.count())

    sc.stop()
  }
}
