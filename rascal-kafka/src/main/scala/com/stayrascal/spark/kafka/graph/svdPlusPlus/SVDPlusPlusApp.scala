package com.stayrascal.spark.kafka.graph.svdPlusPlus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.SVDPlusPlus
import org.apache.spark.graphx.{Edge, GraphLoader, PartitionStrategy}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * SVD++ 是协同过滤算法(Collaborative Filtering)中基于隐语义方法(basic SVD, RSVD, ASVD, SCD++)的一种
  *
  * 基于领域的方法(item-based, user-based)
  * - 收集用户行为
  * - 找到相似的用户或者物品，得到用户和物品的特征向量，进一步计算相似度和找到物品或者用户的相似邻居
  * - 针对电子商务网站，计算物品的相似度计算量少，不必频繁刷新，item-based更优
  * - 对于新闻博客推荐系统，物品数量海量，刷新频繁，user-based更有
  *
  * 基于隐语义的方法
  * - 不依赖于共同评分，基本思想是将用户和物品分别映射到某种真实含义未知的特征向量
  * - 比如将user兴趣表示为4维向量：颜色偏好，重量要求，设计风格偏好，价格偏好
  * - 比如将item特点表示为4维向量：颜色，重量，设计风格，价格
  * - 然后通过两个特征向量的内积来判断用户对一个物品的喜好程度
  *
  * basic SVD的目标函数中只有训练误差SSE，容易导致过拟合
  * RSVD 用户对商品的打分不仅取决于用户和商品间的某种关系，还取决于用户和商品独有的特质，用基线评分来表示这些性质
  * ASVD 去掉RSVD中的用户矩阵，利用用户评过分的商品和用户浏览过尚未评分的商品属性来表示用户属性，存储空间减少，但是迭代时间太长
  * SVD++ 引入隐私反馈，使用用户的历史浏览数据，用户历史评分数据，物品历史浏览数据，物品历史评分数据作为新的参数
  */
object SVDPlusPlusApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 12) {
      println("usage: <input> <output> <minEdge> <numIter> <rank> <minVal> <maxVal> <gamma1> <gamma2> <gamma6> <gamma7> <StroageLevel>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val spark = SparkSession.builder().appName("Spark SVDPlusPlus Application").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val output = args(1)
    val minEdge = args(2).toInt
    val numIter = args(3).toInt
    val rank = args(4).toInt // 因子向量维度
    val minVal = args(5).toDouble //评分下限
    val maxVal = args(6).toDouble //评分上线
    val gamma1 = args(7).toDouble // b*梯度下降学习速率
    val gamma2 = args(8).toDouble // q,p,y梯度下降学习速率
    val gamma6 = args(9).toDouble // b*正则系数
    val gamma7 = args(10).toDouble // q,p,y正则系数
    val storageLevel = args(11)

    val sl: StorageLevel = storageLevel match {
      case "MEMORY_AND_DISK+SER" => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case _ => StorageLevel.MEMORY_ONLY
    }

    val conf = new SVDPlusPlus.Conf(rank, numIter, minVal, maxVal, gamma1, gamma2, gamma6, gamma7)

    var edges: RDD[Edge[Double]] = null
    val dataset = "small"
    if (dataset == "small") {
      val graph = GraphLoader.edgeListFile(sc, input, true, minEdge, sl, sl).partitionBy(PartitionStrategy.RandomVertexCut)
      edges = graph.edges.map { e => {
        var attr = 0.0
        if (e.dstId % 2 == 1) attr = 5.0 else attr = 1.0
        Edge(e.srcId, e.dstId, e.attr.toDouble)
      }
      }
    }
  }
}
