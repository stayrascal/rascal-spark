package com.stayrascal.spark.kafka.graph.labelPropagation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * 半监督学习 Semi-supervised learning
  * 半监督学习基于三种假设:
  * - Smoothness平滑假设：相似的数据具有相同的label
  * - Cluster聚类假设：处于统一聚类下的数据具有相同label
  * - Manifold流形假设：处于同一流形结构下的数据具有相同label
  *
  * 半监督学习最简单的标签传播算法：相似的数据应该具有相同的label
  * - 构造相似矩阵
  * - 传播
  *
  *
  * Label Propagation算法是一种基于标签传播的局部社区划分算法。
  * 对于网络中的每一个节点，在初始阶段，Label Propagation算法对每一个节点一个唯一的标签，
  * 在每一个迭代的过程中，每一个节点根据与其相连的节点所属的标签改变自己的标签，
  * 更改的原则是选择与其相连的节点中所属标签最多的社区标签为自己的社区标签，这便是标签传播的含义。
  * 随着社区标签的不断传播，最终紧密连接的节点将有共同的标签。
  */
object LabelPropagationApp {
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      println("usage: <input> <output>; datagen: <numVertices> <numParition>")
      System.exit(0)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val spark = SparkSession.builder().appName("Spark LabelPropagation Application").master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    val input = args(0)
    val output = args(1)
    val numVertices = args(2).toInt
    val numPAr = args(3).toInt

    val n = numVertices
    val clique1 = for (u <- 0L until n; v <- 0L until n) yield Edge(u, v, 1)
    val clique2 = for (u <- 0L to n; v <- 0L to n) yield Edge(u + n, v + n, 1)
    val twoCliques = sc.parallelize(clique1 ++ clique2 :+ Edge(0L, n, 1), numPAr)
    val graph = Graph.fromEdges(twoCliques, 1)

    //Run label propagation
    val labels = LabelPropagation.run(graph, 20).persist()


    // All vertices within a clique should have the same label
    val clique1Labels = labels.vertices.filter(_._1 < n).map(_._2).collect.toArray
    val b1 = clique1Labels.forall(_ == clique1Labels(0))
    val clique2Labels = labels.vertices.filter(_._1 >= n).map(_._2).collect.toArray
    val b2 = clique1Labels.forall(_ == clique2Labels(0))
    val b3 = clique1Labels(0) != clique2Labels(0)
    println("result %d %d %d".format(b1, b2, b3))
    sc.stop()

  }
}
