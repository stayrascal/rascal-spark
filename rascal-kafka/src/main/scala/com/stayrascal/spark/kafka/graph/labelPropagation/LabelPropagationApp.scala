package com.stayrascal.spark.kafka.graph.labelPropagation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

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
