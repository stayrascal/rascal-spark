package com.stayrascal.spark.example

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class VertexProperty()

case class UserProperty(val name: String) extends VertexProperty

case class ProductProperty(val name: String, val price: Double) extends VertexProperty

object GraphExample {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Rascal Spark For Graph")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    /*basic()*/

    val graph = GraphLoader.edgeListFile(spark.sparkContext, "/Users/zpwu/workspace/spark/data/page-rank/followers.txt")

    val users = spark.sparkContext.textFile("/Users/zpwu/workspace/spark/data/page-rank/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    val cc = graph.connectedComponents().vertices
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    println(ccByUsername.collect.mkString("\n"))
    /*printUserRanks(graph, users)*/
  }

  private def printTrianglecount() = {
    val graph = GraphLoader.edgeListFile(spark.sparkContext, "/Users/zpwu/workspace/spark/data/page-rank/followers.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)

    val triCounts = graph.triangleCount().vertices
    val users = spark.sparkContext.textFile("/Users/zpwu/workspace/spark/data/page-rank/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map {
      case(id, (username, tc)) => (username, tc)
    }
    println(triCountByUsername.collect().mkString("\n"))
  }

  private def printUserRanks(graph: Graph[PartitionID, PartitionID], users: RDD[(VertexId, String)]) = {
    val ranks = graph.pageRank(0.0001).vertices

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    println(ranksByUsername.collect().mkString("\n"))
  }

  def basic(): Unit = {
    val users: RDD[(VertexId, (String, String))] = spark.sparkContext.parallelize(Array(
      (3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    // Create an RDD for edges
    val relationships: RDD[Edge[String]] = spark.sparkContext.parallelize(Array(
      Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
      Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))

    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")

    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    // Count all users which are postdocs
    val postdocCounts = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count

    // Count all the edges where src > dst
    val testCount = graph.edges.filter(e => e.srcId > e.dstId).count()

    // use triplets view creat  e a facts RDD
    val facts: RDD[String] = graph.triplets.map(triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect.foreach(fact => println(fact + "\n"))

    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect.foreach(println(_))
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_) + "\n")
  }
}
