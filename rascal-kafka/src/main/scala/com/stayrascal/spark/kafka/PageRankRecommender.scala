package com.stayrascal.spark.kafka

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object PageRankRecommender {
  val logger = LoggerFactory.getLogger(getClass)

  def pageRank(sc: SparkContext) = {
    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    val ranks = graph.pageRank(0.0001).vertices
    val users = sc.textFile("data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map { case (id, (username, rank)) => (username, rank) }
    ranksByUsername.collect.sortBy(_._2).foreach(println)
    ranks.collect.sortBy(_._2).foreach(println)
  }

  def ppr1(sc: SparkContext) = {
    val relationshipMap = Map(
      "St" -> Seq("Forrest Gump", "Gladiator"),
      "Sm" -> Seq("Forrest Gump"),
      "Ab" -> Seq("The Reader", "Clumgod"),
      "Se" -> Seq("Pulp Fiction", "The Godfather")
    )

    val users = relationshipMap.keys.toSeq
    val movies = relationshipMap.values.flatMap(identity).toSet.toSeq
    val nodeMap = (users ++ movies).zipWithIndex.toMap

    val nodeRdd: RDD[(VertexId, Unit)] = sc.parallelize(users.map { user =>
      (nodeMap(user).toLong, ())
    }).union(sc.parallelize(movies.map({ movie =>
      (nodeMap(movie).toLong, ())
    })))

    val relationshipRdd: RDD[Edge[Unit]] = sc.parallelize(relationshipMap.flatMap { case (user, moives) =>
      val userId = nodeMap(user)
      movies.flatMap { movie =>
        val movieId = nodeMap(movie)
        Seq(Edge(userId, movieId, ()), Edge(movieId, userId, ()))
      }
    }.toSeq)

    val graph = Graph(nodeRdd, relationshipRdd)
    val nodeIdMap = nodeMap.map(_.swap)
    val res = graph.staticPersonalizedPageRank(nodeMap("Sm"), 20).vertices.map { case (id, rank) =>
      (nodeIdMap(id.toInt), rank)
    }.collect

    val pRes = res.sortWith(_._2 > _._2).partition(_._1.length == 2)
    pRes._1.foreach(println)
    pRes._2.foreach(println)
  }

  def ppr2(sc: SparkContext) = {
    val nodeRdd: RDD[(VertexId, Unit)] = sc.parallelize(((1 to 3) ++ (11 to 14)).map(id => (id.toLong, ())))

    val relationshipRdd: RDD[Edge[Unit]] = sc.parallelize(
      Seq((1, 11), (1, 12), (2, 12), (3, 14)).flatMap({ case (user, item) =>
        Seq(Edge(user, item, ()), Edge(item, user, ()))
      })
    )

    val graph = Graph(nodeRdd, relationshipRdd)
    val res = graph.personalizedPageRank(2, 0.0001, 0.15).vertices.collect
    val pRes = res.sortWith(_._2 > _._2).partition(_._1 >= 10)
    pRes._1.foreach(println)
    pRes._2.foreach(println)
  }


  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().appName("PageRankRecommender").master("local[2]").getOrCreate()
    ppr1(spark.sparkContext)
    spark.sparkContext.stop()
  }
}
