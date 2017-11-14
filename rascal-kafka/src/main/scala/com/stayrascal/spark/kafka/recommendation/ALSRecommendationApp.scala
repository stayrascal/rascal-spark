package com.stayrascal.spark.kafka.recommendation

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class ALSRecommendationApp {

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x => ((x.user, x.product), x.rating) }
      .join(data.map { x => ((x.user, x.product), x.rating) }).values
    if (implicitPrefs) {
      println("(Prediction, Rating)")
      println(predictionsAndRatings.take(5).mkString("\n"))
    }
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark ALS recommendation application").master("local[*]").getOrCreate()

    import spark.implicits._

    val ratingsFile = "data/ratings.csv"
    val df = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratingsFile)
    val ratingsDF = df.select($"userId", $"movieId", $"rating", $"timestamp")
    ratingsDF.show(false)

    val moviesFile = "data/movies.csv"
    val moviesDF = spark.read.format("com.databricks.spark.csv").option("header", true).load(ratingsFile).select($"movieId", $"title", $"genres")

    ratingsDF.createOrReplaceTempView("ratings")
    moviesDF.createOrReplaceTempView("movies")

    val numRatings = ratingsDF.count()
    val numUsers = ratingsDF.select($"userId").distinct().count()
    val numMovies = ratingsDF.select($"movieId").distinct().count()
    println(s"got $numRatings ratings from $numUsers users on $numMovies movies.")

    val results = spark.sql("select movies.title, movierates.maxr, movierates.minr, movierates.cntu " +
      "from (SELECT ratings.movieId, max(ratings.rating) as maxr, " +
      "min(ratings.rating) as minr, count(distinct userId) as cntu " +
      "FROM ratings group by ratings.movieId) movierates " +
      "join movies on movierates.movieId=mmovies.movieId order by movierates.cntu desc")
    results.show(false)

    val Array(trainingData, testData) = ratingsDF.randomSplit(Array(0.75, 0.25), seed = 12345L)
    println(s"Training: ${trainingData.count()} test: ${testData.count()}")

    val ratingsRDD = trainingData.rdd.map(row => Rating(row.getString(0).toInt, row.getString(1).toInt, row.getString(2).toDouble))
    val testRDD = testData.rdd.map(row => Rating(row.getString(0).toInt, row.getString(1).toInt, row.getString(2).toDouble))

    val model = new ALS()
      .setIterations(15)
      .setBlocks(-1)
      .setAlpha(1.00)
      .setLambda(0.1)
      .setRank(20)
      .setSeed(12345L)
      .setImplicitPrefs(false)
      .run(ratingsRDD)

    // Making Predictions. Get the top 6 movie predictions for use 668
    println("Rating: (UserID, MovieID, Rating)")
    println("---------------------------------")
    val topRecsForUser = model.recommendProducts(668, 6)
    topRecsForUser.foreach(rating => println(rating.toString))
    println("---------------------------------")

    val rmseTest = computeRmse(model, testRDD, true)
  }
}
