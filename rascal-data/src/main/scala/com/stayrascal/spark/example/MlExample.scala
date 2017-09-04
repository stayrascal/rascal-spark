package com.stayrascal.spark.example

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession

case class Movie(movieId: Int, title: String)

case class User(userId: Int, gender: String, age: Int, occupation: Int, zipCode: String)

object MlExample {

  def parseMovieData(data: String): Movie = {
    val dataField = data.split("::")
    assert(dataField.size == 3)
    Movie(dataField(0).toInt, dataField(1))
  }

  def parseUserData(data: String): User = {
    val dataField = data.split("::")
    assert(dataField.size == 5)
    User(dataField(0).toInt, dataField(1), dataField(2).toInt, dataField(3).toInt, dataField(4))
  }

  def parseRatingData(data: String): Rating = {
    val dataField = data.split("::")
    Rating(dataField(0).toInt, dataField(1).toInt, dataField(2).toDouble)
  }

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Rascal Spark For Movie Recommend")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val moviesFile = "/Users/zpwu/workspace/spark/data/ml-1m/movies.dat"
  val usersFile = "/Users/zpwu/workspace/spark/data/ml-1m/users.dat"
  val ratingFile = "/Users/zpwu/workspace/spark/data/ml-1m/ratings.dat"
  val moviesData = spark.read.textFile(moviesFile).map(parseMovieData).cache()
  val usersData = spark.read.textFile(usersFile).map(parseUserData).cache()
  val ratingData = spark.read.textFile(ratingFile).map(parseRatingData).cache()

  def main(args: Array[String]): Unit = {
    /*showRatings*/


    val tempPartitions = ratingData.randomSplit(Array(0.7, 0.3), 1024L)
    val trainingSetOfRatingsData = tempPartitions(0).cache()
    val testSetOfRatingData = tempPartitions(1).cache()

    val mode = ALS.train(trainingSetOfRatingsData.rdd, 20, 10)
    val recomResult = mode.recommendProducts(2333, 10)

    val movieTitles = moviesData.toDF().collect().map(arr => (arr(0), arr(1)))
    val recomResultWithTitle = recomResult.map(rating => (movieTitles(rating.product), rating.rating))
    println(recomResultWithTitle.mkString("\n"))

    val userProducts = testSetOfRatingData.map { case Rating(user, product, rating) => (user, product) }

    val predictResultOfTestSet = mode.predict(testSetOfRatingData.map { case Rating(user, product, rating) => (user, product) }.rdd)

    val formatResultOfTestSet = testSetOfRatingData.map { case Rating(user, product, rating) => ((user, product), rating) }
    val formatResultOfPredictionResult = predictResultOfTestSet.map { case Rating(user, product, rating) => ((user, product), rating) }
    val finalResultForComparison = formatResultOfPredictionResult.join(formatResultOfTestSet.rdd)
    val name = finalResultForComparison.map {
      case ((user, product), (ratingOfTest, ratingOfPrediction)) =>
        val error = (ratingOfTest - ratingOfPrediction)
        Math.abs(error)
    }.mean()

    println(name)

  }

  private def showRatings = {
    val moviesDF = moviesData.toDF()
    val usersDF = usersData.toDF()
    val ratingDF = ratingData.toDF()
    moviesDF.createOrReplaceTempView("movies")
    usersDF.createOrReplaceTempView("users")
    ratingDF.createOrReplaceTempView("ratings")
    val highRatingMoives = spark.sql("SELECT ratings.user, ratings.product, ratings.rating, movies.title FROM ratings JOIN movies ON movies.movieId=ratings.product  WHERE ratings.user=1680 and ratings.rating > 4.5 ORDER BY ratings.rating DESC")
    highRatingMoives.show()
  }
}
