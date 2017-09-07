package com.stayrascal.spark.example.airplane

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

case class Flight(dayOfMonth: Int, dayOfWeek: Int, crsDepTime: Double, crsArrTime: Double, uniqueCarrier: String,
                  crsElapsedTime: Double, origin: String, dest: String, arrDelay: Int, depDelay: Int, delayFlag: Int)

object AirplaneExample {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Rascal Airplane Prediction")
    .master("local[*]").getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {


    val flightData = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/zpwu/workspace/spark/data/airport/1998.csv")
    val airportData = spark.read.format("com.databricks.spark.csv").option("header", "true").load("/Users/zpwu/workspace/spark/data/airport/airports.csv")

    flightData.createOrReplaceTempView("flights")
    airportData.createOrReplaceTempView("airports")

    val queryFlightnumResult = spark.sql("SELECT COUNT(FlightNum) FROM flights WHERE DepTime BETWEEN 0 AND 600")
    val queryFlightnumResult1 = spark.sql("SELECT COUNT(FlightNum)/COUNT(DISTINCT DayofMonth) FROM flights WHERE  Month = 1 AND DepTime BETWEEN 1001 AND 1400")
    val queryDestResult = spark.sql("SELECT DISTINCT Dest, ArrDelay FROM flights WHERE ArrDelay = 0")
    val queryDestResult2 = spark.sql("SELECT DISTINCT Dest, COUNT(ArrDelay) AS delayTimes FROM flights WHERE ArrDelay = 0 GROUP BY Dest ORDER BY delayTimes DESC")
    val queryDestResult3 = spark.sql("SELECT DISTINCT state, SUM(delayTimes) AS s FROM (SELECT DISTINCT Dest, COUNT(ArrDelay) AS delayTimes FROM flights WHERE ArrDelay = 0 GROUP BY Dest ) a JOIN airports b ON a.Dest = b.iata GROUP BY state ORDER BY s DESC")
    queryDestResult3.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite).option("header", "true").save(s"/Users/zpwu/workspace/spark/data/airport/QueryDestResult.csv")

    //    trainAndPredict(flightData)
  }

  private def trainAndPredict(flightData: DataFrame) = {
    val flightRDD: Dataset[Flight] = getFlightRDD(flightData)
    var mCarrier: Map[String, Int] = getCarrierMap(flightRDD)
    var mOrigin: Map[String, Int] = getOriginMap(flightRDD)
    var mDest: Map[String, Int] = getDestMap(flightRDD)
    var featuredRDD: Dataset[Array[Double]] = getFeatureRDD(flightRDD, mCarrier, mOrigin, mDest)
    val labeledRDD = getLabel(featuredRDD)
    val (trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]) = getDataSet(labeledRDD)
    var paramCateFeaturesInfo: Map[Int, Int] = getParamCateFeatureMap(mCarrier, mOrigin, mDest)

    val paramNumClasses = 2
    val paramMaxDepth = 9
    val paramMaxBins = 7000
    val paramImpurity = "gini"

    val flightDelayModel = DecisionTree.trainClassifier(trainingData, paramNumClasses, paramCateFeaturesInfo, paramImpurity, paramMaxDepth, paramMaxBins)
    val tmpDM = flightDelayModel.toDebugString
    //    print(tmpDM)

    val predictResult = testData.map { flight =>
      val tmpPredictResult = flightDelayModel.predict(flight.features)
      (flight.label, tmpPredictResult)
    }
    predictResult.take(10)

    val numOfCorrectPrediction = predictResult.filter { case (label, result) => (label == result) }.count()
    val predictaccury = numOfCorrectPrediction / testData.count().toDouble

    println(predictaccury)
  }

  private def getLabel(featuredRDD: Dataset[Array[Double]]) = {
    featuredRDD.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))))
  }

  private def getDataSet(labeledRDD: Dataset[LabeledPoint]) = {
    val notDelayedFlights = labeledRDD.filter(x => x.label == 0).randomSplit(Array(0.8, 0.2))(0)
    val delayedFlights = labeledRDD.filter(x => x.label == 1)
    val tmpTTData = notDelayedFlights.rdd ++ delayedFlights.rdd

    val TTData = tmpTTData.randomSplit(Array(0.7, 0.3))
    val trainingData = TTData(0)
    val testData = TTData(1)
    (trainingData, testData)
  }

  private def getParamCateFeatureMap(mCarrier: Map[String, Int], mOrigin: Map[String, Int], mDest: Map[String, Int]) = {
    var paramCateFeaturesInfo = Map[Int, Int]()
    paramCateFeaturesInfo += (0 -> 31)
    paramCateFeaturesInfo += (1 -> 7)
    paramCateFeaturesInfo += (4 -> mCarrier.size)
    paramCateFeaturesInfo += (6 -> mOrigin.size)
    paramCateFeaturesInfo += (7 -> mDest.size)
    paramCateFeaturesInfo
  }

  private def getFeatureRDD(flightRDD: Dataset[Flight], mCarrier: Map[String, Int], mOrigin: Map[String, Int], mDest: Map[String, Int]) = {
    var featuredRDD = flightRDD.map(flight => {
      val vDayOfMonth = flight.dayOfMonth - 1
      val vDayOfWeek = flight.dayOfWeek - 1
      val vCRSDepTime = flight.crsDepTime
      val vCRSArrTime = flight.crsArrTime
      val vCarrierID = mCarrier(flight.uniqueCarrier)
      val vCRSElapsedTime = flight.crsElapsedTime
      val vOriginID = mOrigin(flight.origin)
      val vDestID = mDest(flight.dest)
      val vDelayFlag = flight.delayFlag

      Array(vDelayFlag.toDouble, vDayOfMonth.toDouble, vDayOfWeek.toDouble, vCRSDepTime.toDouble, vCRSArrTime.toDouble,
        vCarrierID.toDouble, vCRSElapsedTime.toDouble, vOriginID.toDouble, vDestID.toDouble)
    }
    )
    featuredRDD
  }

  private def getDestMap(flightRDD: Dataset[Flight]) = {
    var destId: Int = 0
    var mDest: Map[String, Int] = Map()
    flightRDD.map(flight => flight.dest).distinct().collect().foreach(x => {
      mDest += (x -> destId)
      destId += 1
    })
    mDest
  }

  private def getOriginMap(flightRDD: Dataset[Flight]) = {
    var originId: Int = 0
    var mOrigin: Map[String, Int] = Map()
    flightRDD.map(flight => flight.origin).distinct().collect().foreach(x => {
      mOrigin += (x -> originId)
      originId += 1
    })
    mOrigin
  }

  private def getCarrierMap(flightRDD: Dataset[Flight]) = {
    var id: Int = 0
    var mCarrier: Map[String, Int] = Map()
    flightRDD.map(flight => flight.uniqueCarrier).distinct().collect().foreach(x => {
      mCarrier += (x -> id)
      id += 1
    })
    mCarrier
  }

  private def getFlightRDD(flightData: DataFrame) = {
    val tmpFlightDataRDD = flightData.map(row =>
      row(2).toString + "," + row(3).toString + "," + row(5).toString + "," + row(7).toString + "," +
        row(8).toString + "," + row(12).toString + "," + row(16).toString + "," + row(17).toString + "," +
        row(14).toString + "," + row(15).toString)
    val flightRDD = tmpFlightDataRDD.map(parseFields)
    flightRDD
  }

  def parseFields(input: String): Flight = {
    val line = input.split(",")
    var dayOfMonth = if (line(0) != "NA") line(0).toInt else 0
    var dayOfWeek = if (line(1) != "NA") line(1).toInt else 0
    var crsDepTime = if (line(2) != "NA") line(2).toDouble else 0.0
    var crsArrTime = if (line(3) != "NA") line(3).toDouble else 0.0
    var crsElapsedTime = if (line(5) != "NA") line(5).toDouble else 0.0
    var arrDelay = if (line(8) != "NA") line(8).toInt else 0
    var depDelay = if (line(9) != "NA") line(9).toInt else 0
    var delayFlag = if (arrDelay > 30 || depDelay > 30) 1 else 0
    Flight(dayOfMonth, dayOfWeek, crsDepTime, crsArrTime, line(4), crsElapsedTime, line(6), line(7), arrDelay, depDelay, delayFlag)
  }
}
