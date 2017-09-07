package com.stayrascal.spark.pipeline

import java.io.FileOutputStream
import java.util.{Calendar, Date}

import com.stayrascal.spark.config.Config
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, DoubleType}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import scala.pickling.Defaults._
import scala.pickling.binary._

case class StringIndexerParam(uid: String, inputCol: String, outputCol: String, labels: Array[String])

class RascalCombiner(spark: SparkSession) extends Serializable {

  import spark.implicits._

  var stringIndexers: Map[String, StringIndexerModel] = _

  def loadCsv(path: String): DataFrame = {
    spark.read.format("csv").option("header", true).load(path)
  }

  def generateRecordDF(dataDir: String): DataFrame = {
    val tdDF = loadCsv(dataDir + "/td/td*csv")
    val jsProjectDF = loadCsv(dataDir + "/js/project.csv")
    val tcProjectDF = loadCsv(dataDir + "/td/project.csv")


    val projectDF = tcProjectDF.join(jsProjectDF, $"NAME" === $"Project", "left")
      .select($"Id" as "ProjectId", $"Name" as "ProjectName", $"Account")

    tdDF.join(projectDF, $"ProjectId1" === $"ProjectId")
      .select($"Account", $"CreateDate" as "SubmitDate")
  }

  def makeStringIndex(df: DataFrame, labels: List[String]): DataFrame = {
    if (stringIndexers == null) {
      stringIndexers = labels.map(label => (label, new StringIndexer().setInputCol(label).setOutputCol(s"${label}Index").fit(df))).toMap
    }
    stringIndexers.values.foldLeft(df)((d, indexer) => indexer.transform(d))
  }

  def buildFeatures(recordDF: DataFrame, withLabel: Boolean = true): DataFrame = {
    val addedLeaveFeature = calculateLeaveFeature(recordDF)
    val numberized = numberize(addedLeaveFeature, List("TotalYears", "TwYears", "OtherYears"))

    val fieldsToProcess = List("Role", "Grade", "Region")

    val nullRemoved = if (withLabel) {
      val labeled = calculateLabel(numberized)
      replaceNull(labeled, fieldsToProcess)
    } else {
      replaceNull(numberized, fieldsToProcess)
    }

    val leaveNullRemoved = replaceNull(nullRemoved, List("LeaveStartDate", "LeaveEndDate"))

    val vectorized = makeOneHot(makeStringIndex(leaveNullRemoved, fieldsToProcess), fieldsToProcess)

    val assembler = new VectorAssembler()
      .setInputCols(Array("AccountVec", "GenderVec", "RoleVec", "TotalYears", "TwYears", "OtherYears"))
      .setOutputCol("Features")

    val assembled = assembler.transform(vectorized)

    val asDense = udf((v: org.apache.spark.ml.linalg.Vector) => v match {
      case _: SparseVector => v.toDense
      case _ => v
    })

    if (withLabel) {
      assembled.withColumn("Features", asDense('Features))
        .select($"Enddate", $"Account", $"SubmitDate", $"Role", $"Grade", $"Region", $"TotalYears", $"TwYears", $"OtherYears", $"IsOnTime", $"LeaveStartDate",
          $"LeaveEndDate", $"LeaveOnFriday", $"Features")
    } else {
      assembled.withColumn("Features", asDense('Features))
        .select($"Enddate", $"Account", $"SubmitDate", $"Role", $"Grade", $"Region", $"TotalYears", $"TwYears", $"OtherYears", $"LeaveStartDate",
          $"LeaveEndDate", $"LeaveOnFriday", $"Features")
    }
  }

  def replaceNull(df: DataFrame, labels: List[String], replacement: String = "__NA__"): DataFrame = {
    labels.foldLeft(df)((d, label) => d.withColumn(label, when(col(label).isNull, replacement).otherwise(col(label))))
  }

  def calculateLeaveFeature(df: DataFrame): DataFrame = {
    val getDayOfWeek: (Date => Int) = (endDate) => {
      val calendar: Calendar = Calendar.getInstance()
      calendar.setTime(endDate)
      var isFriday = calendar.get(Calendar.DAY_OF_WEEK) - 1 == 5
      val leaveOnFriday = if (isFriday) 1 else 0
      leaveOnFriday
    }

    val dateUDF = udf(getDayOfWeek)
    df.withColumn("LeaveStartDate", col("LeaveStartDate").cast(DateType))
      .withColumn("LeaveEndDate", col("LeaveEndDate").cast(DateType))
      .withColumn("LeaveOnFriday", when(col("LeaveEndDate").isNull, 0).otherwise(dateUDF(col("LeaveEndDate"))))
  }

  def numberize(df: DataFrame, labels: List[String]): DataFrame = {
    labels.foldLeft(df)((d, lbl) => d.withColumn(lbl, col(lbl) cast DoubleType))
  }

  def calculateLabel(df: DataFrame): DataFrame = {
    Encoders.DATE
    df.withColumn("EndDate", $"EndDate" as Encoders.DATE)
      .withColumn("SubmitDate", $"SubmitDate" as Encoders.DATE)
      .withColumn("IsOnTime", $"SubmitDate" < $"EndDate")
  }

  def makeOneHot(df: DataFrame, labels: List[String]): DataFrame = {
    labels.map(label => new OneHotEncoder().setInputCol(s"${label}Index}").setOutputCol(s"${label}Vec").setDropLast(false))
      .foldLeft(df)((d, encoder) => encoder.transform(d))
  }

  def save(path: String): Unit = {
    val stream = new FileOutputStream(path)
    val output = new StreamOutput(stream)

    val pickle = stringIndexers.mapValues(indexer => StringIndexerParam(indexer.uid, indexer.getInputCol, indexer.getOutputCol, indexer.labels))
      .pickleTo(output)
  }
}

object RascalCombiner {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Rascal Spark Demo Data Process")
      .master("local[*]")
    .getOrCreate()

  val combiner = new RascalCombiner(spark)

  import spark.implicits._

  def save(train: DataFrame, label: DataFrame): Unit = {
    train.map(_.getAs[DenseVector]("Features").toArray.mkString(",")).repartition(1).write.csv("/data/for-ml/train.csv")
    label.repartition(1).write.csv("/data/for-ml/label.csv")
  }

  def main(args: Array[String]): Unit = {
    parser.parse(args, Config()) match {
      case Some(cfg) =>
        val recordDF = combiner.generateRecordDF(cfg.dataDir)
        val featured = combiner.buildFeatures(recordDF)
        featured.show()
        val train = featured.select("Features")
        val label = featured.select("IsOnTime")
        save(train, label)
        combiner.save("combiner.txt")
      case None =>
        sys.exit(1)
    }
  }

  def parser = new scopt.OptionParser[Config]("Data Pipeline") {
    head("Data Pupeline", "1.0")

    opt[String]('d', "data-dir").action((d, c) => c.copy(dataDir = d)).text("Data Directory")
  }

}
