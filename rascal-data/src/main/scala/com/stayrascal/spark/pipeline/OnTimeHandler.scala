package com.stayrascal.spark.pipeline

import java.util.{Calendar, Date, Properties}

import com.stayrascal.spark.config.Config
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}
import scopt.OptionParser

class OnTimeHandler(spark: SparkSession) extends Serializable {

  import org.apache.spark.sql.functions._
  import spark.implicits._

  private def loadCsv(path: String): DataFrame = spark.read.format("csv").option("header", "true").load(path)

  private def weekDataReader(weekOfYear: Int, dataDir: String): DataFrame = {
    groupByWriter(dataDir)
    loadCsv(s"$dataDir/td-with-week.csv/week_of_year_repartition=$weekOfYear")
  }

  private def getWeekOfYear(date: Date): Int = {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.setFirstDayOfWeek(Calendar.MONDAY)
    calendar.get(Calendar.WEEK_OF_YEAR)
  }

  private def saveAsTable(dataFrame: DataFrame, dataDir: String) = {
    dataFrame.repartition(1).write.partitionBy("week_of_year_repartition")
      .format("com.databricks.spark.csv").mode(SaveMode.Overwrite)
      .option("header", "true").save(s"$dataDir/td-with-week.csv")
  }

  private def groupByWriter(dataDir: String): DataFrame = {
    val formattedDF = loadCsv(s"$dataDir/td/td*")
      .withColumn("formatted_end_date", col("pse__End_DAte__c").cast("date"))
      .withColumn("week_of_year", weekofyear(col("pse__End_Date__c")))
      .withColumn("week_of_year_repartition", weekofyear(col("pse_End_Date__c")))
    saveAsTable(formattedDF, dataDir)
    weekDataReader(getWeekOfYear(new Date()), dataDir)
  }

  def onTimeWriter(dataDir: String): Unit = {
    val currentWeeekTimeCardDF = groupByWriter(dataDir)
    val jsConsultantDF = loadCsv(s"$dataDir/js/consultant_processed.csv")
    val tcResourceDF = loadCsv(s"$dataDir/td/resourceid_js_map.csv")

    val resourceDF = tcResourceDF.join(jsConsultantDF, $"Employee_ID__c(in js)" === $"Employee ID")
      .select(
        $"resource Id" as "ResourceId",
        $"Employee ID" as "EmployeeId",
        $"Region"
      ).where($"Region" === "China")

    val getDateFormTime: (String => String) = (timeStr) => {
      if (timeStr != null) {
        timeStr.substring(0, 10)
      } else {
        ""
      }
    }

    val dateUDF = udf(getDateFormTime)

    val tdOnTimeDF = resourceDF.join(currentWeeekTimeCardDF, resourceDF("ResourceId") === currentWeeekTimeCardDF("pse__Resource__c"), "left_outer")
      .select($"EmployeeId", $"CreatedDate" as "SubmitDate", $"pse__End_Date__c" as "EndDate")
      .withColumn("EndDate", $"EndDate" as Encoders.DATE)
      .withColumn("SubmitDate", dateUDF(col("SubmitDate")))
      .withColumn("SubmitDate", $"SubmitDate" as Encoders.DATE)
      .withColumn("InTime", $"SubmitDate" <= $"EndDate")
      .dropDuplicates("EmployeeId")
      .sort(desc("InTime"))

    val url = "jdbc:mysql://localhost/td?useInicode=true&characterEncoding=UTF-8"
    val prop = new Properties
    prop.setProperty("user", "td")
    prop.setProperty("password", "td")
    try {
      tdOnTimeDF.select($"EmployeeId", $"InTime").write.jdbc(url, "open_result", prop)
    } catch {
      case _: Exception => tcResourceDF.select($"EmployeeId", $"InTime").write.mode(SaveMode.Append).jdbc(url, "open_result", prop)
    }
  }
}


object OnTimeHandler {
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Rascal Spark Open Result Process")
    .getOrCreate()

  val handler = new OnTimeHandler(spark)

  def main(args: Array[String]): Unit = {

  }

  def parser = new OptionParser[Config]("Data Pipeline") {
    head("Data Pipeline", "1.0")

    opt[String]('d', "data-dir").action((d, c) => {
      c.copy(dataDir = d)
    }).text("Data Directory")
  }
}