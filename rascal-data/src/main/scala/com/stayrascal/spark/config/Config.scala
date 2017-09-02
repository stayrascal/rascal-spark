package com.stayrascal.spark.config

case class Config(dataDir: String = "./data") {

}

case class RascalOnTimeConfig()(dataDir: String = "./data", currentWeekTimeFilePath: String = "./data/td/td_sample.csv")
