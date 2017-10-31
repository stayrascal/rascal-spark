package com.stayrascal.spark.kafka.sqlTemplate

case class TableName(originalName: String, metaName: String, aliasName: String, generateName: String = "")

case class TableField(table: TableName, fieldName: String, orderType: Option[OrderType] = None)

case class OrderType()

trait ReportMetaData extends ParcConfiguration

object SQLTemplate {

}
