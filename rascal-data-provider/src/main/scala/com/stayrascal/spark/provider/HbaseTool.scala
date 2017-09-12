package com.stayrascal.spark.provider

import java.io.FileInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

object HbaseTool {
  val hadoopConf = new Configuration()
  hadoopConf.addResource("classpath:core-site.xml")
  hadoopConf.addResource("classpath:hdfs-site.xml")

  val table = new mutable.HashMap[String, HTable]()
  var conf = HBaseConfiguration.create(hadoopConf)


  def a(conf: Configuration) = {
    val admin = new HBaseAdmin(conf)
    val tableDescriptor = new HTableDescriptor(TableName.valueOf("emp"))
    tableDescriptor.addFamily(new HColumnDescriptor("personal"))
    tableDescriptor.addFamily(new HColumnDescriptor("professional"))

    admin.createTable(tableDescriptor)
    System.out.println(" Table Created ")
  }

  def main(args: Array[String]): Unit = {
    conf.addResource(new FileInputStream("/tmp/hbase/hbase-policy.xml"))
    conf.addResource(new FileInputStream("/tmp/hbase/hbase-site.xml"))
    conf.addResource(new FileInputStream("/tmp/core-site.xml"))
    conf.addResource(new FileInputStream("/tmp/hdfs-site.xml"))

    a(conf)
    println(conf.get("hbase.zookeeper.quorum"))
    val connection = ConnectionFactory.createConnection(conf)

    val get = new Get("/cube/kulin_cales_cube.json".getBytes())
    val tb = connection.getTable(TableName.valueOf("kylin_metadata"))
    val result = tb.get(get)

    val s1: Array[KeyValue] = result.listCells().toArray().map(_.asInstanceOf[KeyValue])
    s1.foreach(f => println(Bytes.toString(f.getFamilyArray())))
  }

  def setConf(c: Configuration) = {
    conf = c
  }

  def getTable(tableName: String): HTable = {
    table.getOrElse(tableName, {
      println("------ new connection -----")
      val tbl = new HTable(conf, tableName)
      table(tableName) = tbl
      tbl
    })
  }

  def getValue(tableName: String, rowKey: String, family: String, qualifiers: Array[String]): Array[(String, String)] = {
    var result: AnyRef = null
    val table_t = getTable(tableName)
    val row1 = new Get((Bytes.toBytes(rowKey)))
    val HBaseRow = table_t.get(row1)
    if (HBaseRow != null && !HBaseRow.isEmpty) {
      result = qualifiers.map(c => {
        (tableName + "." + c, Bytes.toString(HBaseRow.getValue(Bytes.toBytes(family), Bytes.toBytes(c))))
      })
    } else {
      result = qualifiers.map(c => (tableName + "." + c, "null"))
    }
    result.asInstanceOf[Array[(String, String)]]
  }

  def putValue(tableName: String, rowKey: String, family: String, qualifierValue: Array[(String, String)]): Unit = {
    val table = getTable(tableName)
    val new_row = new Put(Bytes.toBytes(rowKey))
    qualifierValue.map(x => {
      var column = x._1
      val value = x._2
      val tt = column.split("\\.")
      if (tt.length == 2) column = tt(1)
      if (!value.isEmpty) {
        new_row.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
      }
    })
    table.put(new_row)
  }

  val family = "F"
}
