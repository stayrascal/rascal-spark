package com.stayrascal.spark.kafka.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

import scala.collection.mutable

object HBaseToSpark {
  val hadoopConf = new Configuration()
  hadoopConf.addResource(getClass.getClassLoader.getResourceAsStream("core-site.xml"))
  hadoopConf.addResource(getClass.getClassLoader.getResourceAsStream("hdfs-site.xml"))

  val table = new mutable.HashMap[String, Table]()
  val conf = HBaseConfiguration.create(hadoopConf)

  def main(args: Array[String]): Unit = {
    conf.set("hbase.zookeeper.quorum", "")
    conf.setInt("hbase.zookeeper.property.clientPort", 2181)

    conf.addResource(getClass.getClassLoader.getResourceAsStream("hbase-policy.xml"))
    conf.addResource(getClass.getClassLoader.getResourceAsStream("hbase-site.xml"))

    val connection = ConnectionFactory.createConnection(conf)

    def createTable(conf: Configuration) = {
      val admin = connection.getAdmin()
      val tableDescriptor = new HTableDescriptor(TableName.valueOf("emp"))
      tableDescriptor.addFamily(new HColumnDescriptor("personal"))
      tableDescriptor.addFamily(new HColumnDescriptor("professional"))
      admin.createTable(tableDescriptor)
    }

    def getTable(tableName: String): Table = {
      val tal = connection.getTable(TableName.valueOf(tableName))
      table(tableName) = tal
      tal
    }

    def getValue(tableName: String, rowKey: String, family: String, qualifiers: Array[String]): Array[(String, String)] = {
      val tbl = getTable(tableName)
      val row = tbl.get(new Get(Bytes.toBytes(rowKey)))
      if (row != null && !row.isEmpty) {
        qualifiers.map(qualifier => (tableName + "." + qualifier, Bytes.toString(row.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)))))
      } else {
        qualifiers.map(qualifier => (tableName + "." + qualifier, "null"))
      }
    }

    def putValue(tableName: String, rowKey: String, family: String, qualifierValues: Array[(String, String)]) = {
      val tal = getTable(tableName)
      val row = new Put(Bytes.toBytes(rowKey))

      qualifierValues.map(qualifier => {
        var column = qualifier._1
        val value = qualifier._2
        val tt = column.split("\\.")
        if (tt.length == 2) column = tt(1)
        if (!value.isEmpty) {
          row.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
        }
      })
      tal.put(row)
    }
  }

}
