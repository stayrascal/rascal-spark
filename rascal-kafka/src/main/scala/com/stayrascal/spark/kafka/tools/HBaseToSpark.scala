package com.stayrascal.spark.kafka.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable

import scala.collection.mutable

object HBaseToSpark {
  val hadoopConf = new Configuration()
  hadoopConf.addResource(getClass.getClassLoader.getResourceAsStream("core-site.xml"))
  hadoopConf.addResource(getClass.getClassLoader.getResourceAsStream("hdfs-site.xml"))

  val table = new mutable.HashMap[String, HTable]()
  val conf = HBaseConfiguration.create(hadoopConf)

  def main(args: Array[String]): Unit = {
    conf.set("hbase.zookeeper.quorum", "")
    conf.setInt("hbase.zookeeper.property.clientPort", 2181)

    conf.addResource(getClass.getClassLoader.getResourceAsStream("hbase-policy.xml"))
    conf.addResource(getClass.getClassLoader.getResourceAsStream("hbase-site.xml"))

  }

}
