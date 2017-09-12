package com.stayrascal.spark.provider

import java.io.PrintWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HadoopClient {

}

object HadoopClient {
  def main(args: Array[String]): Unit = {
    println("Trying to write something to HDFS....")
    val conf = new Configuration()
    conf.addResource("classpath:core-site.xml")
    conf.addResource("classpath:hdfs-site.xml")
    val fs = FileSystem.get(conf)
    fs.listStatus(new Path("/")).map(f => println(f.getPath))

    val output = fs.create(new Path("hdfs:///tmp/mySample.txt"))
    val writer = new PrintWriter(output)
    try {
      writer.write("this is a test content")
      writer.write("\n")
    }
    finally {
      writer.close()
    }
    println("Done!")
  }
}
